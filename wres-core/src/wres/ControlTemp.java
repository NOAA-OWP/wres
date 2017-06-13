package wres;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.DataFactory;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.inputs.MetricInputFactory;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputCollection;
import wres.engine.statistics.metric.outputs.ScalarOutput;
import wres.io.data.caching.MeasurementCache;
import wres.io.data.caching.VariableCache;
import wres.io.utilities.Database;

/**
 * Another Main. The reason for creating this class separately from Main is to defer conflict with the existing
 * Main.java code to a later date, at the request of a teammate. It is expected that eventually one of the two will
 * become Main and the other will be merged into it. In order to make something like MainFunctions where a closed loop
 * request/response is created, a separate Main seemed needed. Has (too many?) private static classes that will need to
 * be split out if they are deemed useful.
 */
public class ControlTemp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ControlTemp.class);
    public static final long LOG_PROGRESS_INTERVAL_MILLIS = 2000;
    private static final AtomicLong lastMessageTime = new AtomicLong();

    /** System property used to retrieve max thread count, passed as -D */
    public static final String MAX_THREADS_PROP_NAME = "wres.maxThreads";
    /** When system property is absent, multiply count of cores by this */
    public static final int MAX_THREADS_DEFAULT_MULTIPLIER = 8;
    public static final int MAX_THREADS;
    // Figure out the max threads from property or by default rule.
    // Ideally priority order would be: -D, SystemSettings, default rule.
    static
    {
        final String maxThreadsStr = System.getProperty(MAX_THREADS_PROP_NAME);
        // TODO: try setting using SystemSettings first, -DmaxThreads second. Priority goes to -D.
        int maxThreads;
        try
        {
            maxThreads = Integer.parseInt(maxThreadsStr);
        }
        catch(final NumberFormatException nfe)
        {
            maxThreads = Runtime.getRuntime().availableProcessors() * MAX_THREADS_DEFAULT_MULTIPLIER;
            LOGGER.warn("Java -D property {} not set, defaulting Control.MAX_THREADS to {}",
                        MAX_THREADS_PROP_NAME,
                        maxThreads);
        }
        if(maxThreads >= 1)
        {
            MAX_THREADS = maxThreads;
        }
        else
        {
            LOGGER.warn("Java -D property {} was likely less than 1, setting Control.MAX_THREADS to 1",
                        MAX_THREADS_PROP_NAME);
            MAX_THREADS = 1;
        }
    }

    private static final String NEWLINE = System.lineSeparator();

    private static final String SQL_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter SQL_FORMATTER = DateTimeFormatter.ofPattern(SQL_FORMAT);

    /**
     * Processes *existing* pairs non-lazily (lacking specification for ingest). Creates two execution queues for pair
     * processing. The first queue is responsible for retrieving data from the data store. The second queue is
     * responsible for doing something with retrieved data.
     *
     * @param args
     */
    public static void main(final String[] args)
    {
        final PairConfig config = PairConfig.of(LocalDateTime.of(1980, 1, 1, 0, 0),
                                                LocalDateTime.of(1999, 12, 31, 23, 59),
                                                "SQIN",
                                                "QINE",
                                                "CFS");

        final List<Future<SingleValuedPairs>> pairs = new ArrayList<>();

        // Create the first-level Queue of work: fetching pairs.
        // Devote 9/10 of the max threads to working this Q (more waiting/sleeping here)
        final int maxFetchThreads = (ControlTemp.MAX_THREADS / 10) * 9;
        final ExecutorService fetchPairExecutor = Executors.newFixedThreadPool(maxFetchThreads);

        // Queue up fetching the pairs from the database.
        final int leadTimesCount = 2880;
        for(int i = 0; i < leadTimesCount; i++)
        {
            final int leadTime = i + 1;
            final Future<SingleValuedPairs> futurePair = getFuturePairByLeadTime(config, leadTime, fetchPairExecutor);
            pairs.add(futurePair);
        }

        // Create the second-level Queue of work: displaying or processing fetched pairs.
        // Devote 1/10 of the max threads to working this Q (less waiting/sleeping here)
        final int maxProcessThreads = ControlTemp.MAX_THREADS / 10;
        final ExecutorService processPairExecutor = Executors.newFixedThreadPool(maxProcessThreads);

        // Create a third-level Queue of work for executing the verification metrics
        final ExecutorService metricPoolExecutor = Executors.newFixedThreadPool(maxFetchThreads);
        final List<Future<MetricOutputCollection<ScalarOutput>>> futureMetrics = new ArrayList<>();

        // Create builder for an immutable collection of metrics that consume single-valued pairs
        // and produce a scalar output
        MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        // Add some metrics and set the executor service for parallel execution
        builder = builder.add(MetricFactory.ofMeanError())
                         .add(MetricFactory.ofMeanAbsoluteError())
                         .add(MetricFactory.ofRootMeanSquareError())
                         .setExecutorService(metricPoolExecutor);

        // Queue up processing of fetched pairs.
        for(final Future<SingleValuedPairs> nextFuturePairs: pairs)
        {
            // Set the future pairs
            builder.setMetricInput(nextFuturePairs);
            // Build and submit the metric task
            futureMetrics.add(processPairExecutor.submit(builder.build()));
        }

        final Map<Integer, MetricOutputCollection<ScalarOutput>> finalResults = new HashMap<>();
        // Retrieve metric results from processing queue.
        try
        {
            // counting on communication of data with index for this example
            for(int i = 0; i < futureMetrics.size(); i++)
            {
                // get each result
                final MetricOutputCollection<ScalarOutput> metrics = futureMetrics.get(i).get();

                final int leadTime = i + 1;
                finalResults.put(leadTime, metrics);

                if(LOGGER.isInfoEnabled() && fetchPairExecutor instanceof ThreadPoolExecutor
                    && processPairExecutor instanceof ThreadPoolExecutor)
                {
                    final long curTime = System.currentTimeMillis();
                    final long lastTime = lastMessageTime.get();
                    if(curTime - lastTime > LOG_PROGRESS_INTERVAL_MILLIS
                        && lastMessageTime.compareAndSet(lastTime, curTime))
                    {
                        final ThreadPoolExecutor tpeFetch = (ThreadPoolExecutor)fetchPairExecutor;
                        final ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor)processPairExecutor;
                        LOGGER.info("Around {} pair lists fetched. Around {} in the fetch queue. Around {} fetched pairs processed. Around {} in processing queue.",
                                    tpeFetch.getCompletedTaskCount(),
                                    tpeFetch.getQueue().size(),
                                    tpeProcess.getCompletedTaskCount(),
                                    tpeProcess.getQueue().size());
                    }
                }
            }
        }
        catch(final InterruptedException ie)
        {
            LOGGER.error("Interrupted while getting results", ie);
            Thread.currentThread().interrupt();
        }
        catch(final ExecutionException ee)
        {
            LOGGER.error("While getting results", ee);
        }
        finally
        {
            fetchPairExecutor.shutdown();
            processPairExecutor.shutdown();
        }

        if(LOGGER.isInfoEnabled() && fetchPairExecutor instanceof ThreadPoolExecutor
            && processPairExecutor instanceof ThreadPoolExecutor)
        {
            final ThreadPoolExecutor tpeFetch = (ThreadPoolExecutor)fetchPairExecutor;
            final ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor)processPairExecutor;
            LOGGER.info("Total of around {} pair lists completed. Total of around {} pairs processed. Done.",
                        tpeFetch.getCompletedTaskCount(),
                        tpeProcess.getCompletedTaskCount());
        }

        if(LOGGER.isInfoEnabled())
        {
            for(final Map.Entry<Integer, MetricOutputCollection<ScalarOutput>> e: finalResults.entrySet())
            {
                LOGGER.info("For lead time " + e.getKey() + " " + e.getValue().toString());
            }
        }

        shutDownGracefully(fetchPairExecutor, processPairExecutor);
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     * 
     * @param fetchPairExecutor
     * @param processPairExecutor
     */
    private static void shutDownGracefully(final ExecutorService fetchPairExecutor,
                                           final ExecutorService processPairExecutor)
    {
        // (There are probably better ways to do this, e.g. awaitTermination)
        int fetchesSkipped = 0;
        int processingSkipped = 0;
        int i = 0;
        boolean deathTime = false;
        while(!fetchPairExecutor.isTerminated() || !processPairExecutor.isTerminated())
        {
            if(i == 0)
            {
                LOGGER.info("Some processing is finishing up before exit.");
            }

            if(i > 10)
            {
                deathTime = true;
            }

            try
            {
                Thread.currentThread().sleep(500);
            }
            catch(final InterruptedException ie)
            {
                deathTime = true;
                Thread.currentThread().interrupt();
            }
            finally
            {
                if(deathTime)
                {
                    LOGGER.info("Forcing shutdown.");
                    fetchesSkipped += fetchPairExecutor.shutdownNow().size();
                    processingSkipped += processPairExecutor.shutdownNow().size();
                }
            }
            i++;
        }

        if(fetchesSkipped > 0 || processingSkipped > 0)
        {
            LOGGER.info("Abandoned {} pair fetch tasks, abandoned {} processing tasks.",
                        fetchesSkipped,
                        processingSkipped);
        }
    }

    /**
     * Calls executorService.submit on a list of pairs for leadTime with config.
     * 
     * @param config
     * @param leadTime
     * @param executorService
     * @return the Future from the executorService
     */
    private static Future<SingleValuedPairs> getFuturePairByLeadTime(final PairConfig config,
                                                                     final int leadTime,
                                                                     final ExecutorService executorService)
    {
        final PairGetterByLeadTime pair = new PairGetterByLeadTime(config, leadTime);
        return executorService.submit(pair);
    }

    /**
     * Retrieves a list of pairs from the database by lead time.
     */
    private static class PairGetterByLeadTime implements Callable<SingleValuedPairs>
    {
        private final PairConfig config;
        private final int leadTime;

        private PairGetterByLeadTime(final PairConfig config, final int leadTime)
        {
            this.config = config;
            this.leadTime = leadTime;
        }

        @Override
        public SingleValuedPairs call() throws IOException
        {
            final List<PairOfDoubles> result = new ArrayList<>();
            String sql;

            try
            {
                sql = getPairSqlFromConfigForLead(this.config, this.leadTime);
            }
            catch(final IOException ioe)
            {
                LOGGER.error("When trying to build sql for pairs:", ioe);
                throw ioe;
            }

            final DataFactory valueFactory = wres.datamodel.DataFactory.instance();

            //Function to compute the ensemble mean from the VectorOfDoubles that contains the ensemble members
            //This assumes none of the ensemble members correspond to the missing value
            final ToDoubleFunction<VectorOfDoubles> mean = FunctionFactory.mean();

            try (Connection con = Database.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(sql))

            {
                while(resultSet.next())
                {
                    final double observationValue = resultSet.getFloat("observation");
                    final Double[] forecastValues = (Double[])resultSet.getArray("forecasts").getArray();
                    final PairOfDoubles pair =
                                             valueFactory.pairOf(observationValue,
                                                                 mean.applyAsDouble(valueFactory.vectorOf(forecastValues)));

                    LOGGER.trace("Adding a pair with observationValue {} and forecastValues {}",
                                 pair.getItemOne(),
                                 pair.getItemTwo());

                    result.add(pair);
                }
            }
            catch(final SQLException se)
            {
                final String message = "Failed to get pair results for lead " + this.leadTime;
                throw new IOException(message, se);
            }
            finally
            {
                LOGGER.trace("Query: ");
                LOGGER.trace(sql);
            }

            return MetricInputFactory.ofSingleValuedPairs(result, null);
        }
    }

    /**
     * Builds a pairing query based on values already present in the database. Side-effects include reaching out to the
     * database for values, and inserting values to generate ids if they are not already present.
     *
     * @param config configuration information for pairing
     * @param lead the lead time to build this query for.
     * @return a SQL string that will retrieve pairs for the given lead time
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    private static String getPairSqlFromConfigForLead(final PairConfig config, final int lead) throws IOException
    {
        if(config.getForecastVariable() == null || config.getObservationVariable() == null
            || config.getTargetUnit() == null)
        {
            throw new IllegalArgumentException("Forecast and obs variables as well as target unit must be specified");
        }

        long startTime = Long.MAX_VALUE; // used during debug
        if(LOGGER.isDebugEnabled())
        {
            startTime = System.currentTimeMillis();
        }

        final int targetUnitID = ControlTemp.getMeasurementUnitID(config.getTargetUnit());
        final int observationVariableID = ControlTemp.getVariableID(config.getObservationVariable(), targetUnitID);
        final int forecastVariableID = ControlTemp.getVariableID(config.getForecastVariable(), targetUnitID);

        if(LOGGER.isDebugEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving meas unit, fc var, obs var IDs took {}ms", duration);
            startTime = System.currentTimeMillis();
        }

        Integer observationVariablePositionID;
        Integer forecastVariablePositionID;

        final String obsVarPosSql = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = "
            + observationVariableID + ";";
        try
        {
            observationVariablePositionID = Database.getResult(obsVarPosSql, "variableposition_id");
        }
        catch(final SQLException se)
        {
            final String message = "Couldn't retrieve variableposition_id for observation variable "
                + config.getObservationVariable() + " with obs id " + observationVariableID;
            throw new IOException(message, se);
        }

        if(LOGGER.isDebugEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving variableposition_id for obs id {} took {}ms", observationVariableID, duration);
            startTime = System.currentTimeMillis();
        }

        final String fcVarPosSql = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = "
            + forecastVariableID + ";";
        try
        {
            forecastVariablePositionID = Database.getResult(fcVarPosSql, "variableposition_id");
        }
        catch(final SQLException se)
        {
            final String message = "Couldn't retrieve variableposition_id for forecast variable "
                + config.getForecastVariable() + " with fc id " + forecastVariableID;
            throw new IOException(message, se);
        }

        if(LOGGER.isDebugEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Retrieving variableposition_id for fc id {} took {}ms", forecastVariableID, duration);
        }

        final String innerWhere = getPairInnerWhereClauseFromConfig(config);

        return "WITH forecast_measurements AS (" + NEWLINE
            + "    SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE
            + "        array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE + "    FROM wres.Forecast F"
            + NEWLINE + "    INNER JOIN wres.ForecastEnsemble FE" + NEWLINE
            + "        ON F.forecast_id = FE.forecast_id" + NEWLINE + "    INNER JOIN wres.ForecastValue FV" + NEWLINE
            + "        ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE
            + "    INNER JOIN wres.UnitConversion UC" + NEWLINE + "        ON UC.from_unit = FE.measurementunit_id"
            + NEWLINE + "    WHERE lead = " + lead + NEWLINE + "        AND FE.variableposition_id = "
            + forecastVariablePositionID + NEWLINE + "        AND UC.to_unit = " + targetUnitID + NEWLINE
            + "        AND " + innerWhere + NEWLINE + "    GROUP BY forecasted_date" + NEWLINE + ")" + NEWLINE
            + "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE
            + "FROM forecast_measurements FM" + NEWLINE + "INNER JOIN wres.Observation O" + NEWLINE
            + "    ON O.observation_time = FM.forecasted_date" + NEWLINE + "INNER JOIN wres.UnitConversion UC" + NEWLINE
            + "    ON UC.from_unit = O.measurementunit_id" + NEWLINE + "WHERE O.variableposition_id = "
            + observationVariablePositionID + NEWLINE + "    AND UC.to_unit = " + targetUnitID + NEWLINE
            + "ORDER BY FM.forecasted_date;";
    }

    /**
     * The following method is obsolete after refactoring MeasurementCache.getMeasurementUnitID to only throw checked
     * exceptions such as IOException or SQLException, also when MeasurementCache.getMeasurementUnitID gives a
     * meaningful error message. Those are the only two justifications for this wrapper method.
     * 
     * @param unitName the unit we want the ID for
     * @return the result of successful MeasurementCache.getMeasurementUnitID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementCache.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getMeasurementUnitID(final String unitName) throws IOException
    {
        int result;
        try
        {
            result = MeasurementCache.getMeasurementUnitID(unitName);
        }
        catch(final Exception e) // see comment below re: Exception
        {
            if(e instanceof RuntimeException)
            {
                throw (RuntimeException)e;
            }

            final String message = "Couldn't retrieve/set measurement id for unit id " + unitName;
            throw new IOException(message, e);
        }
        return result;
    }

    /**
     * The following method becomes obsolete after refactoring VariableCache.getVariableID to only throw checked
     * exceptions such as IOException or SQLException, also when VariableCache.getVariableID gives a meaningful error
     * message. Those are the only two justifications for this wrapper method.
     * 
     * @param variableName the variable name to look for
     * @param measurementUnitId the targeted measurement unit id
     * @return the result of successful VariableCache.getVariableID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementCache.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getVariableID(final String variableName, final int measurementUnitId) throws IOException
    {
        int result;
        try
        {
            result = VariableCache.getVariableID(variableName, measurementUnitId);
        }
        catch(final Exception e) // see comment below re: Exception
        {
            if(e instanceof RuntimeException)
            {
                throw (RuntimeException)e;
            }

            final String message = "Couldn't retrieve/set variableid for " + variableName;
            throw new IOException(message, e);
        }
        return result;
    }

    private static String getPairInnerWhereClauseFromConfig(final PairConfig config)
    {
        if(config.getFromTime() == null && config.getToTime() == null)
        {
            return "";
        }
        else if(config.getFromTime() == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) <= '" + config.getToTime().format(SQL_FORMATTER) + "'";
        }
        else if(config.getToTime() == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) >= '" + config.getFromTime().format(SQL_FORMATTER)
                + "'";
        }
        else
        {
            return "((F.forecast_date + INTERVAL '1 hour' * lead) >= '" + config.getFromTime().format(SQL_FORMATTER)
                + "'" + NEWLINE + "             AND (F.forecast_date + INTERVAL '1 hour' * lead) <= '"
                + config.getToTime().format(SQL_FORMATTER) + "')";
        }
    }

    /**
     * All properties optional, any/each can be null.
     */
    private static class PairConfig
    {
        private final LocalDateTime fromTime;
        private final LocalDateTime toTime;
        private final String forecastVariable;
        private final String observationVariable;
        private final String targetUnit;

        private PairConfig(final LocalDateTime fromTime,
                           final LocalDateTime toTime,
                           final String forecastVariable,
                           final String observationVariable,
                           final String targetUnit)
        {
            this.fromTime = fromTime;
            this.toTime = toTime;
            this.forecastVariable = forecastVariable;
            this.observationVariable = observationVariable;
            this.targetUnit = targetUnit;
        }

        public static PairConfig of(final LocalDateTime fromTime,
                                    final LocalDateTime toTime,
                                    final String forecastVariable,
                                    final String observationVariable,
                                    final String targetUnit)
        {
            return new PairConfig(fromTime, toTime, forecastVariable, observationVariable, targetUnit);
        }

        LocalDateTime getFromTime()
        {
            return this.fromTime;
        }

        LocalDateTime getToTime()
        {
            return this.toTime;
        }

        String getForecastVariable()
        {
            return this.forecastVariable;
        }

        String getObservationVariable()
        {
            return this.observationVariable;
        }

        String getTargetUnit()
        {
            return this.targetUnit;
        }
    }
}
