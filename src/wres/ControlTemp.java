package wres;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.*;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.io.Operations;
import wres.io.config.ProjectConfigPlus;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.io.utilities.InputGenerator;
import wres.util.Strings;

import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

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
    private static final DataFactory dataFac = DefaultDataFactory.getInstance();

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
     * @param args input args
     */
    public static void main(final String[] args)
    {

        ProjectConfig config = null;
        try
        {

            config = ProjectConfigPlus.from(Paths.get("D:/Applications/WRES/Code/wres/nonsrc/config_possibility.xml"))
                                      .getProjectConfig();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        final long start = System.currentTimeMillis(); //Start time

        //Obtain the features
        List<Conditions.Feature> features = config.getConditions().getFeature();

        //Iterate through the features
        try
        {
            //Some output types are processed at the end of the pipeline, others after each input is processed 
            //Construct a processor that retains all output types required at the end of the pipeline: SCALAR and VECTOR      
            MetricProcessor processor = MetricFactory.getInstance(dataFac).getMetricProcessor(config,
                                                                                              MetricOutputGroup.SCALAR,
                                                                                              MetricOutputGroup.VECTOR);

            Map<Conditions.Feature, LinkedList<Future<MetricInput<?>>>> featureMetricInputs = new HashMap<>();

            // Job dispatch per feature per lead occurs first to ensure that there are as many jobs as possible running
            // while generating new retrieval jobs
            for(Conditions.Feature nextFeature: features)
            {
                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Gathering pairs for '" + nextFeature.getLocation().getLid() + "'.");
                }

                featureMetricInputs.put(nextFeature, new LinkedList<>());
                InputGenerator metricInputs = Operations.getInputs(config, nextFeature);
                metricInputs.forEach(featureMetricInputs.get(nextFeature)::add);
            }

            // Once all pair retrieval jobs have been dispatched, cycle through, obtain the pairs, and pass them through
            // the metric processors.
            for (Map.Entry<Conditions.Feature, LinkedList<Future<MetricInput<?>>>> featureMetricInput : featureMetricInputs.entrySet())
            {

                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Processing feature '" + featureMetricInput.getKey().getLocation().getLid() + "'.");
                }

                //Iterate through the inputs and compute all metrics for each input
                int window = 1;

                Future<MetricInput<?>> input;

                // Remove the inputs as they are read to lessen the load on memory
                while((input = featureMetricInput.getValue().poll()) != null)
                {
                    if(LOGGER.isInfoEnabled())
                    {
                        LOGGER.info("Processing lead time '" + String.valueOf(window) + "'.");
                    }

                    MetricOutputForProjectByLeadThreshold nextResult = null;
                    try {
                        processor.apply(input.get());
                    }
                    catch (InterruptedException e) {
                        LOGGER.error("MetricInput generation was interupted.");
                        LOGGER.error(Strings.getStackTrace(e));
                        continue;
                    }
                    catch (ExecutionException e)
                    {
                        LOGGER.error("MetricInput generation was aborted by throwing an exception.");
                        LOGGER.error(Strings.getStackTrace(e));
                        continue;
                    }

                    if (nextResult.hasOutput(MetricOutputGroup.MULTIVECTOR))
                    {
                        try {
                            MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> forProducts =
                                    nextResult.getMultiVectorOutput();

                            //Call wres-vis factory with intermediate output
                        }
                        catch (InterruptedException e) {
                            LOGGER.error("Multivector metric output generation was interupted.");
                            LOGGER.error(Strings.getStackTrace(e));
                            continue;
                        }
                        catch (ExecutionException e) {
                            LOGGER.error("Multivector metric output generation was aborted by throwing an exception.");
                            LOGGER.error(Strings.getStackTrace(e));
                            continue;
                        }
                    }
                    if(LOGGER.isInfoEnabled())
                    {
                        LOGGER.info("Completed lead time '" + String.valueOf(window) + "'.");
                    }

                    window++;
                    input = null;
                }
            }
        }
        catch(MetricConfigurationException e)
        {
            LOGGER.error(e.getMessage(), e);
        }

        final long stop = System.currentTimeMillis(); //End time
        if(LOGGER.isInfoEnabled())
        {
            LOGGER.info("Completed verification in " + ((stop - start) / 1000.0) + " seconds.");
        }

//        final long start = System.currentTimeMillis(); //Start time
//        final PairConfig config = PairConfig.of(LocalDateTime.of(1980, 1, 1, 1, 0),
//                                                LocalDateTime.of(2010, 12, 31, 23, 59),
//                                                "SQIN",
//                                                "QINE",
//                                                "CFS");
//
//        //Build an immutable collection of metrics, to be computed at each of several forecast lead times
//        final MetricFactory metFac = MetricFactory.getInstance(dataFac);
//        final MetricCollection<SingleValuedPairs, ScalarOutput> useMe =
//                                                                      metFac.ofSingleValuedScalarCollection(MetricConstants.MEAN_ERROR,
//                                                                                                            MetricConstants.MEAN_ABSOLUTE_ERROR,
//                                                                                                            MetricConstants.ROOT_MEAN_SQUARE_ERROR);
//
//        // Queue the various tasks by lead time (lead time is the pooling dimension for metric calculation here)
//        final List<CompletableFuture<?>> listOfFutures = new ArrayList<>(); //List of futures to test for completion
//        final int leadTimesCount = 100;
//
//        //Build a thread pool that can be terminated upon exception
//        final ForkJoinPool f = new ForkJoinPool();
//
//        //Sink for the results: the results are added incrementally to an immutable store via a builder
//        final MetricOutputMultiMapByLeadThreshold.Builder<ScalarOutput> resultsBuilder = dataFac.ofMultiMap();
//        //Iterate
//        for(int i = 0; i < leadTimesCount; i++)
//        {
//            final int lead = i + 1;
//            // Complete all tasks asynchronously:
//            // 1. Get some pairs from the database 
//            // 2. When available, compute the single-valued pairs from them ({observation, ensemble mean})
//            // 3. Compute the metrics
//            // 4. Do something with the verification results (store them)
//            // 5. Monitor progress per lead time
//
//            //Threshold = all data for now
//            final Threshold allData = dataFac.getThreshold(Double.NEGATIVE_INFINITY, Condition.GREATER);
//            final CompletableFuture<Void> c = CompletableFuture
//                                                               .supplyAsync(new PairGetterByLeadTime(config,
//                                                                                                     lead,
//                                                                                                     dataFac),
//                                                                            f)
//                                                               .thenApplyAsync(new SingleValuedPairProcessor(dataFac),
//                                                                               f)
//                                                               .thenApplyAsync(useMe, f)
//                                                               .thenAcceptAsync(new ResultProcessor<>(lead,
//                                                                                                      allData,
//                                                                                                      resultsBuilder),
//                                                                                f)
//                                                               .thenAcceptAsync(a -> {
//                                                                   if(LOGGER.isInfoEnabled())
//                                                                   {
//                                                                       LOGGER.info("Completed lead time " + lead);
//                                                                   }
//                                                               }, f);
//            //Add the future to the list
//            listOfFutures.add(c);
//        }
//
//        //Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
//        //Log any exception and append to the error log as the last step
//        Exception logged = null;
//        try
//        {
//            doAllOrException(listOfFutures).join();
//        }
//        catch(final Exception e)
//        {
//            logged = e;
//        }
//        finally
//        {
//            //Terminate
//            f.shutdownNow(); //or shutdown();
//        }
//
//        //Build final results: 
//        MetricOutputMultiMapByLeadThreshold<ScalarOutput> results = resultsBuilder.build();
//
//        //Print info to logger
//        if(LOGGER.isInfoEnabled())
//        {
//            results.forEach((key, value) -> {
//                LOGGER.info(NEWLINE + "Results for metric "
//                    + dataFac.getMetadataFactory().getMetricName(key.getFirstKey()) + " (lead time, threshold, score) "
//                    + NEWLINE + value);
//            });
//            final long stop = System.currentTimeMillis(); //End time
//            LOGGER.info("Completed verification in " + ((stop - start) / 1000.0) + " seconds.");
//        }
//        //Print exception to logger last
//        if(!Objects.isNull(logged))
//        {
//            LOGGER.error(logged.getMessage(), logged);
//        }
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     * 
     * @param futures the futures to compose
     * @return the composed futures
     */

    private static CompletableFuture<?> doAllOrException(final List<CompletableFuture<?>> futures)
    {
        //Complete when all futures are completed
        final CompletableFuture<?> allDone =
                                           CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        //Complete when any of the underlying futures completes exceptionally
        final CompletableFuture<?> oneExceptional = new CompletableFuture<>();
        //Link the two
        for(final CompletableFuture<?> completableFuture: futures)
        {
            //When one completes exceptionally, propagate
            completableFuture.exceptionally(exception -> {
                oneExceptional.completeExceptionally(exception);
                return null;
            });
        }
        //Either all done OR one completes exceptionally
        return CompletableFuture.anyOf(allDone, oneExceptional);
    }

    /**
     * Computes the ensemble mean from the input ensemble forecasts and creates a set of {@link SingleValuedPairs}.
     */

    private static class SingleValuedPairProcessor
    implements Function<List<PairOfDoubleAndVectorOfDoubles>, SingleValuedPairs>
    {

        private final DataFactory metIn;

        public SingleValuedPairProcessor(final DataFactory metIn)
        {
            this.metIn = metIn;
        }

        @Override
        public SingleValuedPairs apply(final List<PairOfDoubleAndVectorOfDoubles> t)
        {
            final ToDoubleFunction<VectorOfDoubles> mean = FunctionFactory.mean();
            final List<PairOfDoubles> returnMe = new ArrayList<>();
            for(final PairOfDoubleAndVectorOfDoubles nextPair: t)
            {
                final PairOfDoubles pair = metIn.pairOf(nextPair.getItemOne(),
                                                        mean.applyAsDouble(metIn.vectorOf(nextPair.getItemTwo())));
                returnMe.add(pair);
            }
            return metIn.ofSingleValuedPairs(returnMe, metIn.getMetadataFactory().getMetadata());
        }
    }

    /**
     * A function that does something with a set of results (in this case, prints to a logger).
     */

    private static class ResultProcessor<S extends MetricOutput<?>> implements Consumer<MetricOutputMapByMetric<S>>
    {

        /**
         * Forecast lead time.
         */
        private final int leadTime;

        /**
         * Threshold.
         */

        private final Threshold threshold;

        /**
         * A store of the results.
         */

        private final MetricOutputMultiMapByLeadThreshold.Builder<S> builder;

        /**
         * Construct the processor with a lead time.
         * 
         * @param leadTime the forecast lead time
         */
        public ResultProcessor(final int leadTime,
                               final Threshold threshold,
                               final MetricOutputMultiMapByLeadThreshold.Builder<S> builder)
        {
            this.threshold = threshold;
            this.leadTime = leadTime;
            this.builder = builder;
        }

        @Override
        public void accept(final MetricOutputMapByMetric<S> t)
        {
            builder.put(leadTime, threshold, t);
        }
    }

    /**
     * Retrieves a list of pairs from the database by lead time.
     */
    private static class PairGetterByLeadTime implements Supplier<List<PairOfDoubleAndVectorOfDoubles>>
    {
        private final PairConfig config;
        private final int leadTime;
        private final DataFactory metIn;

        private PairGetterByLeadTime(final PairConfig config, final int leadTime, final DataFactory metIn)
        {
            this.config = config;
            this.leadTime = leadTime;
            this.metIn = metIn;
        }

        @Override
        public List<PairOfDoubleAndVectorOfDoubles> get()
        {

//START EXCEPTION EXAMPLE            
//            //Demonstrates exception handling
//            if(leadTime == 1000)
//            {
//                throw new PairException("while generating pairs at lead time " + leadTime);
//            }
//END EXCEPTION EXAMPLE            

//COMMENT THIS LINE WHEN DATABASE IS WORKING            
//            return getImaginaryPairsTemp();

//UNCOMMENT FROM HERE ONWARDS WHEN DATABASE IS WORKING                  

            final List<PairOfDoubleAndVectorOfDoubles> result = new ArrayList<>();
            String sql = "";
            try
            {
                sql = getPairSqlFromConfigForLead(this.config, this.leadTime);
            }
            catch(final IOException ioe)
            {
                LOGGER.error("When trying to build sql for pairs:", ioe);
                //throw ioe;
            }
            if(LOGGER.isInfoEnabled())
            {
                LOGGER.info("Requesting pairs for lead time " + leadTime + ".");
            }
            //final  valueFactory = wres.datamodel.DataFactory.instance();
            try (Connection con = Database.getConnection(); ResultSet resultSet = Database.getResults(con, sql))
            {
                while(resultSet.next())
                {
                    final double observationValue = resultSet.getFloat("observation");
                    final Double[] forecastValues = (Double[])resultSet.getArray("forecasts").getArray();
                    final PairOfDoubleAndVectorOfDoubles pair = metIn.pairOf(observationValue, forecastValues);
                    LOGGER.trace("Adding a pair with observationValue {} and forecastValues {}",
                                 pair.getItemOne(),
                                 pair.getItemTwo());
                    result.add(pair);
                }
            }
            catch(final SQLException se)
            {
                LOGGER.error("Failed to get pair results for lead " + this.leadTime, se);
                //throw new IOException(message, se);
            }
            finally
            {
                LOGGER.trace("Query: ");
                LOGGER.trace(sql);
            }
            return result;
        }
    }

    /**
     * Returns a moderately-sized (10k) test dataset of pairs comprising an observation and a vector of ensemble
     * forecast values.
     * 
     * @return a set of pairs
     */

    private static List<PairOfDoubleAndVectorOfDoubles> getImaginaryPairsTemp()
    {
        //Construct some single-valued pairs
        final DataFactory dataFactory = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        final double[] ensemble = new double[50];
        //Add 50 members with the same value
        for(int i = 0; i < ensemble.length; i++)
        {
            ensemble[i] = 10;
        }
        //Add 10k pairs
        for(int i = 0; i < 10000; i++)
        {
            //Add an ensemble forecast
            values.add(dataFactory.pairOf(5.0, ensemble));
        }
        return values;
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
        final String partA = "WITH forecast_measurements AS (" + NEWLINE
            + "    SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE
            + "        array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE + "    FROM wres.Forecast F"
            + NEWLINE + "    INNER JOIN wres.ForecastEnsemble FE" + NEWLINE
            + "        ON F.forecast_id = FE.forecast_id" + NEWLINE + "    INNER JOIN wres.ForecastValue FV" + NEWLINE
            + "        ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE
            + "    INNER JOIN wres.UnitConversion UC" + NEWLINE + "        ON UC.from_unit = FE.measurementunit_id"
            + NEWLINE + "    WHERE lead = " + lead + NEWLINE + "        AND FE.variableposition_id = "
            + forecastVariablePositionID + NEWLINE + "        AND UC.to_unit = " + targetUnitID + NEWLINE;
        String partB = "";
        if(innerWhere.length() > 0)
        {
            partB += "        AND " + innerWhere + NEWLINE;
        }
        partB += "    GROUP BY forecasted_date" + NEWLINE + ")" + NEWLINE
            + "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE
            + "FROM forecast_measurements FM" + NEWLINE + "INNER JOIN wres.Observation O" + NEWLINE
            + "    ON O.observation_time = FM.forecasted_date" + NEWLINE + "INNER JOIN wres.UnitConversion UC" + NEWLINE
            + "    ON UC.from_unit = O.measurementunit_id" + NEWLINE + "WHERE O.variableposition_id = "
            + observationVariablePositionID + NEWLINE + "    AND UC.to_unit = " + targetUnitID + NEWLINE
            + "ORDER BY FM.forecasted_date;";

        return partA + partB;
    }

    /**
     * The following method is obsolete after refactoring MeasurementUnits.getMeasurementUnitID to only throw checked
     * exceptions such as IOException or SQLException, also when MeasurementUnits.getMeasurementUnitID gives a
     * meaningful error message. Those are the only two justifications for this wrapper method.
     * 
     * @param unitName the unit we want the ID for
     * @return the result of successful MeasurementUnits.getMeasurementUnitID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementUnits.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getMeasurementUnitID(final String unitName) throws IOException
    {
        int result;
        try
        {
            result = MeasurementUnits.getMeasurementUnitID(unitName);
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
     * The following method becomes obsolete after refactoring Variables.getVariableID to only throw checked exceptions
     * such as IOException or SQLException, also when Variables.getVariableID gives a meaningful error message. Those
     * are the only two justifications for this wrapper method.
     * 
     * @param variableName the variable name to look for
     * @param measurementUnitId the targeted measurement unit id
     * @return the result of successful Variables.getVariableID
     * @throws IOException when any checked (aka non-RuntimeException) exception occurs
     */
    // TODO: refactor MeasurementUnits.getMeasurementUnitID to declare it throws only checked exceptions, not Exception, then remove:
    private static int getVariableID(final String variableName, final int measurementUnitId) throws IOException
    {
        int result;
        try
        {
            result = Variables.getVariableID(variableName, measurementUnitId);
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
