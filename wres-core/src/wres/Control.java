package wres;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.xml.bind.ValidationEvent;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.SafePairOfDoubleAndVectorOfDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.metric.*;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.io.config.ProjectConfigPlus;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.utilities.Database;
import wres.vis.ChartEngineFactory;

import static java.nio.file.Files.probeContentType;

/**
 * Another way to execute a project.
 */
public class Control implements Function<String[], Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);
    public static final long LOG_PROGRESS_INTERVAL_MILLIS = 2000;
    private static final AtomicLong lastMessageTime = new AtomicLong();
    private static final AtomicBoolean messagedForEarliestDate = new AtomicBoolean(false);
    private static final AtomicBoolean messagedForLatestDate = new AtomicBoolean(false);
    private static final AtomicBoolean messagedSqlStatement = new AtomicBoolean(false);

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
            //LOGGER.warn("Java -D property {} not set, defaulting Control.MAX_THREADS to {}",
            //            MAX_THREADS_PROP_NAME,
            //            maxThreads);
        }
        if(maxThreads >= 1)
        {
            MAX_THREADS = maxThreads;
        }
        else
        {
            //LOGGER.warn("Java -D property {} was likely less than 1, setting Control.MAX_THREADS to 1",
            //            MAX_THREADS_PROP_NAME);
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
    public Integer apply(final String[] args)
    {
        List<ProjectConfigPlus> projectConfiggies = getProjects(args);

        if (projectConfiggies.isEmpty())
        {
            LOGGER.error("Validate project configuration files and pass them on the command line like this: wres executeConfigProject c:/path/to/config1.xml c:/path/to/config2.xml");
            return null;
        }
        else
        {
            LOGGER.info("Found {} valid projects, beginning execution", projectConfiggies.size());
        }

        // Create the first-level Queue of work: fetching pairs.
        // Devote 9/10 of the max threads to working this Q (more waiting/sleeping here)
        int maxFetchThreads = (Control.MAX_THREADS / 10) * 9;
        ExecutorService fetchPairExecutor = Executors.newFixedThreadPool(maxFetchThreads);

        // Create the second-level Queue of work: displaying or processing fetched pairs.
        // Devote 1/10 of the max threads to working this Q (less waiting/sleeping here)
        int maxProcessThreads = Control.MAX_THREADS / 10;
        ExecutorService processPairExecutor = Executors.newFixedThreadPool(maxProcessThreads);

        for (ProjectConfigPlus projectConfigPlus : projectConfiggies)
        {
            ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

            List<Future<List<PairOfDoubleAndVectorOfDoubles>>> pairs = new ArrayList<>();

            // Queue up fetching the pairs from the database.
            int leadTimesCount = 2880;
            for (int i = 0; i < leadTimesCount; i++)
            {
                int leadTime = i + 1;
                Future<List<PairOfDoubleAndVectorOfDoubles>> futurePair =
                        getFuturePairByLeadTime(projectConfig, leadTime, fetchPairExecutor);
                pairs.add(futurePair);
            }

            List<Future<MetricOutputMapByMetric<ScalarOutput>>> futureMetrics = new ArrayList<>();

            // Queue up processing of fetched pairs.
            for (int i = 0; i < pairs.size(); i++)
            {
                // Here, using index in list to communicate the lead time.
                // Another structure might be appropriate, for example,
                // see getPairs in
                // wres.io.config.specification.MetricSpecification
                // which uses a Map.
                int leadTime = i + 1;
                PairsByLeadProcessor processTask =
                        new PairsByLeadProcessor(pairs.get(i),
                                                 projectConfig,
                                                 leadTime);

                Future<MetricOutputMapByMetric<ScalarOutput>> futureMetric =
                        processPairExecutor.submit(processTask);
                futureMetrics.add(futureMetric);
            }

            Map<Integer, MetricOutputMapByMetric<ScalarOutput>> finalResults = new HashMap<>();
            // Retrieve metric results from processing queue.
            try
            {
                // counting on communication of data with index for this example
                for (int i = 0; i < futureMetrics.size(); i++)
                {
                    // get each result
                    MetricOutputMapByMetric<ScalarOutput> metrics = futureMetrics.get(i).get();

                    int leadTime = i + 1;
                    finalResults.put(leadTime, metrics);

                    if (LOGGER.isInfoEnabled() && fetchPairExecutor instanceof ThreadPoolExecutor
                            && processPairExecutor instanceof ThreadPoolExecutor)
                    {
                        long curTime = System.currentTimeMillis();
                        long lastTime = lastMessageTime.get();
                        if (curTime - lastTime > LOG_PROGRESS_INTERVAL_MILLIS && lastMessageTime
                                .compareAndSet(lastTime, curTime))
                        {
                            ThreadPoolExecutor tpeFetch = (ThreadPoolExecutor) fetchPairExecutor;
                            ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor) processPairExecutor;
                            LOGGER.info(
                                    "Around {} pair lists fetched. Around {} in the fetch queue. Around {} fetched pairs processed. Around {} in processing queue.",
                                    tpeFetch.getCompletedTaskCount(),
                                    tpeFetch.getQueue().size(), tpeProcess.getCompletedTaskCount(),
                                    tpeProcess.getQueue().size());
                        }
                    }
                }
            }
            catch (InterruptedException ie)
            {
                LOGGER.error("Interrupted while getting results", ie);
                Thread.currentThread().interrupt();
                break;
            }
            catch (ExecutionException ee)
            {
                LOGGER.error("While getting results", ee);
                break;
            }
            finally
            {
                fetchPairExecutor.shutdown();
                processPairExecutor.shutdown();
            }

            if (LOGGER.isInfoEnabled() && fetchPairExecutor instanceof ThreadPoolExecutor
                    && processPairExecutor instanceof ThreadPoolExecutor)
            {
                ThreadPoolExecutor tpeFetch = (ThreadPoolExecutor) fetchPairExecutor;
                ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor) processPairExecutor;
                LOGGER.info(
                        "Total of around {} pair lists completed. Total of around {} pairs processed. Done.",
                        tpeFetch.getCompletedTaskCount(), tpeProcess.getCompletedTaskCount());
            }

            if (LOGGER.isInfoEnabled())
            {
                for (final Map.Entry<Integer, MetricOutputMapByMetric<ScalarOutput>> e : finalResults
                        .entrySet())
                {
                    LOGGER.info(
                            "For lead time " + e.getKey() + " " + e.getValue().toString());
                }
            }
        }
        shutDownGracefully(fetchPairExecutor, processPairExecutor);

        return 0;
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
     * Get project configurations from command line file args.
     *
     * @param args
     * @return the successfully found, read, unmarshalled project configs
     */
    private List<ProjectConfigPlus> getProjects(String[] args)
    {
        List<Path> existingProjectFiles = new ArrayList<>();

        for (String arg : args)
        {
            Path path = Paths.get(arg);
            if (Files.exists(path))
            {
                existingProjectFiles.add(path);
            }
            else
            {
                LOGGER.warn("Project configuration file {} does not exist!", path);
            }
        }

        List<ProjectConfigPlus> projectConfiggies = new ArrayList<>();

        for (Path path : existingProjectFiles)
        {
            try
            {
                ProjectConfigPlus projectConfigPlus = ProjectConfigPlus.from(path);
                projectConfiggies.add(projectConfigPlus);
            }
            catch (IOException ioe)
            {
                LOGGER.error("Could not read project configuration: ", ioe);
            }
        }
        return projectConfiggies;
    }

    /**
     * Task whose job is to wait for pairs to arrive, then run metrics on them.
     */
    private static class PairsByLeadProcessor implements Callable<MetricOutputMapByMetric<ScalarOutput>>
    {
        private final Future<List<PairOfDoubleAndVectorOfDoubles>> futurePair;
        private final ProjectConfig projectConfig;
        private final int leadTime;

        private PairsByLeadProcessor(final Future<List<PairOfDoubleAndVectorOfDoubles>> futurePairs,
                                     final ProjectConfig projectConfig,
                                     final int leadTime)
        {
            this.futurePair = futurePairs;
            this.projectConfig = projectConfig;
            this.leadTime = leadTime;
        }

        @Override
        public MetricOutputMapByMetric<ScalarOutput> call()
                throws ProcessingException, ChartEngineException,
                GenericXMLReadingHandlerException, XYChartDataSourceException,
                IOException, URISyntaxException
        {
            // initialized to empty list in case of failure
            List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();

            // Wait for the result from whichever queue is executing it.
            try
            {
                pairs = futurePair.get();
            }
            catch(final InterruptedException ie)
            {
                LOGGER.error("Interrupted while getting pair for lead time {}", this.leadTime);
                Thread.currentThread().interrupt();
            }
            catch(final ExecutionException ee)
            {
                // This is when execution from the upstream queue has failed.
                // Propagate an exception back up.
                final String message = "While getting pair for lead time " + this.leadTime;
                throw new ProcessingException(message, ee);
            }

            // Grow a List of PairOfDoubleAndVectorOfDoubles into a simpler
            // List of PairOfDouble for metric calculation.
            final List<PairOfDoubles> simplePairs = Slicer.getFlatDoublePairs(pairs);

            // What follows for the rest of call() was originally from MetricCollectionTest.

            // Convert pairs into metric input
            final MetricInputFactory inputFactory = DefaultMetricInputFactory.getInstance();
            final MetricOutputFactory outputFactory = DefaultMetricOutputFactory.getInstance();
            final MetadataFactory metFac = inputFactory.getMetadataFactory();
            final Metadata meta = metFac.getMetadata(simplePairs.size(),
                    metFac.getDimension(projectConfig.getPair().getUnit()),
                    metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
            final SingleValuedPairs input = inputFactory.ofSingleValuedPairs(simplePairs,
                                                                             meta);

            // generate some graphics, this is almost certainly not where we
            // will do it, but it was a first "go" at integrating graphics into
            // the pipeline.
            try
            {
                ChartEngine ce = ChartEngineFactory
                        .buildSingleValuedPairsChartEngine(input,
                                "singleValuedPairsTemplate.xml",
                                null);

                //Generate the output file.
                ChartTools.generateOutputImageFile(
                        Paths.get(new URI(projectConfig.getOutputs().getDestination().get(1).getPath() + "/" + Integer.toString(leadTime) + ".png")).toFile(),
                        ce.buildChart(),
                        projectConfig.getOutputs().getDestination().get(1).getGraphical().getWidth(),
                        projectConfig.getOutputs().getDestination().get(1).getGraphical().getHeight());
            }
            catch (GenericXMLReadingHandlerException
                    |ChartEngineException
                    |XYChartDataSourceException
                    |URISyntaxException
                    |IOException e)
            {
                LOGGER.error("Could not create charts.", e);
                throw e;
            }

            // Create an immutable collection of metrics that consume single-valued pairs
            // and produce a scalar output
            //Build an immutable collection of metrics, to be computed at each of several forecast lead times
            final MetricFactory metricFactory = MetricFactory.getInstance(outputFactory);
            final MetricCollection<SingleValuedPairs, ScalarOutput> collection =
                                                                               metricFactory.ofSingleValuedScalarCollection(MetricConstants.MEAN_ERROR,
                                                                                                                            MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                                                                            MetricConstants.ROOT_MEAN_SQUARE_ERROR);
            //Compute sequentially (i.e. not in parallel)
            return collection.apply(input);
        }
    }

    /**
     * A label for our own checked exception distinct from ExecutionException which can be thrown by an executor while
     * getting a task. Might help distinguish exceptions in Q1 from exceptions in Q2
     */
    private static class ProcessingException extends Exception
    {
        public ProcessingException(final String s, final Throwable t)
        {
            super(s, t);
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
    private static Future<List<PairOfDoubleAndVectorOfDoubles>> getFuturePairByLeadTime(final ProjectConfig config,
                                                                                        final int leadTime,
                                                                                        final ExecutorService executorService)
    {
        final PairGetterByLeadTime pair = new PairGetterByLeadTime(config, leadTime);
        return executorService.submit(pair);
    }

    /**
     * Retrieves a list of pairs from the database by lead time.
     */
    private static class PairGetterByLeadTime implements Callable<List<PairOfDoubleAndVectorOfDoubles>>
    {
        private final ProjectConfig config;
        private final int leadTime;

        private PairGetterByLeadTime(final ProjectConfig config, final int leadTime)
        {
            this.config = config;
            this.leadTime = leadTime;
        }

        @Override
        public List<PairOfDoubleAndVectorOfDoubles> call() throws SQLException
        {
            final List<PairOfDoubleAndVectorOfDoubles> result = new ArrayList<>();
            String sql;

            try
            {
                sql = getPairSqlFromConfigForLead(this.config, this.leadTime);
                if (LOGGER.isDebugEnabled() && !messagedSqlStatement.getAndSet(true))
                {
                    LOGGER.debug("SQL query: {}", sql);
                }
            }
            catch(SQLException e)
            {
                LOGGER.error("When trying to build sql for pairs:", e);
                throw e;
            }

            try (Connection con = Database.getConnection();
            Statement statement = con.createStatement();
            ResultSet resultSet = statement.executeQuery(sql))
            {
                while(resultSet.next())
                {
                    final double observationValue = resultSet.getFloat("observation");
                    final Double[] forecastValues = (Double[])resultSet.getArray("forecasts").getArray();
                    final PairOfDoubleAndVectorOfDoubles pair =
                            SafePairOfDoubleAndVectorOfDoubles.of(observationValue, forecastValues);

                    LOGGER.trace("Adding a pair with observationValue {} and forecastValues {}",
                                 pair.getItemOne(),
                                 pair.getItemTwo());

                    result.add(pair);
                }
            }
            catch(SQLException se)
            {
                String message = "Failed to get pair results for lead " + this.leadTime + " using this query: " + sql;
                throw new SQLException(message, se);
            }

            return result;
        }
    }

    /**
     * Builds a pairing query based on values already present in the database. Side-effects include reaching out to the
     * database for values, and inserting values to generate ids if they are not already present.
     *
     * @param config configuration information for pairing
     * @param lead the lead time to build this query for.
     * @return a SQL string that will retrieve pairs for the given lead time
     * @throws SQLException when any checked (aka non-RuntimeException) exception occurs
     */
    private static String getPairSqlFromConfigForLead(final ProjectConfig config, final int lead) throws SQLException
    {
        if(config.getInputs() == null
                || config.getInputs().getLeft() == null
                || config.getInputs().getLeft().getVariable() == null
                || config.getInputs().getLeft().getVariable().getValue() == null
                || config.getInputs().getRight() == null
                || config.getInputs().getRight().getVariable() == null
                || config.getInputs().getRight().getVariable().getValue() == null
                || config.getPair() == null
                || config.getPair().getUnit() == null)
        {
            throw new IllegalArgumentException("Forecast and obs variables as well as target unit must be specified.");
        }

        final String observationVariableName = config.getInputs()
                                                     .getLeft()
                                                     .getVariable()
                                                     .getValue();

        final String forecastVariableName = config.getInputs()
                                                  .getRight()
                                                  .getVariable()
                                                  .getValue();

        long startTime = Long.MAX_VALUE; // used during debug
        if(LOGGER.isTraceEnabled())
        {
            startTime = System.currentTimeMillis();
        }

        int targetUnitID = MeasurementUnits.getMeasurementUnitID(config.getPair().getUnit());
        int observationVariableID = Variables.getVariableID(observationVariableName, targetUnitID);
        int forecastVariableID = Variables.getVariableID(forecastVariableName, targetUnitID);

        if(LOGGER.isTraceEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.trace("Retrieving meas unit, fc var, obs var IDs took {}ms", duration);
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
        catch(SQLException se)
        {
            final String message = "Couldn't retrieve variableposition_id for observation variable "
                + observationVariableName + " with obs id " + observationVariableID;
            throw new SQLException(message, se);
        }

        if(LOGGER.isTraceEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.trace("Retrieving variableposition_id for obs id {} took {}ms", observationVariableID, duration);
            startTime = System.currentTimeMillis();
        }

        final String fcVarPosSql = "SELECT variableposition_id FROM wres.VariablePosition WHERE variable_id = "
            + forecastVariableID + ";";
        try
        {
            forecastVariablePositionID = Database.getResult(fcVarPosSql, "variableposition_id");
        }
        catch(SQLException se)
        {
            String message = "Couldn't retrieve variableposition_id for forecast variable "
                + forecastVariableName + " with fc id " + forecastVariableID;
            throw new SQLException(message, se);
        }

        if(LOGGER.isTraceEnabled())
        {
            final long duration = System.currentTimeMillis() - startTime;
            LOGGER.trace("Retrieving variableposition_id for fc id {} took {}ms", forecastVariableID, duration);
        }

        final String innerWhere = getPairInnerWhereClauseFromConfig(config);

        final String partA = "WITH forecast_measurements AS (" + NEWLINE
                + "    SELECT F.forecast_date + INTERVAL '1 hour' * lead AS forecasted_date," + NEWLINE
                + "        array_agg(FV.forecasted_value * UC.factor) AS forecasts" + NEWLINE
                + "    FROM wres.Forecast F" + NEWLINE
                + "    INNER JOIN wres.ForecastEnsemble FE" + NEWLINE
                + "        ON F.forecast_id = FE.forecast_id" + NEWLINE
                + "    INNER JOIN wres.ForecastValue FV" + NEWLINE
                + "        ON FV.forecastensemble_id = FE.forecastensemble_id" + NEWLINE
                + "    INNER JOIN wres.UnitConversion UC" + NEWLINE
                + "        ON UC.from_unit = FE.measurementunit_id" + NEWLINE
                + "    WHERE lead = " + lead + NEWLINE
                + "        AND FE.variableposition_id = " + forecastVariablePositionID + NEWLINE
                + "        AND UC.to_unit = " + targetUnitID + NEWLINE;
        String partB = "";
        if (innerWhere.length() > 0)
        {
            partB += "        AND " + innerWhere + NEWLINE;
        }
        partB += "    GROUP BY forecasted_date" + NEWLINE
                + ")" + NEWLINE
                + "SELECT O.observed_value * UC.factor AS observation, FM.forecasts" + NEWLINE
                + "FROM forecast_measurements FM" + NEWLINE
                + "INNER JOIN wres.Observation O" + NEWLINE
                + "    ON O.observation_time = FM.forecasted_date" + NEWLINE
                + "INNER JOIN wres.UnitConversion UC" + NEWLINE
                + "    ON UC.from_unit = O.measurementunit_id" + NEWLINE
                + "WHERE O.variableposition_id = " + observationVariablePositionID + NEWLINE
                + "    AND UC.to_unit = " + targetUnitID + NEWLINE
                + "ORDER BY FM.forecasted_date;";

        return partA + partB;
    }

    private static String getPairInnerWhereClauseFromConfig(final ProjectConfig config)
    {
        Objects.requireNonNull(config);

        final LocalDateTime earliest = getEarliestDateTimeFromDataSources(config);
        final LocalDateTime latest = getLatestDateTimeFromDataSources(config);

        if (earliest == null && latest == null)
        {
            return "";
        }

        if (earliest == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) <= '" + latest.format(SQL_FORMATTER) + "'";
        }
        else if (latest == null)
        {
            return "(F.forecast_date + INTERVAL '1 hour' * lead) >= '" + earliest.format(SQL_FORMATTER)
                + "'";
        }
        else
        {
            return "((F.forecast_date + INTERVAL '1 hour' * lead) >= '" + earliest.format(SQL_FORMATTER)
                + "'" + NEWLINE + "             AND (F.forecast_date + INTERVAL '1 hour' * lead) <= '"
                + latest.format(SQL_FORMATTER) + "')";
        }
    }

    /**
     * Returns the later of any "earliest" date specified in left or right datasource.
     * If only one date is specified, that one is returned.
     * If no dates for "earliest" are specified, null is returned.
     * @param config
     * @return the most narrow "earliest" date, null otherwise
     */
    private static LocalDateTime getEarliestDateTimeFromDataSources(final ProjectConfig config)
    {
        if (config.getConditions() == null)
        {
            return null;
        }

        String earliest = "";

        try
        {
            earliest = config.getConditions().getDates().getEarliest();
            return LocalDateTime.parse(earliest);
        }
        catch (final NullPointerException npe)
        {
            if (!messagedForEarliestDate.getAndSet(true))
            {
                LOGGER.info("No \"earliest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" /> under <conditions> (near line {} column {} of project file) to specify an earliest date.",
                        config.getConditions().sourceLocation().getLineNumber(),
                        config.getConditions().sourceLocation().getColumnNumber());
            }
            return null;
        }
        catch (final DateTimeParseException dtpe)
        {
            if (!messagedForEarliestDate.getAndSet(true))
            {
                LOGGER.warn(
                        "Correct the date \"{}\" near line {} column {} to ISO8601 format such as \"2017-06-27T16:16\"",
                        earliest,
                        config.getConditions().getDates().sourceLocation().getLineNumber(),
                        config.getConditions().getDates().sourceLocation().getColumnNumber());
            }
            return null;
        }
    }

    /**
     * Returns the earlier of any "latest" date specified in left or right datasource.
     * If only one date is specified, that one is returned.
     * If no dates for "latest" are specified, null is returned.
     * @param config
     * @return the most narrow "latest" date, null otherwise.
     */
    private static LocalDateTime getLatestDateTimeFromDataSources(final ProjectConfig config)
    {
        if (config.getConditions() == null)
        {
            return null;
        }

        String latest = "";

        try
        {
            latest = config.getConditions().getDates().getLatest();
            return LocalDateTime.parse(latest);
        }
        catch (final NullPointerException npe)
        {
            if (!messagedForLatestDate.getAndSet(true))
            {
                LOGGER.info("No \"latest\" date found in project. Use <dates earliest=\"2017-06-27T16:14\" latest=\"2017-07-06T11:35\" />  under <conditions> (near line {} col {} of project file) to specify a latest date.",
                        config.getConditions().sourceLocation().getLineNumber(),
                        config.getConditions().sourceLocation().getColumnNumber());
            }
            return null;
        }
        catch (final DateTimeParseException dtpe)
        {
            if (!messagedForLatestDate.getAndSet(true))
            {
                LOGGER.warn(
                        "Correct the date \"{}\" after line {} col {} to ISO8601 format such as \"2017-06-27T16:16\"",
                        latest,
                        config.getConditions().getDates().sourceLocation().getLineNumber(),
                        config.getConditions().getDates().sourceLocation().getColumnNumber());
            }
            return null;
        }
    }
}

