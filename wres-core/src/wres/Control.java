package wres;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.metric.*;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.io.Operations;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.vis.ChartEngineFactory;

/**
 * Another way to execute a project.
 */
public class Control implements Function<String[], Integer>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);
    public static final long LOG_PROGRESS_INTERVAL_MILLIS = 2000;
    private static final AtomicLong lastMessageTime = new AtomicLong();

    /** System property used to retrieve max thread count, passed as -D */
    public static final String MAX_THREADS_PROP_NAME = "wres.maxThreads";

    public static final int MAX_THREADS;
    // Figure out the max threads from property or by default rule.
    // Ideally priority order would be: -D, SystemSettings, default rule.
    static
    {
        String maxThreadsStr = System.getProperty(MAX_THREADS_PROP_NAME);
        int maxThreads;
        try
        {
            maxThreads = Integer.parseInt(maxThreadsStr);
        }
        catch(final NumberFormatException nfe)
        {
            maxThreads =  SystemSettings.maximumThreadCount();
        }

        if (maxThreads >= 1)
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
            LOGGER.error("Validate project configuration files and place them in the command line like this: wres executeConfigProject c:/path/to/config1.xml c:/path/to/config2.xml");
            return null;
        }
        else
        {
            LOGGER.info("Found {} valid projects, beginning execution", projectConfiggies.size());
        }

        // Create a queue of work: displaying or processing fetched pairs.
        int maxProcessThreads = Control.MAX_THREADS;
        ExecutorService processPairExecutor = Executors.newFixedThreadPool(maxProcessThreads);

        DataFactory dataFac = DefaultDataFactory.getInstance();

        //Sink for the results: the results are added incrementally to an immutable store via a builder
        MetricOutputMultiMap.Builder<ScalarOutput> resultsBuilder = dataFac.ofMultiMap();

        for (ProjectConfigPlus projectConfigPlus : projectConfiggies)
        {
            ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

            // Need to ingest first.
            boolean ingestResult = Operations.ingest(projectConfig);

            if (!ingestResult)
            {
                LOGGER.warn("Ingest did not complete smoothly. Aborting.");
                return null;
            }

            Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> pairs;
            // Ask the IO module for pairs
            try
            {
                pairs = Operations.getPairs(projectConfig);
            }
            catch (ExecutionException | SQLException e)
            {
                LOGGER.error("While getting results", e);
                return null;
            }
            catch (InterruptedException ie)
            {
                LOGGER.error("Interrupted while getting results", ie);
                Thread.currentThread().interrupt();
                return null;
            }

            List<Future<MetricOutputMapByMetric<ScalarOutput>>> futureMetrics = new ArrayList<>();

            // Queue up processing of fetched pairs.
            for (Map.Entry<Integer, List<PairOfDoubleAndVectorOfDoubles>> pair : pairs.entrySet())
            {
                // Here, using index in list to communicate the lead time.
                // Another structure might be appropriate, for example,
                // see getPairs in
                // wres.io.config.specification.MetricSpecification
                // which uses a Map.
                int leadTime = pair.getKey();
                PairsByLeadProcessor processTask =
                        new PairsByLeadProcessor(pair.getValue(),
                                                 projectConfig,
                                                 leadTime);

                Future<MetricOutputMapByMetric<ScalarOutput>> futureMetric =
                        processPairExecutor.submit(processTask);
                futureMetrics.add(futureMetric);
            }

            // use resultsBuilder instead of finalResults
            // Map<Integer, MetricOutputMapByMetric<ScalarOutput>> finalResults = new HashMap<>();

            // Retrieve metric results from processing queue.
            try
            {
                // counting on communication of data with index for this example
                for (int i = 0; i < futureMetrics.size(); i++)
                {
                    // get each result
                    MetricOutputMapByMetric<ScalarOutput> metrics = futureMetrics.get(i).get();

                    int leadTime = i + 1;
                    Threshold fakeThreshold = dataFac.getThreshold(Double.NEGATIVE_INFINITY, Threshold.Condition.GREATER);
                    resultsBuilder.add(leadTime, fakeThreshold, metrics);

                    if (LOGGER.isInfoEnabled() && processPairExecutor instanceof ThreadPoolExecutor)
                    {
                        long curTime = System.currentTimeMillis();
                        long lastTime = lastMessageTime.get();
                        if (curTime - lastTime > LOG_PROGRESS_INTERVAL_MILLIS && lastMessageTime
                                .compareAndSet(lastTime, curTime))
                        {
                            ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor) processPairExecutor;
                            LOGGER.info(
                                    "Around {} fetched pairs processed. Around {} in processing queue.",
                                    tpeProcess.getCompletedTaskCount(),
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
                processPairExecutor.shutdown();
            }

            if (LOGGER.isInfoEnabled() && processPairExecutor instanceof ThreadPoolExecutor)
            {
                ThreadPoolExecutor tpeProcess = (ThreadPoolExecutor) processPairExecutor;
                LOGGER.info("Total of around {} pairs processed. Done.",
                            tpeProcess.getCompletedTaskCount());
            }

            //Build final results:
            MetricOutputMultiMap<ScalarOutput> results = resultsBuilder.build();

            // Make charts!
            try
            {
                for (Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<ScalarOutput>> e : results
                        .entrySet())
                {
                    DestinationConfig dest = projectConfig.getOutputs().getDestination().get(1);
                    String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);

                    ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(
                            e.getValue(),
                            dataFac.getMetadataFactory(),
                            ChartEngineFactory.VisualizationPlotType.LEAD_THRESHOLD,
                            "scalarOutputTemplate.xml",
                            graphicsString);

                    Path outputImage = Paths.get(dest.getPath() + e.getKey().getFirstKey() + "_output.png");

                    if (LOGGER.isWarnEnabled() && Files.exists(outputImage))
                    {
                        LOGGER.warn("File {} already existed and is being overwritten.",
                                    outputImage);
                    }

                    File outputImageFile = outputImage.toFile();

                    int width = SystemSettings.getDefaultChartWidth();
                    int height = SystemSettings.getDefaultChartHeight();

                    if (dest.getGraphical() != null && dest.getGraphical().getWidth() != null)
                    {
                        width = dest.getGraphical().getWidth();
                    }
                    if (dest.getGraphical() != null && dest.getGraphical().getHeight() != null)
                    {
                        height = dest.getGraphical().getHeight();
                    }

                    ChartTools.generateOutputImageFile(outputImageFile,
                                                       engine.buildChart(),
                                                       width,
                                                       height);
                }
            }
            catch (ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
            {
                LOGGER.error("Could not generate plots:", e);
                return null;
            }

            if (LOGGER.isInfoEnabled())
            {
                results.forEach((key, value) -> {
                    LOGGER.info(NEWLINE + "Results for metric "
                            + dataFac.getMetadataFactory().getMetricName(key.getFirstKey()) + " (lead time, threshold, score) "
                            + NEWLINE + value);
                });
            }
        }

        shutDownGracefully(processPairExecutor);

        return 0;
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param processPairExecutor
     */
    private static void shutDownGracefully(ExecutorService processPairExecutor)
    {
        // (There are probably better ways to do this, e.g. awaitTermination)
        int processingSkipped = 0;
        int i = 0;
        boolean deathTime = false;
        while(!processPairExecutor.isTerminated())
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
                    processingSkipped += processPairExecutor.shutdownNow().size();
                }
            }
            i++;
        }

        if(processingSkipped > 0)
        {
            LOGGER.info("Abandoned {} pair fetch tasks, abandoned {} processing tasks.",
                        processingSkipped);
        }
    }

    /**
     * Get project configurations from command line file args.
     *
     * If there are no command line args, look in System Settings for directory
     * to scan for configurations.
     *
     * @param args
     * @return the successfully found, read, unmarshalled project configs
     */
    private List<ProjectConfigPlus> getProjects(String[] args)
    {
        List<Path> existingProjectFiles = new ArrayList<>();

        if (args.length > 0)
        {
            for (String arg : args)
            {
                Path path = Paths.get(arg);
                if (Files.exists(path))
                {
                    existingProjectFiles.add(path);
                }
                else
                {
                    LOGGER.warn("Project configuration file {} does not exist!",
                            path);
                }
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
        private final List<PairOfDoubleAndVectorOfDoubles> pairs;
        private final ProjectConfig projectConfig;
        private final int leadTime;

        private PairsByLeadProcessor(final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                     final ProjectConfig projectConfig,
                                     final int leadTime)
        {
            this.pairs = pairs;
            this.projectConfig = projectConfig;
            this.leadTime = leadTime;
        }

        @Override
        public MetricOutputMapByMetric<ScalarOutput> call()
                throws ProcessingException, ChartEngineException,
                GenericXMLReadingHandlerException, XYChartDataSourceException,
                IOException, URISyntaxException
        {
            // Grow a List of PairOfDoubleAndVectorOfDoubles into a simpler
            // List of PairOfDouble for metric calculation.
            List<PairOfDoubles> simplePairs = Slicer.getFlatDoublePairs(pairs);

            // What follows for the rest of call() was originally from MetricCollectionTest.

            // Convert pairs into metric input
            DataFactory dataFactory = DefaultDataFactory.getInstance();
            MetadataFactory metFac = dataFactory.getMetadataFactory();
            Metadata meta = metFac.getMetadata(simplePairs.size(),
                    metFac.getDimension(projectConfig.getPair().getUnit()),
                    metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));
            SingleValuedPairs input = dataFactory.ofSingleValuedPairs(simplePairs,
                                                                       meta);

            // Create an immutable collection of metrics that consume single-valued pairs
            // and produce a scalar output
            //Build an immutable collection of metrics, to be computed at each of several forecast lead times
            MetricFactory metricFactory = MetricFactory.getInstance(dataFactory);
            MetricCollection<SingleValuedPairs, ScalarOutput> collection =
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
}

