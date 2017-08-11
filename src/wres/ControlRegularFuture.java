package wres;

import com.sun.xml.bind.Locatable;
import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions.Feature;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.*;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.io.Operations;
import wres.io.concurrency.WRESCallable;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.utilities.InputGenerator;
import wres.util.ProgressMonitor;
import wres.vis.ChartEngineFactory;

import javax.xml.bind.ValidationEvent;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.function.Function;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class ControlRegularFuture implements Function<String[], Integer>
{

    public static void main(String[] args)
    {
        new ControlRegularFuture().apply(new String[]{"C:/Users/HSL/Desktop/WRES_TEST/wres_config_test.xml"});
    }

    /**
     * Processes one or more projects whose paths are provided in the input arguments.
     *
     * @param args the paths to one or more project configurations
     */
    public Integer apply(final String[] args)
    {
        // Validate the configurations

        List<ProjectConfigPlus> projectConfiggies = getProjects(args);
        boolean validated = validateProjects(projectConfiggies);
        if(!validated)
        {
            return -1;
        }

        ExecutorService processPairExecutor = null;
        try
        {
            ThreadFactory factory = runnable -> new Thread(runnable, "ControlRegularFuture Thread: ");
            processPairExecutor = Executors.newFixedThreadPool(MAX_THREADS, factory);

            // Iterate through the configurations
            for(ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                // Process the next configuration
                boolean processed = processProjectConfig(projectConfigPlus, processPairExecutor);
                if(!processed)
                {
                    return -1;
                }
            }
            return 0;
        }
        finally
        {
            shutDownGracefully(processPairExecutor);
        }
    }

    /**
     * Quick validation of the project configuration, will emit detailed information to the user regarding issues about
     * the configuration. Strict for now, i.e. return false even on minor xml problems. Does not return on first issue,
     * tries to inform the user of all issues before returning.
     *
     * @param projectConfigPlus
     * @return true if no issues were detected, false otherwise
     */

    public static boolean isProjectValid(ProjectConfigPlus projectConfigPlus)
    {
        // Assume valid until demonstrated otherwise
        boolean result = true;

        for(ValidationEvent ve: projectConfigPlus.getValidationEvents())
        {
            if(LOGGER.isWarnEnabled())
            {
                if(ve.getLocator() != null)
                {
                    LOGGER.warn("In file {}, near line {} and column {}, WRES found an issue with the project "
                        + "configuration. The parser said:",
                                projectConfigPlus.getPath(),
                                ve.getLocator().getLineNumber(),
                                ve.getLocator().getColumnNumber(),
                                ve.getLinkedException());
                }
                else
                {
                    LOGGER.warn("In file {}, WRES found an issue with the project configuration. The parser said:",
                                projectConfigPlus.getPath(),
                                ve.getLinkedException());
                }
            }
            // Any validation event means we fail.
            result = false;
        }

        // Validate graphics portion
        result = result && isGraphicsPortionOfProjectValid(projectConfigPlus);

        return result;
    }

    /**
     * Validates graphics portion, similar to isProjectValid, but targeted.
     * 
     * @param projectConfigPlus
     * @return
     */

    public static boolean isGraphicsPortionOfProjectValid(ProjectConfigPlus projectConfigPlus)
    {
        final String BEGIN_TAG = "<chartDrawingParameters>";
        final String END_TAG = "</chartDrawingParameters>";
        final String BEGIN_COMMENT = "<!--";
        final String END_COMMENT = "-->";

        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        for(DestinationConfig d: projectConfig.getOutputs().getDestination())
        {
            String customString = projectConfigPlus.getGraphicsStrings().get(d);
            if(customString != null)
            {
                // For to give a helpful message, find closeby tag without NPE
                Locatable nearbyTag;
                if(d.getGraphical() != null && d.getGraphical().getConfig() != null)
                {
                    // best case
                    nearbyTag = d.getGraphical().getConfig();
                }
                else if(d.getGraphical() != null)
                {
                    // not as targeted but close
                    nearbyTag = d.getGraphical();
                }
                else
                {
                    // destination tag.
                    nearbyTag = d;
                }

                // If a custom vis config was provided, make sure string either
                // starts with the correct tag or starts with a comment.
                String trimmedCustomString = customString.trim();
                result = result
                    && checkTrimmedString(trimmedCustomString, BEGIN_TAG, BEGIN_COMMENT, nearbyTag, projectConfigPlus);
                result = result
                    && checkTrimmedString(trimmedCustomString, END_TAG, END_COMMENT, nearbyTag, projectConfigPlus);
            }
        }
        return result;
    }

    /**
     * Validates a list of {@link ProjectConfigPlus}. Returns true if the projects validate successfully, false
     * otherwise.
     * 
     * @return true if the projects validate successfully, false otherwise
     */

    private boolean validateProjects(List<ProjectConfigPlus> projectConfiggies)
    {
        if(projectConfiggies.isEmpty())
        {
            LOGGER.error("Please correct project configuration files (see above) and pass them in the command line "
                + "like this: wres executeConfigProject c:/path/to/config1.xml c:/path/to/config2.xml");
        }
        else
        {
            LOGGER.info("Successfully unmarshalled {} project configuration(s), validating further...",
                        projectConfiggies.size());
        }

        boolean validationsPassed = true;

        // Validate all projects, not stopping until all are done
        for(ProjectConfigPlus projectConfigPlus: projectConfiggies)
        {
            if(!isProjectValid(projectConfigPlus))
            {
                validationsPassed = false;
            }
        }

        if(validationsPassed)
        {
            LOGGER.info("Successfully read and validated {} project configuration(s). Beginning execution...",
                        projectConfiggies.size());
        }
        return validationsPassed;
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService}
     * 
     * @param projectConfigPlus the project configuration
     * @param processPairExecutor the {@link ExecutorService}
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processProjectConfig(ProjectConfigPlus projectConfigPlus, ExecutorService processPairExecutor)
    {

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Need to ingest first.
        boolean ingestResult = Operations.ingest(projectConfig);

        if(!ingestResult)
        {
            LOGGER.error("Error while attempting to ingest data.");
            return false;
        }

        // Obtain the features
        List<Feature> features = projectConfig.getConditions().getFeature();

        // Iterate through the features
        for(Feature nextFeature: features)
        {
            if(LOGGER.isInfoEnabled())
            {
                LOGGER.info("Processing feature '{}'.", nextFeature.getLocation().getLid());
            }

            // Sink for the results: the results are added incrementally to an immutable store via a builder
            // Some output types are processed at the end of the pipeline, others after each input is processed
            // Construct a processor that retains all output types required at the end of the pipeline: SCALAR and VECTOR
            MetricProcessor processor = null;
            try
            {
                processor = MetricFactory.getInstance(DATA_FACTORY).getMetricProcessor(projectConfig,
                                                                                       MetricOutputGroup.SCALAR,
                                                                                       MetricOutputGroup.VECTOR);
            }
            catch(MetricConfigurationException e)
            {
                LOGGER.error("While processing the metric configuration:", e);
                return false;
            }

            // Build an InputGenerator for the next feature
            InputGenerator metricInputs = Operations.getInputs(projectConfig, nextFeature);

            // Queue up processing of fetched pairs.
            for(Future<MetricInput<?>> nextInput: metricInputs)
            {
                PairsByLeadProcessor processTask = new PairsByLeadProcessor(processor, nextInput);
                processTask.setOnRun(ProgressMonitor.onThreadStartHandler());
                processTask.setOnComplete(ProgressMonitor.onThreadCompleteHandler());

                // TODO: Add consumer to pipeline here for intermediate results
                processPairExecutor.submit(processTask);
                //Future<MetricOutputForProjectByLeadThreshold> intermediate = processPairExecutor.submit(processTask);
                //processMultiVectorCharts(projectConfigPlus, processor.getStoredMetricOutput().getMultiVectorOutput());
            }

            //  Complete the end-of-pipeline processing
            if(processor.hasStoredMetricOutput())
            {
                processCachedCharts(projectConfigPlus, processor, MetricOutputGroup.SCALAR);
                //  TODO: support processing of vector output in wres-vis and here
                //processCharts(projectConfigPlus, processor, MetricOutputGroup.VECTOR);

                if(LOGGER.isInfoEnabled() && processPairExecutor instanceof ThreadPoolExecutor)
                {
                    LOGGER.info("Completed processing of feature '{}'.", nextFeature.getLocation().getLid());
                }
            }
        }
        return true;
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param projectConfigPlus the project configuration
     * @param processor the {@link MetricProcessor} that contains the results for all chart types
     * @param outGroup the {@link MetricOutputGroup} for which charts are required
     * @return true it the processing completed successfully, false otherwise
     */

    private boolean processCachedCharts(ProjectConfigPlus projectConfigPlus,
                                        MetricProcessor processor,
                                        MetricOutputGroup outGroup)
    {
        if(!processor.hasStoredMetricOutput() || !processor.getStoredMetricOutput().hasOutput(outGroup))
        {
            LOGGER.error("Error while building charts for the metrics. Metric outputs are not available for output type {}.",
                         outGroup);
            return false;
        }

        try
        {
            switch(outGroup)
            {
                case SCALAR:
                    return processScalarCharts(projectConfigPlus, processor.getStoredMetricOutput().getScalarOutput());
                case VECTOR:
                case MULTIVECTOR:
                case MATRIX:
                default:
                    LOGGER.error("Unsupported chart type {}.", outGroup);
                    return false;
            }
        }
        catch(InterruptedException e)
        {
            LOGGER.error("Interrupted while preparing acquiring metric results.", e);
            Thread.currentThread().interrupt();
            return false;
        }
        catch(ExecutionException e)
        {
            LOGGER.error("While acquiring metric results:", e);
            return false;
        }
    }

    /**
     * Processes a set of charts associated with {@link ScalarOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param projectConfigPlus the project configuration
     * @param scalarResults the scalar outputs
     * @return true it the processing completed succesfully, false otherwise
     */

    private boolean processScalarCharts(ProjectConfigPlus projectConfigPlus,
                                        MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarResults)
    {

        // Build charts
        try
        {
            for(Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<ScalarOutput>> e: scalarResults.entrySet())
            {
                DestinationConfig dest = projectConfigPlus.getProjectConfig().getOutputs().getDestination().get(1);
                String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);

                ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(e.getValue(),
                                                                                            DATA_FACTORY.getMetadataFactory(),
                                                                                            ChartEngineFactory.VisualizationPlotType.LEAD_THRESHOLD,
                                                                                            "scalarOutputTemplate.xml",
                                                                                            graphicsString);

                Path outputImage = Paths.get(dest.getPath() + e.getKey().getFirstKey() + "_output.png");

                if(LOGGER.isWarnEnabled() && outputImage.toFile().exists())
                {
                    LOGGER.warn("File {} already existed and is being overwritten.", outputImage);
                }

                File outputImageFile = outputImage.toFile();

                int width = SystemSettings.getDefaultChartWidth();
                int height = SystemSettings.getDefaultChartHeight();

                if(dest.getGraphical() != null && dest.getGraphical().getWidth() != null)
                {
                    width = dest.getGraphical().getWidth();
                }
                if(dest.getGraphical() != null && dest.getGraphical().getHeight() != null)
                {
                    height = dest.getGraphical().getHeight();
                }

                ChartTools.generateOutputImageFile(outputImageFile, engine.buildChart(), width, height);

                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info(scalarResults.toString());
                }
            }
        }
        catch(ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
        {
            LOGGER.error("While generating plots:", e);
            return false;
        }
        return true;
    }

    /**
     * Get project configurations from command line file args. If there are no command line args, look in System
     * Settings for directory to scan for configurations.
     *
     * @param args
     * @return the successfully found, read, unmarshalled project configs
     */
    private List<ProjectConfigPlus> getProjects(String[] args)
    {
        List<Path> existingProjectFiles = new ArrayList<>();

        if(args.length > 0)
        {
            for(String arg: args)
            {
                Path path = Paths.get(arg);
                if(path.toFile().exists())
                {
                    existingProjectFiles.add(path);

                    // TODO: Needs to be temporary; used to log execution information
                    if (path.toFile().isFile())
                    {
                        MainFunctions.setProjectPath(path.toAbsolutePath().toString());
                    }
                }
                else
                {
                    LOGGER.warn("Project configuration file {} does not exist!", path);
                }
            }
        }

        List<ProjectConfigPlus> projectConfiggies = new ArrayList<>();

        for(Path path: existingProjectFiles)
        {
            try
            {
                ProjectConfigPlus projectConfigPlus = ProjectConfigPlus.from(path);
                projectConfiggies.add(projectConfigPlus);
            }
            catch(IOException ioe)
            {
                LOGGER.error("Could not read project configuration: ", ioe);
            }
        }
        return projectConfiggies;
    }

    /**
     * Task whose job is to wait for pairs to arrive, then run metrics on them.
     */
    private static class PairsByLeadProcessor extends WRESCallable<MetricOutputForProjectByLeadThreshold>
    {
        private final MetricProcessor processor;
        private final Future<MetricInput<?>> input;

        private PairsByLeadProcessor(final MetricProcessor processor, final Future<MetricInput<?>> input)
        {
            this.processor = processor;
            this.input = input;
        }

        @Override
        protected MetricOutputForProjectByLeadThreshold execute() throws Exception
        {
            try
            {
                MetricInput<?> nextInput = input.get();
                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Completed processing of pairs for feature '{}' at lead time {}.",
                                nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                nextInput.getMetadata().getLeadTime());
                }
                return processor.apply(nextInput);
            }
            catch(Exception e)
            {
                LOGGER.error("While processing pairs:",e);
                throw e;
            }
        }

        @Override
        protected String getTaskName()
        {
            return "Process Metrics for Pairs by Lead Time";
        }

        @Override
        protected Logger getLogger()
        {
            return LOGGER;
        }
    }

    /**
     * Checks a trimmed string in the graphics configuration.
     * 
     * @param trimmedCustomString the trimmed string
     * @param tag the tag
     * @param comment the comment
     * @param nearbyTag a nearby tag
     * @param projectConfigPlus the configuration
     * @return true if the tag is valid, false otherwise
     */

    private static boolean checkTrimmedString(String trimmedCustomString,
                                              String tag,
                                              String comment,
                                              Locatable nearbyTag,
                                              ProjectConfigPlus projectConfigPlus)
    {
        if(!trimmedCustomString.endsWith(tag) && !trimmedCustomString.endsWith(comment))
        {
            String msg = "In file {}, near line {} and column {}, " + "WRES found an issue with the project "
                + " configuration in the area of custom " + "graphics configuration. If customization is "
                + "provided, please end it with " + tag;

            LOGGER.warn(msg,
                        projectConfigPlus.getPath(),
                        nearbyTag.sourceLocation().getLineNumber(),
                        nearbyTag.sourceLocation().getColumnNumber());

            return false;
        }
        return true;
    }

    /**
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param processPairExecutor
     */
    private static void shutDownGracefully(ExecutorService processPairExecutor)
    {
        if(Objects.isNull(processPairExecutor))
        {
            return;
        }
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
                Thread.sleep(500);
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
            LOGGER.info("Abandoned {} processing tasks.", processingSkipped);
        }
    }

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlRegularFuture.class);

    /**
     * Log interval.
     */

    public static final long LOG_PROGRESS_INTERVAL_MILLIS = 2000;

    /**
     * Default data factory.
     */

    private static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();

    /**
     * System property used to retrieve max thread count, passed as -D
     */

    public static final String MAX_THREADS_PROP_NAME = "wres.maxThreads";

    /**
     * Maximum threads.
     */

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
            maxThreads = SystemSettings.maximumThreadCount();
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

}
