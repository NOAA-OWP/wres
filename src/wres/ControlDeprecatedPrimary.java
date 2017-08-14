package wres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import javax.xml.bind.ValidationEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.bind.Locatable;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.ChartTools;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import ohd.hseb.hefs.utils.xml.GenericXMLReadingHandlerException;
import wres.config.generated.Conditions.Feature;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.VectorOutput;
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

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
 * 
 * @author jesse
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 * @deprecated
 */
@Deprecated
public class ControlDeprecatedPrimary implements Function<String[], Integer>
{

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
        // Process the configurations in parallel
        ExecutorService processPairExecutor = null;
        try
        {
            // Build a processing pipeline.           
            // Currently, this uses a fixed thread pool with an unbounded queue. Since the WRES is typically I/O bound, 
            // I/O threads will block and cause an OutOfMemoryException at some point, unless all data fits into RAM, 
            // which is highly unlikely for many practical problems, given the design of the Data Storage Model. 
            // This will probably require a bounded queue/executor (see JCIP by Goetz: http://jcip.net/). However, a 
            // bounded queue is difficult to calibrate effectively. Instead, consider the approach demonstrated in 
            // ControlNonBlocking.java, which uses a ForkJoinPool together with CompletableFuture. This allows for 
            // work-stealing from I/O tasks that are waiting, and allows for consumers to continue operating as pairs 
            // become available, thereby avoiding deadlock and OutOfMemoryException with large datasets
            ThreadFactory factory = runnable -> new Thread(runnable, "Control Thread: ");
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
        // Shutdown
        finally
        {
            shutDownGracefully(processPairExecutor);
        }
    }

    /**
     * Quick validation of the project configuration, will return detailed information to the user regarding issues
     * about the configuration. Strict for now, i.e. return false even on minor xml problems. Does not return on first
     * issue, tries to inform the user of all issues before returning.
     *
     * @param projectConfigPlus the project configuration
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
     * @param projectConfigPlus the project configuration
     * @return true if the graphics configuration is valid, false otherwise
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
                    // Best case
                    nearbyTag = d.getGraphical().getConfig();
                }
                else if(d.getGraphical() != null)
                {
                    // Not as targeted but close
                    nearbyTag = d.getGraphical();
                }
                else
                {
                    // Destination tag.
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
     * @param projectConfiggies a list of project configurations to validate
     * @return true if the projects validate successfully, false otherwise
     */

    private static boolean validateProjects(List<ProjectConfigPlus> projectConfiggies)
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
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService}.
     * 
     * @param projectConfigPlus the project configuration
     * @param processPairExecutor the {@link ExecutorService}
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processProjectConfig(ProjectConfigPlus projectConfigPlus, ExecutorService processPairExecutor)
    {

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        // Need to ingest first
        boolean ingestResult = Operations.ingest(projectConfig);

        if(!ingestResult)
        {
            LOGGER.error("Error while attempting to ingest data.");
            return false;
        }

        // Obtain the features
        List<Feature> features = projectConfig.getConditions().getFeature();

        // Iterate through the features and process them
        for(Feature nextFeature: features)
        {
            if(!processFeature(nextFeature, projectConfigPlus, processPairExecutor))
            {
                return false;
            }
        }
        return true;
    }

    /**
     * Processes a {@link ProjectConfigPlus} for a specific {@link Feature} using a prescribed {@link ExecutorService}.
     * 
     * @param feature the feature to process
     * @param projectConfigPlus the project configuration
     * @param processPairExecutor the {@link ExecutorService}
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processFeature(Feature feature,
                                   ProjectConfigPlus projectConfigPlus,
                                   ExecutorService processPairExecutor)
    {

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        if(LOGGER.isInfoEnabled())
        {
            LOGGER.info("Processing feature '{}'.", feature.getLocation().getLid());
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
        InputGenerator metricInputs = Operations.getInputs(projectConfig, feature);

        // Queue up processing of fetched pairs.            
        List<PairsByLeadProcessor> tasks = new ArrayList<>();
        for(Future<MetricInput<?>> nextInput: metricInputs)
        {
            PairsByLeadProcessor processTask = new PairsByLeadProcessor(feature,
                                                                        projectConfigPlus,
                                                                        processor,
                                                                        nextInput);
            processTask.setOnRun(ProgressMonitor.onThreadStartHandler());
            processTask.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            tasks.add(processTask);
        }
        //Invoke all tasks and process within-pipeline products
        try
        {
            processPairExecutor.invokeAll(tasks);
        }
        catch(InterruptedException e)
        {
            LOGGER.error("Interrupted while processing metrics for feature {}.", feature.getLocation().getLid(), e);
            Thread.currentThread().interrupt();
            return false;
        }

        //  Complete the end-of-pipeline processing
        if(processor.willCacheMetricOutput())
        {
            processCachedCharts(feature,
                                projectConfigPlus,
                                processor,
                                MetricOutputGroup.SCALAR,
                                MetricOutputGroup.VECTOR);
            if(LOGGER.isInfoEnabled() && processPairExecutor instanceof ThreadPoolExecutor)
            {
                LOGGER.info("Completed processing of feature '{}'.", feature.getLocation().getLid());
            }
        }
        return true;
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param processor the {@link MetricProcessor} that contains the results for all chart types
     * @param outGroup the {@link MetricOutputGroup} for which charts are required
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processCachedCharts(Feature feature,
                                        ProjectConfigPlus projectConfigPlus,
                                        MetricProcessor processor,
                                        MetricOutputGroup... outGroup)
    {
        //True until failed
        boolean returnMe = true;
        try
        {
            for(MetricOutputGroup nextGroup: outGroup)
            {
                switch(nextGroup)
                {
                    case SCALAR:
                        returnMe = returnMe && processScalarCharts(feature,
                                                                   projectConfigPlus,
                                                                   processor.getStoredMetricOutput().getScalarOutput());
                        break;
                    case VECTOR:
                        returnMe = returnMe && processVectorCharts(feature,
                                                                   projectConfigPlus,
                                                                   processor.getStoredMetricOutput().getVectorOutput());
                        break;
                    case MULTIVECTOR:
                        returnMe = returnMe
                            && processMultiVectorCharts(feature,
                                                        projectConfigPlus,
                                                        processor.getStoredMetricOutput().getMultiVectorOutput());
                        break;
                    default:
                        LOGGER.error("Unsupported chart type {}.", nextGroup);
                        returnMe = false;
                }
            }
        }
        catch(InterruptedException e)
        {
            LOGGER.error("Interrupted while processing charts.", e);
            Thread.currentThread().interrupt();
            returnMe = false;
        }
        catch(ExecutionException e)
        {
            LOGGER.error("While processing charts:", e);
            returnMe = false;
        }
        return returnMe;
    }

    /**
     * Processes a set of charts associated with {@link ScalarOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param scalarResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processScalarCharts(Feature feature,
                                               ProjectConfigPlus projectConfigPlus,
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
                //Build the output file name
                StringBuilder pathBuilder = new StringBuilder();
                pathBuilder.append(dest.getPath())
                           .append(feature.getLocation().getLid())
                           .append("_")
                           .append(e.getKey().getFirstKey())
                           .append("_")
                           .append(e.getKey().getSecondKey())
                           .append("_")
                           .append(projectConfigPlus.getProjectConfig().getInputs().getRight().getVariable().getValue())
                           .append(".png");
                Path outputImage = Paths.get(pathBuilder.toString());
                writeChart(outputImage, engine, dest);
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
     * Processes a set of charts associated with {@link VectorOutput} across multiple metrics, forecast lead times, and
     * thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}. TODO: implement when wres-vis can handle
     * these.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param vectorResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processVectorCharts(Feature feature,
                                               ProjectConfigPlus projectConfigPlus,
                                               MetricOutputMultiMapByLeadThreshold<VectorOutput> vectorResults)
    {
//        // Build charts
//        try
//        {
//            for(Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<VectorOutput>> e: vectorResults.entrySet())
//            {
//                DestinationConfig dest = projectConfigPlus.getProjectConfig().getOutputs().getDestination().get(1);
//                String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
//
//                ChartEngine engine = null;
//
//                //Build the output file name
//                StringBuilder pathBuilder = new StringBuilder();
//                pathBuilder.append(dest.getPath())
//                           .append(feature.getLocation().getLid())
//                           .append("_")
//                           .append(e.getKey().getFirstKey())
//                           .append("_")
//                           .append(e.getKey().getSecondKey())
//                           .append("_")
//                           .append(projectConfigPlus.getProjectConfig().getInputs().getRight().getVariable().getValue())
//                           .append(".png");
//                Path outputImage = Paths.get(pathBuilder.toString());
//                writeChart(outputImage, engine, dest);
//            }
//        }
//        catch(ChartEngineException | GenericXMLReadingHandlerException | XYChartDataSourceException | IOException e)
//        {
//            LOGGER.error("While generating plots:", e);
//            return false;
//        }
        return true;
    }

    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, forecast lead times,
     * and thresholds, stored in a {@link MetricOutputMultiMapByLeadThreshold}. 
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the scalar outputs
     * @return true it the processing completed successfully, false otherwise
     */

    private static boolean processMultiVectorCharts(Feature feature,
                                                    ProjectConfigPlus projectConfigPlus,
                                                    MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> multiVectorResults)
    {
        // Build charts
        try
        {
            // Build the charts for each metric
            for(Map.Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<MultiVectorOutput>> e: multiVectorResults.entrySet())
            {
                DestinationConfig dest = projectConfigPlus.getProjectConfig().getOutputs().getDestination().get(1);
                String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                // TODO: make this template call to "reliabilityDiagramTemplate" generic across metrics. 
                // Is it even needed? The Metadata contains the chart type and this is linked to the PlotType?
                Map<Integer, ChartEngine> engines =
                                                  ChartEngineFactory.buildMultiVectorOutputChartEngineByLead(e.getValue(),
                                                                                                             DATA_FACTORY.getMetadataFactory(),
                                                                                                             null,
                                                                                                             "reliabilityDiagramTemplate.xml",
                                                                                                             graphicsString);
                // Build one chart per lead time
                for(Map.Entry<Integer, ChartEngine> nextEntry: engines.entrySet())
                {
                    //Build the output file name
                    StringBuilder pathBuilder = new StringBuilder();
                    pathBuilder.append(dest.getPath())
                               .append(feature.getLocation().getLid())
                               .append("_")
                               .append(e.getKey().getFirstKey())
                               .append("_")
                               .append(e.getKey().getSecondKey())
                               .append("_")
                               .append(projectConfigPlus.getProjectConfig()
                                                        .getInputs()
                                                        .getRight()
                                                        .getVariable()
                                                        .getValue())
                               .append("_")
                               .append(nextEntry.getKey())
                               .append("h")
                               .append(".png");
                    Path outputImage = Paths.get(pathBuilder.toString());
                    writeChart(outputImage, nextEntry.getValue(), dest);
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
     * Writes an output chart to a specified path.
     * 
     * @param outputImage the path to the output image
     * @param engine the chart engine
     * @param dest the destination configuration
     * @throws XYChartDataSourceException
     * @throws ChartEngineException
     * @throws IOException
     */

    private static void writeChart(Path outputImage,
                                   ChartEngine engine,
                                   DestinationConfig dest) throws IOException,
                                                           ChartEngineException,
                                                           XYChartDataSourceException
    {
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
    }

    /**
     * Get project configurations from command line file args. If there are no command line args, look in System
     * Settings for directory to scan for configurations.
     *
     * @param args the paths to the projects
     * @return the successfully found, read, unmarshalled project configs
     */
    private static List<ProjectConfigPlus> getProjects(String[] args)
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
                    if(path.toFile().isFile())
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
     * Task that waits for pairs to arrive, computes metrics from them, and then produces any intermediate
     * (within-pipeline) products.
     */
    private static class PairsByLeadProcessor extends WRESCallable<MetricOutputForProjectByLeadThreshold>
    {
        /**
         * The metric processor.
         */
        private final MetricProcessor processor;

        /**
         * The future metric input.
         */
        private final Future<MetricInput<?>> input;

        /**
         * The project configuration.
         */

        private final ProjectConfigPlus projectConfigPlus;

        /**
         * The feature.
         */

        private Feature feature;

        /**
         * Construct.
         * 
         * @param projectConfigPlus the project configuration
         * @param processor the metric processor
         * @param input the future metric input
         */

        private PairsByLeadProcessor(Feature feature,
                                     ProjectConfigPlus projectConfigPlus,
                                     MetricProcessor processor,
                                     Future<MetricInput<?>> input)
        {
            this.feature = feature;
            this.projectConfigPlus = projectConfigPlus;
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
                MetricOutputForProjectByLeadThreshold results = processor.apply(nextInput);
                //Process the within-pipeline products
                processMultiVectorCharts(feature, projectConfigPlus, results.getMultiVectorOutput());
                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Completed processing of intermediate metrics results for feature '{}' at lead time {}.",
                                nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                nextInput.getMetadata().getLeadTime());
                }
                return results;
            }
            catch(InterruptedException e)
            {
                LOGGER.error("Interrupted while processing pairs.", e);
                Thread.currentThread().interrupt();
                throw e;
            }
            catch(Exception e)
            {
                LOGGER.error("While processing pairs:", e);
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
     * @param processPairExecutor the executor to shutdown
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

    private static final Logger LOGGER = LoggerFactory.getLogger(ControlDeprecatedPrimary.class);

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
