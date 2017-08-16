package wres;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MapBiKey;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MetricOutputMultiMapByLeadThreshold;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.Threshold;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.io.Operations;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.utilities.InputGenerator;
import wres.vis.ChartEngineFactory;
import wres.vis.ChartEngineFactory.VisualizationPlotType;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}. The processing
 * pipeline is constructed with a {@link ForkJoinPool} and uses {@link CompletableFuture} to chain together tasks
 * asynchronously. This allows for work-stealing from I/O tasks that are waiting, and allows for consumers to continue
 * operating as pairs become available, thereby avoiding deadlock and OutOfMemoryException with large datasets.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */
public class Control implements Function<String[], Integer>
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
        // Process the configurations with a ForkJoinPool
        ExecutorService processPairExecutor = null;
        ExecutorService metricExecutor = null;
        try
        {
            // Build a processing pipeline
            processPairExecutor = new ForkJoinPool();
            
            // If null, uses ForkJoinPool.commonPool()
            //metricExecutor = null;

            // Iterate through the configurations
            for(ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                // Process the next configuration
                boolean processed = processProjectConfig(projectConfigPlus, processPairExecutor, metricExecutor);
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
            shutDownGracefully(metricExecutor);
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
        boolean validationsPassed = true;

        if(projectConfiggies.isEmpty())
        {
            LOGGER.error("Please correct project configuration files and pass them in the command line "
                + "like this: wres executeConfigProject c:/path/to/config1.xml c:/path/to/config2.xml");
            return false;
        }
        else
        {
            LOGGER.info("Successfully unmarshalled {} project configuration(s), validating further...",
                        projectConfiggies.size());
        }

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
     * @param metricExecutor an optional {@link ExecutorService} for processing metrics
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processProjectConfig(ProjectConfigPlus projectConfigPlus,
                                         ExecutorService processPairExecutor,
                                         ExecutorService metricExecutor)
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
            if(!processFeature(nextFeature, projectConfigPlus, processPairExecutor, metricExecutor))
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
     * @param metricExecutor an optional {@link ExecutorService} for processing metrics
     * @return true if the project processed successfully, false otherwise
     */

    private boolean processFeature(Feature feature,
                                   ProjectConfigPlus projectConfigPlus,
                                   ExecutorService processPairExecutor,
                                   ExecutorService metricExecutor)
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
                                                                                   metricExecutor,
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

        // Queue the various tasks by lead time (lead time is the pooling dimension for metric calculation here)
        final List<CompletableFuture<?>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // Iterate
        for(Future<MetricInput<?>> nextInput: metricInputs)
        {
            // Complete all tasks asynchronously:
            // 1. Get some pairs from the database
            // 2. Compute the metrics
            // 3. Process any intermediate verification results
            final CompletableFuture<Void> c =
                                            CompletableFuture.supplyAsync(new PairsByLeadProcessor(nextInput),
                                                                          processPairExecutor)
                                                             .thenApplyAsync(processor, processPairExecutor)
                                                             .thenAcceptAsync(new IntermediateResultProcessor(feature,
                                                                                                              projectConfigPlus),
                                                                              processPairExecutor);

            //Add the future to the list
            listOfFutures.add(c);
        }

        // Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
        try
        {
            doAllOrException(listOfFutures).join();
        }
        catch(final Exception e)
        {
            LOGGER.error("While processing feature '{}'.", feature.getLocation().getLid(), e);
            return false;
        }

        // Complete the end-of-pipeline processing
        if(processor.hasStoredMetricOutput())
        {
            processCachedCharts(feature,
                                projectConfigPlus,
                                processor,
                                MetricOutputGroup.SCALAR,
                                MetricOutputGroup.VECTOR);

            try
            {
                boolean filesWritten = writeOutputFiles( projectConfig,
                                                         feature,
                                                         processor.getStoredMetricOutput() );
                if (!filesWritten)
                {
                    return false;
                }
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn("Interrupted while writing output files.");
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                LOGGER.error("While writing output files: ", e );
                return false;
            }

            if(LOGGER.isInfoEnabled())
            {
                LOGGER.info("Completed processing of feature '{}'.", feature.getLocation().getLid());
            }
        }
        else
        {
            if(LOGGER.isInfoEnabled())
            {
                LOGGER.info("No cached outputs to generate for feature '{}'.", feature.getLocation().getLid());
            }
        }
        return true;
    }

    private boolean writeOutputFiles( ProjectConfig projectConfig,
                                      Feature feature,
                                      MetricOutputForProjectByLeadThreshold storedMetricOutput )
            throws InterruptedException, ExecutionException
    {
        boolean result = true;
        if ( projectConfig.getOutputs() == null
             || projectConfig.getOutputs().getDestination() == null )
        {
            LOGGER.info("No numeric output files specified for project.");
            return false;
        }

        for ( DestinationConfig d : projectConfig.getOutputs().getDestination() )
        {
            Map<Integer,StringJoiner> rows = new TreeMap<>();
            StringJoiner headerRow = new StringJoiner( "," );
            headerRow.add("LEAD_TIME");

            if ( d.getType() == DestinationType.NUMERIC )
            {
                for ( Map.Entry<MapBiKey<MetricConstants,MetricConstants>, MetricOutputMapByLeadThreshold<ScalarOutput>> m
                        : storedMetricOutput.getScalarOutput().entrySet() )
                {
                    String name = m.getKey().getFirstKey().name();
                    String secondName = m.getKey().getSecondKey().name();

                    for ( Threshold t : m.getValue().keySetByThreshold() )
                    {
                        String column = name + "_" + secondName + "_" + t;
                        headerRow.add( column );

                        for ( MapBiKey<Integer, Threshold> key : m.getValue().sliceByThreshold( t ).keySet() )
                        {

                            if ( rows.get( key.getFirstKey() ) == null )
                            {
                                StringJoiner row = new StringJoiner( "," );
                                row.add( Integer.toString( key.getFirstKey() ) );
                                rows.put( key.getFirstKey(), row );
                            }

                            StringJoiner row = rows.get( key.getFirstKey() );

                            row.add( m.getValue().get( key ).toString() );
                        }
                    }
                }
            }

            Path outputPath = Paths.get( d.getPath() + feature.getLocation().getLid() + ".csv" );
            try (BufferedWriter w =
                    Files.newBufferedWriter( outputPath,
                                             StandardCharsets.UTF_8,
                                             StandardOpenOption.CREATE))
            {
                w.write( headerRow.toString() );
                w.write( System.lineSeparator() );
                for ( StringJoiner row : rows.values() )
                {
                    w.write( row.toString() );
                    w.write( System.lineSeparator() );
                }
            }
            catch ( IOException ioe )
            {
                LOGGER.error("Failed to write to {}", outputPath, ioe);
                result = false;
            }
        }

        return result;
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param feature the feature for which the charts are defined
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
        if(!processor.hasStoredMetricOutput())
        {
            LOGGER.error("No cached outputs to process.");
            return false;
        }

        // True until failed
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

        //Check for results
        if(Objects.isNull(scalarResults))
        {
            LOGGER.error("No scalar outputs from which to generate charts.");
            return false;
        }

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
                                                                                            null,
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
//        //Check for results
//        if(Objects.isNull(vectorResults))
//        {
//            LOGGER.error("No vector outputs from which to generate charts.");
//            return false;
//        }
//        
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
        //Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.error("No multivector outputs from which to generate charts.");
            return false;
        }

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
                                                                                                             VisualizationPlotType.RELIABILITY_DIAGRAM_FOR_LEAD,
                                                                                                             null,
                                                                                                             graphicsString);
                // Build one chart per lead time
                for(Map.Entry<Integer, ChartEngine> nextEntry: engines.entrySet())
                {
                    // Build the output file name
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
    private static class PairsByLeadProcessor implements Supplier<MetricInput<?>>
    {
        /**
         * The future metric input.
         */
        private final Future<MetricInput<?>> input;

        /**
         * Construct.
         * 
         * @param input the future metric input
         */

        private PairsByLeadProcessor(final Future<MetricInput<?>> input)
        {
            this.input = input;
        }

        @Override
        public MetricInput<?> get()
        {
            MetricInput<?> nextInput = null;
            try
            {
                nextInput = input.get();
                if(LOGGER.isInfoEnabled())
                {
                    LOGGER.info("Completed processing of pairs for feature '{}' at lead time {}.",
                                 nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                 nextInput.getMetadata().getLeadTime());
                }
            }
            catch(InterruptedException e)
            {
                LOGGER.error("Interrupted while processing pairs.", e);
                Thread.currentThread().interrupt();
            }
            catch(Exception e)
            {
                LOGGER.error("While processing pairs:", e);
            }
            return nextInput;
        }
    }

    /**
     * A function that does something with a set of results (in this case, prints to a logger).
     */

    private static class IntermediateResultProcessor implements Consumer<MetricOutputForProjectByLeadThreshold>
    {

        /**
         * The project configuration.
         */

        private final ProjectConfigPlus projectConfigPlus;

        /**
         * The feature.
         */

        private final Feature feature;

        /**
         * Construct.
         * 
         * @param projectConfigPlus the project configuration
         */

        private IntermediateResultProcessor(Feature feature, ProjectConfigPlus projectConfigPlus)
        {
            this.projectConfigPlus = projectConfigPlus;
            this.feature = feature;
        }

        @Override
        public void accept(final MetricOutputForProjectByLeadThreshold input)
        {
            MetricOutputMetadata meta = null;
            try
            {
                if(input.hasOutput(MetricOutputGroup.MULTIVECTOR))
                {
                    processMultiVectorCharts(feature, projectConfigPlus, input.getMultiVectorOutput());
                    meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    if(LOGGER.isDebugEnabled())
                    {
                        LOGGER.debug("Completed processing of intermediate metrics results for feature '{}' at lead time {}.",
                                     meta.getIdentifier().getGeospatialID(),
                                     meta.getLeadTime());
                    }
                }
            }
            catch(Exception e)
            {
                LOGGER.error("While processing intermediate results:", e);
            }
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
     * Kill off the executors passed in even if there are remaining tasks.
     *
     * @param executor the executor to shutdown
     */
    private static void shutDownGracefully(ExecutorService executor)
    {
        if(Objects.isNull(executor))
        {
            return;
        }
        // (There are probably better ways to do this, e.g. awaitTermination)
        int processingSkipped = 0;
        int i = 0;
        boolean deathTime = false;
        while(!executor.isTerminated())
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
                    processingSkipped += executor.shutdownNow().size();
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

    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

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
