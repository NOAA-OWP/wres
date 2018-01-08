package wres;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import wres.config.ProjectConfigException;
import wres.config.Validation;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.PlotTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.outputs.VectorOutput;
import wres.engine.statistics.metric.ConfigMapper;
import wres.engine.statistics.metric.MetricConfigurationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricProcessor;
import wres.engine.statistics.metric.MetricProcessorByTime;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.config.SystemSettings;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.retrieval.InputGenerator;
import wres.io.retrieval.IterationFailedException;
import wres.io.utilities.NoDataException;
import wres.io.writing.ChartWriter;
import wres.io.writing.ChartWriter.ChartWritingException;
import wres.io.writing.CommaSeparated;
import wres.util.ProgressMonitor;
import wres.vis.ChartEngineFactory;

/**
 * A complete implementation of a processing pipeline originating from one or more {@link ProjectConfig}.
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
        Integer result = MainFunctions.FAILURE;

        // Unmarshal the configurations
        final List<ProjectConfigPlus> projectConfiggies = getProjects(args);

        if ( !projectConfiggies.isEmpty() )
        {
            LOGGER.info( "Successfully unmarshalled {} project "
                         + "configuration(s),  validating further...",
                         projectConfiggies.size() );
        }
        else
        {
            LOGGER.error( "Please correct project configuration files and pass "
                          + "them in the command line like this: "
                          + "bin/wres.bat execute c:/path/to/config1.xml "
                          + "c:/path/to/config2.xml" );
            return 1;
        }

        // Validate unmarshalled configurations
        final boolean validated = Validation.validateProjects(projectConfiggies);
        if ( validated )
        {
            LOGGER.info( "Successfully validated {} project configuration(s). "
                         + "Beginning execution...",
                         projectConfiggies.size() );
        }
        else
        {
            LOGGER.error( "One or more projects did not pass validation.");
            return 1;
        }

        // Build a processing pipeline
        // Essential to use a separate thread pool for thresholds and metrics as ArrayBlockingQueue operates a FIFO 
        // policy. If dependent tasks (thresholds) are queued ahead of independent ones (metrics) in the same pool, 
        // there is a DEADLOCK probability       
        ThreadFactory pairFactory = runnable -> new Thread( runnable, "Pair Thread" );
        ThreadFactory thresholdFactory = runnable -> new Thread( runnable, "Threshold Dispatch Thread" );
        ThreadFactory metricFactory = runnable -> new Thread( runnable, "Metric Thread" );
        // Processes pairs       
        ThreadPoolExecutor pairExecutor = new ThreadPoolExecutor( SystemSettings.maximumThreadCount(),
                                                                  SystemSettings.maximumThreadCount(),
                                                                  SystemSettings.poolObjectLifespan(),
                                                                  TimeUnit.MILLISECONDS,
                                                                  new ArrayBlockingQueue<>( SystemSettings.maximumThreadCount()
                                                                                            * 5 ),
                                                                  pairFactory );
        // Dispatches thresholds
        ExecutorService thresholdExecutor = Executors.newSingleThreadExecutor( thresholdFactory );
        // Processes metrics
        ThreadPoolExecutor metricExecutor = new ThreadPoolExecutor( SystemSettings.maximumThreadCount(),
                                                                    SystemSettings.maximumThreadCount(),
                                                                    SystemSettings.poolObjectLifespan(),
                                                                    TimeUnit.MILLISECONDS,
                                                                    new ArrayBlockingQueue<>( SystemSettings.maximumThreadCount()
                                                                                              * 5 ),
                                                                    metricFactory );
        // Set the rejection policy to run in the caller, slowing producers
        pairExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        metricExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );

        try
        {
            // Iterate through the configurations
            for(final ProjectConfigPlus projectConfigPlus: projectConfiggies)
            {
                // Process the next configuration
                processProjectConfig( projectConfigPlus,
                                      pairExecutor,
                                      thresholdExecutor,
                                      metricExecutor );

            }

            result = 0;
            //return 0;
        }
        catch ( WresProcessingException | IOException e )
        {

            LOGGER.error( "Could not complete project execution:", e );

            result = -1;
            //return -1;
        }
        // Shutdown
        finally
        {
            shutDownGracefully(metricExecutor);
            shutDownGracefully(thresholdExecutor);
            shutDownGracefully(pairExecutor);
        }

        return result;
    }

    /**
     * Processes a {@link ProjectConfigPlus} using a prescribed {@link ExecutorService} for each of the pairs, 
     * thresholds and metrics.
     * 
     * @param projectConfigPlus the project configuration
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws IOException when an issue occurs during ingest
     * @throws WresProcessingException when an issue occurs during processing
     */

    private void   processProjectConfig( final ProjectConfigPlus projectConfigPlus,
                                         final ExecutorService pairExecutor,
                                         final ExecutorService thresholdExecutor,
                                         final ExecutorService metricExecutor )
            throws IOException
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        ProgressMonitor.setShowStepDescription( true );
        ProgressMonitor.resetMonitor();

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Beginning ingest for project {}...",
                          projectConfigPlus.getCanonicalPath() );
        }

        // Need to ingest first
        List<IngestResult> availableSources = Operations.ingest( projectConfig);

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Finished ingest for project {}...",
                          projectConfigPlus.getCanonicalPath() );
        }

        ProgressMonitor.setShowStepDescription( false );

        Set<Feature> decomposedFeatures;

        try
        {
            decomposedFeatures = Operations.decomposeFeatures( projectConfig );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Failed to retrieve the set of features.", e );
        }

        List<Feature> successfulFeatures = new ArrayList<>();
        List<Feature> missingDataFeatures = new ArrayList<>();

        for ( Feature feature : decomposedFeatures )
        {
            ProgressMonitor.resetMonitor();
            FeatureProcessingResult result =
                    processFeature( feature,
                                    projectConfigPlus,
                                    availableSources,
                                    pairExecutor,
                                    thresholdExecutor,
                                    metricExecutor );

            if ( result.hadData() )
            {
                successfulFeatures.add( result.getFeature() );
            }
            else
            {
                missingDataFeatures.add( result.getFeature() );

                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "Feature {} failed due to",
                                  result.getFeature(),
                                  result.getCause() );
                }
            }
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "The following features succeeded: {}",
                         ConfigHelper.getFeaturesDescription( successfulFeatures ) );
        }

        if ( LOGGER.isInfoEnabled() && !missingDataFeatures.isEmpty() )
        {
            LOGGER.info( "The following features were missing data: {}",
                         ConfigHelper.getFeaturesDescription(
                                 missingDataFeatures ) );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            if ( decomposedFeatures.size() == successfulFeatures.size() )
            {
                LOGGER.info( "All features in project {} were successfully "
                             + "evaluated.",
                             projectConfigPlus.getCanonicalPath() );
            }
            else
            {
                LOGGER.info( "{} out of {} features in project {} were successfully "
                             + "evaluated, {} out of {} features were missing data.",
                             successfulFeatures.size(),
                             decomposedFeatures.size(),
                             projectConfigPlus.getCanonicalPath(),
                             missingDataFeatures.size(),
                             decomposedFeatures.size() );
            }
        }
    }


    /**
     * Processes a {@link ProjectConfigPlus} for a specific {@link Feature} using a prescribed {@link ExecutorService}
     * for each of the pairs, thresholds and metrics.
     * 
     * @param feature the feature to process
     * @param projectConfigPlus the project configuration
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws WresProcessingException when an error occurs during processing
     */

    private FeatureProcessingResult processFeature( final Feature feature,
                                                    final ProjectConfigPlus projectConfigPlus,
                                                    final List<IngestResult> projectSources,
                                                    final ExecutorService pairExecutor,
                                                    final ExecutorService thresholdExecutor,
                                                    final ExecutorService metricExecutor )
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        if(LOGGER.isInfoEnabled())
        {
            // This will NOT work if we are using any other feature format
            LOGGER.info( "Processing feature '{}'", feature.getLocationId() );
        }

        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        // Construct a processor that retains all output types required at the end of the pipeline: SCALAR and VECTOR
        // TODO: support additional processor types
        MetricProcessorByTime processor = null;
        try
        {
            processor = MetricFactory.getInstance(DATA_FACTORY).getMetricProcessorByTime(projectConfig,
                                                                                   thresholdExecutor,          
                                                                                   metricExecutor,
                                                                                   MetricOutputGroup.SCALAR,
                                                                                   MetricOutputGroup.VECTOR);
        }
        catch(final MetricConfigurationException e)
        {
            throw new WresProcessingException( "While processing the metric configuration:",
                                               e );
        }

        // Build an InputGenerator for the next feature
        InputGenerator metricInputs = null;
        try
        {
            metricInputs = Operations.getInputs( projectConfig,
                                                 feature,
                                                 projectSources );
        }
        catch ( IngestException e )
        {
            throw new WresProcessingException( "While attempting to get "
                                               + "metric inputs:", e );
        }

        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        final List<CompletableFuture<?>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        try
        {
            // Iterate
            for ( final Future<MetricInput<?>> nextInput : metricInputs )
            {
                // Complete all tasks asynchronously:
                // 1. Get some pairs from the database
                // 2. Compute the metrics
                // 3. Process any intermediate verification results
                final CompletableFuture<Void> c =
                        CompletableFuture.supplyAsync( new PairsByTimeWindowProcessor(
                                                               nextInput ),
                                                       pairExecutor )
                                         .thenApplyAsync( processor,
                                                          pairExecutor )
                                         .thenAcceptAsync( new IntermediateResultProcessor(
                                                                   feature,
                                                                   projectConfigPlus ),
                                                           pairExecutor )
                                         .thenAccept(
                                                 aVoid -> ProgressMonitor.completeStep() );

                //Add the future to the list
                listOfFutures.add( c );
            }
        }
        catch ( IterationFailedException re )
        {
            if ( Control.wasNoDataExceptionInThisStack( re ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    re );
            }
        }

        // Complete all tasks or one exceptionally: join() is blocking, representing a final sink for the results
        try
        {
            doAllOrException(listOfFutures).join();
        }
        catch( CompletionException e )
        {
            String message = "Error while processing feature "
                             + ConfigHelper.getFeatureDescription( feature );
            LOGGER.error( message, e );

            if ( Control.wasNoDataExceptionInThisStack( e ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    e );
            }

            // Otherwise, propagate the exception up to the top.
            throw new WresProcessingException( message, e );
        }

        // Generated stored output if available
        if ( processor.hasStoredMetricOutput() )
        {
            processCachedProducts( projectConfigPlus, processor, feature );
        }
        else if ( LOGGER.isInfoEnabled() )
        {
            String description = ConfigHelper.getFeatureDescription( feature );
            LOGGER.info( "No metric results generated for feature: '"
                         + description + "'" );
        }

        return new FeatureProcessingResult( feature, true, null );
    }

    /**
     * Completes the processing of products, including graphical and numerical products, at the end of a processing 
     * pipeline using the cached {@link MetricOutput} stored in the {@link MetricProcessor}, and in keeping with 
     * the supplied {@link ProjectConfig}.
     * 
     * @param projectConfigPlus the project configuration
     * @param processor the processor with cached metric outputs
     * @param feature the feature being processed
     */

    private static void   processCachedProducts( ProjectConfigPlus projectConfigPlus,
                                               MetricProcessorByTime processor,
                                               Feature feature )
    {
        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        //Generate graphical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.GRAPHIC ) )
        {
            LOGGER.debug( "Beginning to build charts for feature {}...",
                          feature.getLocationId() );

            processCachedCharts( feature,
                                 projectConfigPlus,
                                 processor,
                                 MetricOutputGroup.SCALAR,
                                 MetricOutputGroup.VECTOR );

            LOGGER.debug( "Finished building charts for feature {}.",
                          feature.getLocationId() );
        }

        //Generate numerical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.NUMERIC ) )
        {
            LOGGER.debug( "Beginning to write numeric output for feature {}...",
                          feature.getLocationId() );

            try
            {
                CommaSeparated.writeOutputFiles( projectConfig,
                                                 feature,
                                                 processor.getStoredMetricOutput() );

            }
            catch ( final ProjectConfigException pce )
            {
                throw new WresProcessingException(
                                                   "Please include valid numeric output clause(s) in"
                                                   + " project configuration. Example: <destination>"
                                                   + "<path>c:/Users/myname/wres_output/</path>"
                                                   + "</destination>",
                                                   pce );
            }
            catch ( IOException e )
            {
                throw new WresProcessingException( "While writing output files: ",
                                                   e );
            }

            LOGGER.debug( "Finished writing numeric output for feature {}.",
                          feature.getLocationId() );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Completed processing of feature '{}'.", feature.getLocationId() );
        }
    }

    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param feature the feature for which the charts are defined
     * @param projectConfigPlus the project configuration
     * @param processor the {@link MetricProcessor} that contains the results for all chart types
     * @param outGroup the {@link MetricOutputGroup} for which charts are required
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processCachedCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricProcessorByTime processor,
                                               final MetricOutputGroup... outGroup)
    {
        if(!processor.hasStoredMetricOutput())
        {
            LOGGER.warn( "No cached outputs to process. ");
            return;
        }

        try
        {
            for(final MetricOutputGroup nextGroup: outGroup)
            {
                if(processor.getStoredMetricOutput().hasOutput(nextGroup))
                {
                    switch(nextGroup)
                    {
                        case SCALAR:
                            processScalarCharts( feature,
                                                 projectConfigPlus,
                                                 processor.getStoredMetricOutput()
                                                          .getScalarOutput() );
                            break;
                        case VECTOR:
                            processVectorCharts( feature,
                                                 projectConfigPlus,
                                                 processor.getStoredMetricOutput()
                                                          .getVectorOutput() );
                            break;
                        case MULTIVECTOR:
                            processMultiVectorCharts( feature,
                                                      projectConfigPlus,
                                                      processor.getStoredMetricOutput()
                                                               .getMultiVectorOutput() );
                            break;
                        case BOXPLOT:
                            processBoxPlotCharts( feature,
                                                      projectConfigPlus,
                                                      processor.getStoredMetricOutput()
                                                               .getBoxPlotOutput() );
                            break;
                        default:
                            LOGGER.warn( "Unsupported chart type {}.",
                                         nextGroup );
                            return;
                    }
                }
                else
                {
                    //Return silently: chart type not necessarily configured
                    return;
                }
            }
        }
        catch ( final MetricOutputAccessException e )
        {
            if ( Thread.currentThread().isInterrupted() )
            {
                LOGGER.warn( "Interrupted while processing charts.", e );
            }
            throw new WresProcessingException( "Error while processing charts:", e );
        }
    }

    /**
     * Processes a set of charts associated with {@link ScalarOutput} across multiple metrics, time windows, and
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param scalarResults the scalar outputs
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processScalarCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> scalarResults)
    {

        //Check for results
        if(Objects.isNull(scalarResults))
        {
            LOGGER.warn("No scalar outputs from which to generate charts.");
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<ScalarOutput>> e: scalarResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(Objects.nonNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        //Default to global type
                        if ( Objects.isNull( plotType ) )
                        {
                            plotType = dest.getGraphical().getPlotType();
                        }
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }

                    final ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine(e.getValue(),
                                                                                                      plotType,
                                                                                                      templateResourceName,
                                                                                                      graphicsString);
                    //Build the output
                    File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                    Path outputImage = Paths.get( destDir.toString(),
                                                  ConfigHelper.getFeatureDescription( feature )
                                                  + "_"
                                                  + e.getKey()
                                                  .getKey().name()
                                                  + "_"
                                                  + config.getInputs()
                                                  .getRight()
                                                  .getVariable()
                                                  .getValue()
                                                  + ".png");
                    ChartWriter.writeChart(outputImage, engine, dest);
                }
            }
        }
        catch( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating scalar charts:", e);
        }
    }

    /**
     * Processes a set of charts associated with {@link VectorOutput} across multiple metrics, time windows, and
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}. these.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param vectorResults the vector outputs
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processVectorCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricOutputMultiMapByTimeAndThreshold<VectorOutput> vectorResults)
    {
        //Check for results
        if(Objects.isNull(vectorResults))
        {
            LOGGER.warn("No vector outputs from which to generate charts.");
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<VectorOutput>> e: vectorResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(Objects.nonNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        //Default to global type
                        if ( Objects.isNull( plotType ) )
                        {
                            plotType = dest.getGraphical().getPlotType();
                        }
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }
                    final Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildVectorOutputChartEngine(e.getValue(),
                                                                        DATA_FACTORY,
                                                                        plotType,
                                                                        templateResourceName,
                                                                        graphicsString);
                    // Build the outputs
                    for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                    {
                        // Build the output file name
                        File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                        Path outputImage = Paths.get( destDir.toString(),
                                                      ConfigHelper.getFeatureDescription( feature )
                                                      + "_"
                                                      + e.getKey()
                                                      .getKey().name()
                                                      + "_"
                                                      + config.getInputs()
                                                      .getRight()
                                                      .getVariable()
                                                      .getValue()
                                                      + "_"
                                                      + ((MetricConstants)nextEntry.getKey()).name()
                                                      + ".png" );
                        ChartWriter.writeChart(outputImage, nextEntry.getValue(), dest);
                    }
                }
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating vector charts:", e );
        }
    }

    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, time windows,
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the multi-vector outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    private static void   processMultiVectorCharts( final Feature feature,
                                                    final ProjectConfigPlus projectConfigPlus,
                                                    final MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults)
    {
        //Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.warn( "No multi-vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            // Build the charts for each metric
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> e: multiVectorResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    PlotTypeSelection plotType = null;
                    String templateResourceName = null;
                    if(Objects.nonNull(nextConfig))
                    {
                        plotType = nextConfig.getPlotType();
                        //Default to global type
                        if ( Objects.isNull( plotType ) )
                        {
                            plotType = dest.getGraphical().getPlotType();
                        }
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }

                    final Map<Object, ChartEngine> engines =
                        ChartEngineFactory.buildMultiVectorOutputChartEngine(e.getValue(),
                                                                             DATA_FACTORY,
                                                                             plotType,
                                                                             templateResourceName,
                                                                             graphicsString);
                    // Build the outputs
                    for(final Map.Entry<Object, ChartEngine> nextEntry: engines.entrySet())
                    {
                        // Build the output file name
                        // TODO: adopt a more general naming convention as the pipelines expand
                        // For now, the only supported temporal pipeline is per lead time
                        Object key = nextEntry.getKey();
                        if( key instanceof TimeWindow ) 
                        {
                            key = ( (TimeWindow) key ).getLatestLeadTimeInHours();
                        }
                        File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                        Path outputImage = Paths.get( destDir.toString(),
                                                      ConfigHelper.getFeatureDescription( feature )
                                                      + "_"
                                                      + e.getKey()
                                                      .getKey().name()
                                                      + "_"
                                                      + config.getInputs()
                                                      .getRight()
                                                      .getVariable()
                                                      .getValue()
                                                      + "_"
                                                      + key
                                                      + ".png" );
                        ChartWriter.writeChart(outputImage, nextEntry.getValue(), dest);
                    }
                }
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e)
        {
            throw new WresProcessingException( "Error while generating multi-vector charts:", e );
        }
    }
    
    /**
     * Processes a set of charts associated with {@link BoxPlotOutput} across multiple metrics, time window, and 
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param boxPlotResults the box plot outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    private static void   processBoxPlotCharts( final Feature feature,
                                              final ProjectConfigPlus projectConfigPlus,
                                              final MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        //Check for results
        if ( Objects.isNull( boxPlotResults ) )
        {
            LOGGER.warn( "No box-plot outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Build charts
        try
        {
            // Build the charts for each metric
            for(final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> e: boxPlotResults.entrySet())
            {
                List<DestinationConfig> destinations =
                        ConfigHelper.getGraphicalDestinations( config );

                for ( DestinationConfig dest : destinations )
                {
                    final String graphicsString = projectConfigPlus.getGraphicsStrings().get(dest);
                    // Build the chart engine
                    final MetricConfig nextConfig = getMetricConfiguration(e.getKey().getKey(), config);
                    String templateResourceName = null;
                    if(Objects.nonNull(nextConfig))
                    {
                        templateResourceName = nextConfig.getTemplateResourceName();
                    }

                    final Map<Pair<TimeWindow, Threshold>, ChartEngine> engines =
                            ChartEngineFactory.buildBoxPlotChartEngine( e.getValue(),
                                                                        DATA_FACTORY,
                                                                        templateResourceName,
                                                                        graphicsString );
                    // Build the outputs
                    for(final Map.Entry<Pair<TimeWindow, Threshold>, ChartEngine> nextEntry: engines.entrySet())
                    {
                        // Build the output file name
                        File destDir = ConfigHelper.getDirectoryFromDestinationConfig( dest );
                        // TODO: adopt a more general naming convention as the pipelines expand
                        // For now, the only temporal pipeline is by lead time
                        long key = nextEntry.getKey().getLeft().getLatestLeadTimeInHours();
                        Path outputImage = Paths.get( destDir.toString(),
                                                      ConfigHelper.getFeatureDescription( feature )
                                                      + "_"
                                                      + e.getKey()
                                                      .getKey().name()
                                                      + "_"
                                                      + config.getInputs()
                                                      .getRight()
                                                      .getVariable()
                                                      .getValue()
                                                      + "_"
                                                      + key
                                                      + ".png" );
                        ChartWriter.writeChart(outputImage, nextEntry.getValue(), dest);
                    }
                }
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e)
        {
            throw new WresProcessingException( "Error while generating multi-vector charts:", e );
        }
    }    

    /**
     * Get project configurations from command line file args. If there are no command line args, look in System
     * Settings for directory to scan for configurations.
     *
     * @param args the paths to the projects
     * @return the successfully found, read, unmarshalled project configs
     */
    private static List<ProjectConfigPlus> getProjects(final String[] args)
    {
        final List<Path> existingProjectFiles = new ArrayList<>();

        if(args.length > 0)
        {
            for(final String arg: args)
            {
                final Path path = Paths.get(arg);
                if(path.toFile().exists())
                {
                    existingProjectFiles.add(path);
                }
                else
                {
                    LOGGER.warn("Project configuration file {} does not exist!", path);
                }
            }
        }

        final List<ProjectConfigPlus> projectConfiggies = new ArrayList<>();

        for(final Path path: existingProjectFiles)
        {
            try
            {
                final ProjectConfigPlus projectConfigPlus = ProjectConfigPlus.from(path);
                projectConfiggies.add(projectConfigPlus);
            }
            catch(final IOException ioe)
            {
                LOGGER.error("Could not read project configuration: ", ioe);
            }
        }
        return projectConfiggies;
    }

    /**
     * Task that waits for pairs to arrive and then returns them.
     */
    private static class PairsByTimeWindowProcessor implements Supplier<MetricInput<?>>
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

        private PairsByTimeWindowProcessor(final Future<MetricInput<?>> input)
        {
            Objects.requireNonNull( input, "Specify a non-null input for the processor." );
            this.input = input;
        }

        @Override
        public MetricInput<?> get()
        {
            MetricInput<?> nextInput = null;
            try
            {
                nextInput = input.get();
                if(LOGGER.isDebugEnabled())
                {
                    LOGGER.debug("Completed processing of pairs for feature '{}' and time window {}.",
                                nextInput.getMetadata().getIdentifier().getGeospatialID(),
                                nextInput.getMetadata().getTimeWindow());
                }
            }
            catch(final InterruptedException e)
            {
                LOGGER.warn( "Interrupted while processing pairs:", e );
                Thread.currentThread().interrupt();
            }
            catch( ExecutionException e )
            {
                throw new WresProcessingException( "While processing pairs:", e );
            }

            return nextInput;
        }
    }

    /**
     * A task the processes an intermediate set of results.
     */

    private static class IntermediateResultProcessor implements Consumer<MetricOutputForProjectByTimeAndThreshold>
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
         * @param feature the feature
         * @param projectConfigPlus the project configuration
         */

        private IntermediateResultProcessor(final Feature feature, final ProjectConfigPlus projectConfigPlus)
        {
            Objects.requireNonNull( feature, "Specify a non-null feature for the processor." );
            Objects.requireNonNull( projectConfigPlus, "Specify a non-null configuration for the processor." );
            this.projectConfigPlus = projectConfigPlus;
            this.feature = feature;
        }

        @Override
        public void accept(final MetricOutputForProjectByTimeAndThreshold input)
        {
            MetricOutputMetadata meta = null;
            try
            {
                if ( configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                  DestinationType.GRAPHIC ) )
                {
                    //Multivector output
                    if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR ) )
                    {
                        processMultiVectorCharts( feature,
                                                  projectConfigPlus,
                                                  input.getMultiVectorOutput() );
                        meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    //Box-plot output
                    if ( input.hasOutput( MetricOutputGroup.BOXPLOT ) )
                    {
                        processBoxPlotCharts( feature,
                                              projectConfigPlus,
                                              input.getBoxPlotOutput() );
                        meta = input.getBoxPlotOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    if ( LOGGER.isDebugEnabled() )
                    {
                        if ( Objects.nonNull( meta ) )
                        {
                            LOGGER.debug( "Completed processing of intermediate metrics results for feature '{}' "
                                          + "and time window {}.",
                                          meta.getIdentifier().getGeospatialID(),
                                          meta.getTimeWindow() );
                        }
                        else
                        {
                            LOGGER.debug( "Completed processing of intermediate metrics results for feature '{}' with "
                                          + "unknown time window.",
                                          feature.getLocationId() );
                        }
                    }
                }
            }
            catch ( final MetricOutputAccessException e )
            {
                if ( Thread.currentThread().isInterrupted() )
                {
                    LOGGER.warn( "Interrupted while processing intermediate results:", e );
                }
                throw new WresProcessingException( "Error while processing intermediate results:", e );
            }
        }
    }

    /**
     * An exception representing that execution of a step failed.
     * Needed because Java 8 Function world does not
     * deal kindly with checked Exceptions.
     */
    private static class WresProcessingException extends RuntimeException
    {
        private static final long serialVersionUID = 6988169716259295343L;

        WresProcessingException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    /**
     * Composes a list of {@link CompletableFuture} so that execution completes when all futures are completed normally
     * or any one future completes exceptionally. None of the {@link CompletableFuture} passed to this utility method
     * should already handle exceptions otherwise the exceptions will not be caught here (i.e. all futures will process
     * to completion).
     *
     * @param futures the futures to compose
     * @return the composed futures
     * @throws CompletionException if completing exceptionally 
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
    private static void shutDownGracefully(final ExecutorService executor)
    {
        if(Objects.isNull(executor))
        {
            return;
        }

        executor.shutdown();

        try
        {
            executor.awaitTermination( 5, TimeUnit.SECONDS );
        }
        catch ( InterruptedException ie )
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Locates the metric configuration corresponding to the input {@link MetricConstants} or null if no corresponding
     * configuration could be found.
     * 
     * @param metric the metric
     * @param config the project configuration
     * @return the metric configuration or null
     */

    private static MetricConfig getMetricConfiguration(final MetricConstants metric, final ProjectConfig config)
    {
        // Find the corresponding configuration
        final Optional<MetricConfig> returnMe = config.getOutputs().getMetric().stream().filter(a -> {
            try
            {
                return metric.equals(ConfigMapper.from(a.getName()));
            }
            catch(final MetricConfigurationException e)
            {
                LOGGER.error("Could not map metric name '{}' to metric configuration.", metric, e);
                return false;
            }
        }).findFirst();
        return returnMe.isPresent() ? returnMe.get() : null;
    }


    /**
     * Returns true if the given config has one or more of given output type.
     * @param config the config to search
     * @param type the type of output to look for
     * @return true if the output type is present, false otherwise
     */

    private static boolean configNeedsThisTypeOfOutput( ProjectConfig config,
                                                        DestinationType type )
    {
        if ( config.getOutputs() == null
             || config.getOutputs().getDestination() == null )
        {
            LOGGER.debug( "No destinations specified for config {}", config );
            return false;
        }

        for ( DestinationConfig d : config.getOutputs().getDestination() )
        {
            if ( d.getType().equals( type ) )
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(Control.class);

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
        final String maxThreadsStr = System.getProperty(MAX_THREADS_PROP_NAME);
        int maxThreads;
        try
        {
            maxThreads = Integer.parseInt(maxThreadsStr);
        }
        catch(final NumberFormatException nfe)
        {
            maxThreads = SystemSettings.maximumThreadCount();
        }

        // Since ForkJoinPool goes ape when this is 2 or less...
        if ( maxThreads >= 3 )
        {
            MAX_THREADS = maxThreads;
        }
        else
        {
            LOGGER.warn( "Java -D property {} or SystemSettings "
                         + "maximumThreadCount was likely less than 1, setting "
                         + " Control.MAX_THREADS to 3",
                         MAX_THREADS_PROP_NAME );

            MAX_THREADS = 3;
        }
    }

    private static class FeatureProcessingResult
    {
        private final Feature feature;
        private final boolean hadData;
        private final Throwable cause;

        private FeatureProcessingResult( Feature feature,
                                         boolean hadData,
                                         Throwable cause )
        {
            Objects.requireNonNull( feature );
            this.feature = feature;
            this.hadData = hadData;
            this.cause = cause;
        }

        Feature getFeature()
        {
            return this.feature;
        }

        boolean hadData()
        {
            return this.hadData;
        }

        Throwable getCause()
        {
            return this.cause;
        }

        @Override
        public String toString()
        {
            if ( hadData() )
            {
                return "Feature "
                       + ConfigHelper.getFeatureDescription( this.getFeature() )
                       + " had data.";
            }
            else
            {
                return "Feature "
                       + ConfigHelper.getFeatureDescription( this.getFeature() )
                       + " had no data: "
                       + this.getCause();
            }
        }
    }

    /**
     * Look at a chain of exceptions, returns true if ANY is a NoDataException
     * @param e the exception to look at
     * @return true when a NoDataException is found, false otherwise
     */
    private static boolean wasNoDataExceptionInThisStack( Exception e )
    {
        Throwable cause = e;
        while ( cause != null )
        {
            if ( cause instanceof NoDataException )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }

}
