package wres.control;

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
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.PlotTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
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

            return 0;
        }
        catch ( WresProcessingException | IOException e )
        {

            LOGGER.error( "Could not complete project execution:", e );

            return -1;
        }
        // Shutdown
        finally
        {
            shutDownGracefully(metricExecutor);
            shutDownGracefully(thresholdExecutor);
            shutDownGracefully(pairExecutor);
        }
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

        LOGGER.debug( "Beginning ingest for project {}...", projectConfigPlus );

        // Need to ingest first
        List<IngestResult> availableSources = Operations.ingest( projectConfig);

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

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
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( "Not enough data found for feature {}:",
                                 ConfigHelper.getFeatureDescription( result.getFeature() ),
                                 result.getCause() );
                }
                missingDataFeatures.add( result.getFeature() );
            }
        }

        printFeaturesReport( projectConfigPlus,
                             decomposedFeatures,
                             successfulFeatures,
                             missingDataFeatures );
    }


    /**
     * Print a report to the log about which features were successful and not.
     * Also, throw an exception if zero features were successful.
     * @param projectConfigPlus the project config just processed
     * @param decomposedFeatures the features decomposed from the config
     * @param successfulFeatures the features that succeeded
     * @param missingDataFeatures the features that were missing data
     * @throws WresProcessingException when zero features were successful
     */

    private void printFeaturesReport( final ProjectConfigPlus projectConfigPlus,
                                      final Set<Feature> decomposedFeatures,
                                      final List<Feature> successfulFeatures,
                                      final List<Feature> missingDataFeatures )
    {
        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "The following features succeeded: {}",
                         ConfigHelper.getFeaturesDescription( successfulFeatures ) );

            if ( !missingDataFeatures.isEmpty() )
            {
                LOGGER.info( "The following features were missing data: {}",
                             ConfigHelper.getFeaturesDescription( missingDataFeatures ) );
            }
        }

        if ( successfulFeatures.isEmpty() )
        {
            throw new WresProcessingException( "No features were successful.",
                                               null );
        }

        if ( LOGGER.isInfoEnabled() )
        {
            if ( decomposedFeatures.size() == successfulFeatures.size() )
            {
                LOGGER.info( "All features in project {} were successfully "
                             + "evaluated.",
                             projectConfigPlus );
            }
            else
            {
                LOGGER.info( "{} out of {} features in project {} were successfully "
                             + "evaluated, {} out of {} features were missing data.",
                             successfulFeatures.size(),
                             decomposedFeatures.size(),
                             projectConfigPlus,
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
     * @param projectSources the project sources
     * @param pairExecutor the {@link ExecutorService} for processing pairs
     * @param thresholdExecutor the {@link ExecutorService} for processing thresholds
     * @param metricExecutor the {@link ExecutorService} for processing metrics
     * @throws WresProcessingException when an error occurs during processing
     * @return a feature result
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
        MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor = null; // Ensemble processor
        MetricProcessorByTime<EnsemblePairs> ensembleProcessor = null; // Single-valued processor
        MetricProcessorByTime<?> processor = null; // The processor used
        try
        {
            MetricOutputGroup[] cacheMe = null;
            // Multivector outputs by threshold must be cached across time windows
            if ( hasMultiVectorOutputsToCache( projectConfig ) )
            {
                cacheMe = new MetricOutputGroup[] { MetricOutputGroup.SCALAR, MetricOutputGroup.VECTOR,
                                                    MetricOutputGroup.MULTIVECTOR };
            }
            else
            {
                cacheMe = new MetricOutputGroup[] { MetricOutputGroup.SCALAR, MetricOutputGroup.VECTOR };
            }

            MetricFactory mF = MetricFactory.getInstance( DATA_FACTORY );
            DatasourceType type = projectConfig.getInputs().getRight().getType();
            if ( type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) || type.equals( DatasourceType.SIMULATIONS ) )
            {
                singleValuedProcessor = mF.ofMetricProcessorByTimeSingleValuedPairs( projectConfig,
                                                                                     thresholdExecutor,
                                                                                     metricExecutor,
                                                                                     cacheMe );
                processor = singleValuedProcessor;
            }
            else
            {
                ensembleProcessor = mF.ofMetricProcessorByTimeEnsemblePairs( projectConfig,
                                                                             thresholdExecutor,
                                                                             metricExecutor,
                                                                             cacheMe );
                processor = ensembleProcessor;
            }
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
                // 4. Monitor progress
                final CompletableFuture<Void> c =
                        CompletableFuture.supplyAsync( new PairsByTimeWindowProcessor(
                                                                                       nextInput,
                                                                                       singleValuedProcessor,
                                                                                       ensembleProcessor ),
                                                       pairExecutor )
                                         .thenAcceptAsync( new IntermediateResultProcessor(
                                                                                            feature,
                                                                                            projectConfigPlus,
                                                                                            processor ),
                                                           pairExecutor )
                                         .thenAccept(
                                                      aVoid -> ProgressMonitor.completeStep() );

                // Add the future to the list
                listOfFutures.add( c );
            }
        }
        catch ( IterationFailedException re )
        {
            if ( Control.wasInsufficientDataOrNoDataInThisStack( re ) )
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
            // If there was simply not enough data for this feature, OK
            if ( Control.wasInsufficientDataOrNoDataInThisStack( e ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    e );
            }

            // Otherwise, chain and propagate the exception up to the top.
            String message = "Error while processing feature "
                             + ConfigHelper.getFeatureDescription( feature );
            throw new WresProcessingException( message, e );
        }

        // Generate cached output if available
        if ( processor.hasCachedMetricOutput() )
        {
            processCachedProducts( projectConfigPlus, processor, feature );
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
                                               MetricProcessorByTime<?> processor,
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
                                 processor );

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
                                                 processor.getCachedMetricOutput() );

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
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void   processCachedCharts( final Feature feature,
                                               final ProjectConfigPlus projectConfigPlus,
                                               final MetricProcessorByTime<?> processor )
    {
        if(!processor.hasCachedMetricOutput())
        {
            LOGGER.warn( "No cached outputs to process. ");
            return;
        }

        try
        {
            // Process scalar charts
            if ( processor.willCacheMetricOutput( MetricOutputGroup.SCALAR )
                 && processor.getCachedMetricOutput().hasOutput( MetricOutputGroup.SCALAR ) )
            {
                processScalarCharts( feature,
                                     projectConfigPlus,
                                     processor.getCachedMetricOutput()
                                              .getScalarOutput() );
            }
            // Process vector charts
            if ( processor.willCacheMetricOutput( MetricOutputGroup.VECTOR )
                 && processor.getCachedMetricOutput().hasOutput( MetricOutputGroup.VECTOR ) )
            {
                processVectorCharts( feature,
                                     projectConfigPlus,
                                     processor.getCachedMetricOutput()
                                              .getVectorOutput() );
            }
            // Process multivector charts
            if ( processor.willCacheMetricOutput( MetricOutputGroup.MULTIVECTOR )
                 && processor.getCachedMetricOutput().hasOutput( MetricOutputGroup.MULTIVECTOR ) )
            {
                processMultiVectorCharts( feature,
                                          projectConfigPlus,
                                          processor.getCachedMetricOutput()
                                                   .getMultiVectorOutput() );
            }
            // Process box plot charts
            if ( processor.willCacheMetricOutput( MetricOutputGroup.BOXPLOT )
                 && processor.getCachedMetricOutput().hasOutput( MetricOutputGroup.BOXPLOT ) )
            {
                processBoxPlotCharts( feature,
                                      projectConfigPlus,
                                      processor.getCachedMetricOutput()
                                               .getBoxPlotOutput() );
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
     * @param scalarResults the metric results
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void processScalarCharts( final Feature feature,
                                             final ProjectConfigPlus projectConfigPlus,
                                             final MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> scalarResults )
    {

        // Check for results
        if ( Objects.isNull( scalarResults ) )
        {
            LOGGER.warn( "No scalar outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<ScalarOutput>> e : scalarResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeScalarChart( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }
    
    /**
     * Writes a single chart associated with {@link ScalarOutput} for a single metric and time window, stored in a 
     * {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param scalarResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeScalarChart( Feature feature,
                                          ProjectConfigPlus projectConfigPlus,
                                          DestinationConfig destConfig,
                                          MetricConstants metricId,
                                          MetricOutputMapByTimeAndThreshold<ScalarOutput> scalarResults )
    {
        // Build charts
        try
        {
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig = getNamedConfigOrAllValid( metricId, projectConfigPlus.getProjectConfig() );
            // Default to global type parameter
            PlotTypeSelection plotType = destConfig.getGraphical().getPlotType();
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                // Local type parameter
                if ( nextConfig.getPlotType() != null )
                {
                    plotType = nextConfig.getPlotType();
                }
                templateResourceName = nextConfig.getTemplateResourceName();
            }

            ChartEngine engine = ChartEngineFactory.buildGenericScalarOutputChartEngine( scalarResults,
                                                                                         plotType,
                                                                                         templateResourceName,
                                                                                         graphicsString );
            //Build the output
            File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
            Path outputImage = Paths.get( destDir.toString(),
                                          ConfigHelper.getFeatureDescription( feature )
                                                              + "_"
                                                              + metricId.name()
                                                              + "_"
                                                              + projectConfigPlus.getProjectConfig()
                                                                                 .getInputs()
                                                                                 .getRight()
                                                                                 .getVariable()
                                                                                 .getValue()
                                                              + ".png" );
            ChartWriter.writeChart( outputImage, engine, destConfig );
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating scalar charts:", e );
        }
    }   

    /**
     * Processes a set of charts associated with {@link MultiValuedScoreOutput} across multiple metrics, time windows, 
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}. these.
     * 
     * @param feature the feature for which the chart is defined
     * @param projectConfigPlus the project configuration
     * @param vectorResults the metric results
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void processVectorCharts( final Feature feature,
                                             final ProjectConfigPlus projectConfigPlus,
                                             final MetricOutputMultiMapByTimeAndThreshold<MultiValuedScoreOutput> vectorResults )
    {
        // Check for results
        if ( Objects.isNull( vectorResults ) )
        {
            LOGGER.warn( "No vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiValuedScoreOutput>> e : vectorResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeVectorCharts( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }
    
    /**
     * Writes a set of charts associated with {@link MultiValuedScoreOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param vectorResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeVectorCharts( Feature feature,
                                          ProjectConfigPlus projectConfigPlus,
                                          DestinationConfig destConfig,
                                          MetricConstants metricId,
                                          MetricOutputMapByTimeAndThreshold<MultiValuedScoreOutput> vectorResults )
    {
        // Build charts
        try
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig =
                    getNamedConfigOrAllValid( metricId, config );
            // Default to global type parameter
            PlotTypeSelection plotType = destConfig.getGraphical().getPlotType();
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                // Local type parameter
                if ( nextConfig.getPlotType() != null )
                {
                    plotType = nextConfig.getPlotType();
                }
                templateResourceName = nextConfig.getTemplateResourceName();
            }
            Map<MetricConstants, ChartEngine> engines =
                    ChartEngineFactory.buildVectorOutputChartEngine( vectorResults,
                                                                     DATA_FACTORY,
                                                                     plotType,
                                                                     templateResourceName,
                                                                     graphicsString );
            // Build the outputs
            for ( final Map.Entry<MetricConstants, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
                Path outputImage = Paths.get( destDir.toString(),
                                              ConfigHelper.getFeatureDescription( feature )
                                                                  + "_"
                                                                  + metricId.name()
                                                                  + "_"
                                                                  + config.getInputs()
                                                                          .getRight()
                                                                          .getVariable()
                                                                          .getValue()
                                                                  + "_"
                                                                  + nextEntry.getKey().name()
                                                                  + ".png" );
                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
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
     * @param multiVectorResults the metric results
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    private static void processMultiVectorCharts( final Feature feature,
                                                  final ProjectConfigPlus projectConfigPlus,
                                                  final MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults )
    {
        // Check for results
        if(Objects.isNull(multiVectorResults))
        {
            LOGGER.warn( "No multi-vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> e : multiVectorResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeMultiVectorCharts( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }
    
    /**
     * Writes a set of charts associated with {@link MultiVectorOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param multiVectorResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeMultiVectorCharts( Feature feature,
                                                ProjectConfigPlus projectConfigPlus,
                                                DestinationConfig destConfig,
                                                MetricConstants metricId,
                                                MetricOutputMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults )
    {
        // Build charts
        try
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig = getNamedConfigOrAllValid( metricId, config );
            // Default to global type parameter
            PlotTypeSelection plotType = destConfig.getGraphical().getPlotType();
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                // Local type parameter
                if ( nextConfig.getPlotType() != null )
                {
                    plotType = nextConfig.getPlotType();
                }
                templateResourceName = nextConfig.getTemplateResourceName();
            }

            final Map<Object, ChartEngine> engines =
                    ChartEngineFactory.buildMultiVectorOutputChartEngine( multiVectorResults,
                                                                          DATA_FACTORY,
                                                                          plotType,
                                                                          templateResourceName,
                                                                          graphicsString );
            // Build the outputs
            for ( final Map.Entry<Object, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only supported temporal pipeline is per lead time
                Object key = nextEntry.getKey();
                if ( key instanceof TimeWindow )
                {
                    key = ( (TimeWindow) key ).getLatestLeadTimeInHours();
                }
                else if ( key instanceof Threshold )
                {
                    key = ( (Threshold) key ).toStringSafe();
                }
                File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
                Path outputImage = Paths.get( destDir.toString(),
                                              ConfigHelper.getFeatureDescription( feature )
                                                                  + "_"
                                                                  + metricId.name()
                                                                  + "_"
                                                                  + config.getInputs()
                                                                          .getRight()
                                                                          .getVariable()
                                                                          .getValue()
                                                                  + "_"
                                                                  + key
                                                                  + ".png" );
                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
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

    private static void processBoxPlotCharts( final Feature feature,
                                              final ProjectConfigPlus projectConfigPlus,
                                              final MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        // Check for results
        if ( Objects.isNull( boxPlotResults ) )
        {
            LOGGER.warn( "No box-plot outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Map.Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> e : boxPlotResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeBoxPlotCharts( feature, projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }

    /**
     * Writes a set of charts associated with {@link BoxPlotOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param feature the feature
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param boxPlotResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeBoxPlotCharts( Feature feature,
                                            ProjectConfigPlus projectConfigPlus,
                                            DestinationConfig destConfig,
                                            MetricConstants metricId,
                                            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        // Build charts
        try
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig = getNamedConfigOrAllValid( metricId, config );
            String templateResourceName = null;
            if ( Objects.nonNull( nextConfig ) )
            {
                templateResourceName = nextConfig.getTemplateResourceName();
            }

            final Map<Pair<TimeWindow, Threshold>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEngine( boxPlotResults,
                                                                templateResourceName,
                                                                graphicsString );
            // Build the outputs
            for ( final Map.Entry<Pair<TimeWindow, Threshold>, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                File destDir = ConfigHelper.getDirectoryFromDestinationConfig( destConfig );
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only temporal pipeline is by lead time
                long key = nextEntry.getKey().getLeft().getLatestLeadTimeInHours();
                Path outputImage = Paths.get( destDir.toString(),
                                              ConfigHelper.getFeatureDescription( feature )
                                                                  + "_"
                                                                  + metricId.name()
                                                                  + "_"
                                                                  + config.getInputs()
                                                                          .getRight()
                                                                          .getVariable()
                                                                          .getValue()
                                                                  + "_"
                                                                  + key
                                                                  + ".png" );
                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | ProjectConfigException e )
        {
            throw new WresProcessingException( "Error while generating box-plot charts:", e );
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
     * Task that computes a set of metric results for a particular time window.
     */
    private static class PairsByTimeWindowProcessor implements Supplier<MetricOutputForProjectByTimeAndThreshold>
    {
        /**
         * The future metric input.
         */
        private final Future<MetricInput<?>> futureInput;

        /**
         * Processor for single-valued pairs.
         */

        private final MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor;

        /**
         * Processor for ensemble pairs.
         */

        private final MetricProcessorByTime<EnsemblePairs> ensembleProcessor;

        /**
         * Construct.
         * 
         * @param futureInput the future metric input
         * @param singleValuedProcessor a processor for {@link SingleValuedPairs}
         * @param ensembleProcessor a processor for {@link EnsemblePairs}
         */

        private PairsByTimeWindowProcessor( final Future<MetricInput<?>> futureInput,
                                            MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor,
                                            MetricProcessorByTime<EnsemblePairs> ensembleProcessor )
        {
            Objects.requireNonNull( futureInput, "Specify a non-null input for the processor." );
            this.futureInput = futureInput;
            this.singleValuedProcessor = singleValuedProcessor;
            this.ensembleProcessor = ensembleProcessor;
        }

        @Override
        public MetricOutputForProjectByTimeAndThreshold get()
        {
            MetricInput<?> input = null;
            try
            {
                input = futureInput.get();
                LOGGER.debug( "Completed processing of pairs for feature '{}' and time window {}.",
                              input.getMetadata().getIdentifier().getGeospatialID(),
                              input.getMetadata().getTimeWindow() );
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
            // Process the pairs
            if ( input instanceof SingleValuedPairs )
            {
                return singleValuedProcessor.apply( (SingleValuedPairs) input );
            }
            else if ( input instanceof EnsemblePairs )
            {
                return ensembleProcessor.apply( (EnsemblePairs) input );
            }
            else
            {
                throw new WresProcessingException( "While processing pairs: encountered an unexpected type of pairs." );
            }
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
         * The processor used to determined whether the output is intermediate or being cached.
         */

        private final MetricProcessorByTime<?> processor;
        
        /**
         * Construct.
         * 
         * @param feature the feature
         * @param projectConfigPlus the project configuration
         * @param processor the metric processor
         */

        private IntermediateResultProcessor(final Feature feature, final ProjectConfigPlus projectConfigPlus, final MetricProcessorByTime<?> processor)
        {
            Objects.requireNonNull( feature, "Specify a non-null feature for the results processor." );
            Objects.requireNonNull( projectConfigPlus, "Specify a non-null configuration for the results processor." );
            Objects.requireNonNull( processor, "Specify a non-null metric processor for the results processor." );           
            this.projectConfigPlus = projectConfigPlus;
            this.feature = feature;
            this.processor = processor;
        }

        @Override
        public void accept(final MetricOutputForProjectByTimeAndThreshold input)
        {
            try
            {
                if ( configNeedsThisTypeOfOutput( projectConfigPlus.getProjectConfig(),
                                                  DestinationType.GRAPHIC ) )
                {
                    MetricOutputMetadata meta = null;

                    //Multivector output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.MULTIVECTOR )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.MULTIVECTOR ) )
                    {
                        processMultiVectorCharts( feature,
                                                  projectConfigPlus,
                                                  input.getMultiVectorOutput() );
                        meta = input.getMultiVectorOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
                    //Box-plot output available, not being cached to the end
                    if ( input.hasOutput( MetricOutputGroup.BOXPLOT )
                         && !processor.willCacheMetricOutput( MetricOutputGroup.BOXPLOT ) )
                    {
                        processBoxPlotCharts( feature,
                                              projectConfigPlus,
                                              input.getBoxPlotOutput() );
                        meta = input.getBoxPlotOutput().entrySet().iterator().next().getValue().getMetadata();
                    }
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

        WresProcessingException( String message )
        {
            super( message );
        }
        
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
     * configuration could be found. If the configuration contains a {@link MetricConfigName#ALL_VALID}, the 
     * prescribed metric identifier is ignored and the configuration is returned for 
     * {@link MetricConfigName#ALL_VALID}.
     * 
     * @param metric the metric
     * @param config the project configuration
     * @return the metric configuration or null
     */

    private static MetricConfig getNamedConfigOrAllValid( final MetricConstants metric, final ProjectConfig config )
    {
        // Deal with MetricConfigName.ALL_VALID first
        MetricConfig allValid = ConfigHelper.getMetricConfigByName( config, MetricConfigName.ALL_VALID );
        if ( allValid != null )
        {
            return allValid;
        }
        // Find the corresponding configuration
        final Optional<MetricConfig> returnMe = config.getOutputs().getMetric().stream().filter( a -> {
            try
            {
                return metric.equals( ConfigMapper.from( a.getName() ) );
            }
            catch ( final MetricConfigurationException e )
            {
                LOGGER.error( "Could not map metric name '{}' to metric configuration.", metric, e );
                return false;
            }
        } ).findFirst();
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
     * or if ANY is an InsufficientDataException
     * @param e the exception (and its chained causes) to look at
     * @return true when either NoDataException or InsufficientDataException is
     * found, false otherwise
     */
    private static boolean wasInsufficientDataOrNoDataInThisStack( Exception e )
    {
        Throwable cause = e;
        while ( cause != null )
        {
            if ( cause instanceof NoDataException
                 || cause instanceof InsufficientDataException )
            {
                return true;
            }
            cause = cause.getCause();
        }
        return false;
    }
    
    /**
     * Returns <code>true</code> if the input configuration requires any outputs of the type 
     * {@link MetricOutputGroup#MULTIVECTOR} that must be cached across time windows; that is, when the plot type 
     * configuration is {@link PlotTypeSelection#THRESHOLD_LEAD} for any or all metrics.
     * 
     * @param projectConfig the project configuration
     * @return true if the input configuration requires cached outputs of the {@link MetricOutputGroup#MULTIVECTOR} 
     *            type, false otherwise
     * @throws NullPointerException if the input is null
     * @throws MetricConfigurationException if the configuration is invalid
     */

    private static boolean hasMultiVectorOutputsToCache( ProjectConfig projectConfig )
            throws MetricConfigurationException
    {
        Objects.requireNonNull( projectConfig, "Specify non-null project configuration." );
        // Does the configuration contain any multivector types?        
        boolean hasMultiVectorType = MetricProcessor.getMetricsFromConfig( projectConfig )
                                                    .stream()
                                                    .anyMatch( a -> a.isInGroup( MetricOutputGroup.MULTIVECTOR ) );

        // Does it contain an threshold lead types?
        boolean hasThresholdLeadType = false; //Assume not

        // Does it contain any metric-local threshold lead types?
        for ( MetricConfig next : projectConfig.getOutputs().getMetric() )
        {
            if ( next.getPlotType() != null && next.getPlotType().equals( PlotTypeSelection.THRESHOLD_LEAD ) )
            {
                hasThresholdLeadType = true; //Yes
                break;
            }
        }

        // Check for metric-global threshold lead types if required
        if ( !hasThresholdLeadType )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( projectConfig );
            for ( DestinationConfig next : destinations )
            {
                if ( next.getGraphical().getPlotType().equals( PlotTypeSelection.THRESHOLD_LEAD ) )
                {
                    hasThresholdLeadType = true;
                    break;
                }
            }
        }
        return hasMultiVectorType && hasThresholdLeadType;
    }

}
