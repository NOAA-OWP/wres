package wres.control;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ohd.hseb.charter.ChartEngine;
import ohd.hseb.charter.ChartEngineException;
import ohd.hseb.charter.datasource.XYChartDataSourceException;
import wres.config.FeaturePlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.engine.statistics.metric.processing.MetricProcessorException;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.io.Operations;
import wres.io.config.ConfigHelper;
import wres.io.config.ProjectConfigPlus;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.InputGenerator;
import wres.io.retrieval.IterationFailedException;
import wres.io.utilities.NoDataException;
import wres.io.writing.ChartWriter;
import wres.io.writing.ChartWriter.ChartWritingException;
import wres.io.writing.CommaSeparatedWriter;
import wres.io.writing.NetcdfOutputWriter;
import wres.util.ProgressMonitor;
import wres.vis.ChartEngineFactory;

/**
 * Class with functions to help in generating metrics and processing metric products.
 *
 * TODO: abstract away the functions used for graphical processing to a separate helper, GraphicalProductsHelper.
 *
 * @author james.brown@hydrosolved.com
 * @author jesse.bickel@***REMOVED***
 */
class ProcessorHelper
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ProcessorHelper.class );

    /**
     * Default data factory.
     */

    private static final DataFactory DATA_FACTORY = DefaultDataFactory.getInstance();

    private ProcessorHelper()
    {
        // Helper class with static methods therefore no construction allowed.
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

    static void processProjectConfig( final ProjectConfigPlus projectConfigPlus,
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
        ProjectDetails projectDetails = Operations.ingest( projectConfig);

        LOGGER.debug( "Finished ingest for project {}...", projectConfigPlus );

        ProgressMonitor.setShowStepDescription( false );

        Set<Feature> decomposedFeatures;

        try
        {
            decomposedFeatures = Operations.decomposeFeatures( projectDetails );
        }
        catch ( SQLException e )
        {
            IOException ioe = new IOException( "Failed to retrieve the set of features.", e );
            ProcessorHelper.addException( ioe );
            throw ioe;
        }

        List<Feature> successfulFeatures = new ArrayList<>();
        List<Feature> missingDataFeatures = new ArrayList<>();
        
        // Read any threshold source in the configuration
        Map<FeaturePlus,Set<Threshold>> thresholds = ConfigHelper.readThresholdsFromProjectConfig( projectConfig );

        // Get the consumers (output writers) of metric outputs
        List<Consumer<MetricOutputMapByTimeAndThreshold<?>>> writers =
                ProcessorHelper.getWriters( projectConfig );

        // Reduce our triad of executors to one object
        ExecutorServices executors = new ExecutorServices( pairExecutor,
                                                           thresholdExecutor,
                                                           metricExecutor );

        int currentFeature = 0;

        for ( Feature feature : decomposedFeatures )
        {
            ProgressMonitor.resetMonitor();

            if ( LOGGER.isInfoEnabled() )
            {
                currentFeature++;
                LOGGER.info( "[{}/{}] Processing feature '{}'",
                             currentFeature,
                             decomposedFeatures.size(),
                             ConfigHelper.getFeatureDescription( feature ) );
            }

            FeatureProcessingResult result =
                    processFeature( feature,
                                    thresholds.get( FeaturePlus.of( feature ) ),
                                    projectConfigPlus,
                                    projectDetails,
                                    executors,
                                    writers );

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

    private static void printFeaturesReport( final ProjectConfigPlus projectConfigPlus,
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
     * @param thresholds an optional set of (canonical) thresholds for which
     *                   results are required, may be null
     * @param projectConfigPlus the project configuration
     * @param projectDetails the project details to use
     * @param executors the executors for pairs, thresholds, and metrics
     * @param writers the writers of output
     * @throws WresProcessingException when an error occurs during processing
     * @return a feature result
     */

    static FeatureProcessingResult processFeature( final Feature feature,
                                                   final Set<Threshold> thresholds,
                                                   final ProjectConfigPlus projectConfigPlus,
                                                   final ProjectDetails projectDetails,
                                                   final ExecutorServices executors,
                                                   final List<Consumer<MetricOutputMapByTimeAndThreshold<?>>> writers )
    {

        final ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        final String featureDescription = ConfigHelper.getFeatureDescription( feature );
        final String errorMessage = "While processing feature "+ featureDescription;

        // Sink for the results: the results are added incrementally to an immutable store via a builder
        // Some output types are processed at the end of the pipeline, others after each input is processed
        MetricProcessorForProject processor = null;
        try
        {
            processor = MetricFactory.getInstance( DATA_FACTORY )
                                     .getMetricProcessorForProject( projectConfig,
                                                                    thresholds,
                                                                    executors.getThresholdExecutor(),
                                                                    executors.getMetricExecutor() );
        }
        catch(final MetricProcessorException e )
        {
            throw new WresProcessingException( errorMessage, e );
        }

        // Build an InputGenerator for the next feature
        InputGenerator metricInputs = Operations.getInputs( projectDetails,
                                                            feature );

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
                                                                                       processor ),
                                                       executors.getPairExecutor() )
                                         .thenAcceptAsync( new IntermediateResultProcessor( feature,
                                                                                            projectConfigPlus,
                                                                                            processor.getCachedMetricOutputTypes(),
                                                                                            writers ),
                                                           executors.getPairExecutor() )
                                         .thenAccept(
                                                      aVoid -> ProgressMonitor.completeStep() );

                // Add the future to the list
                listOfFutures.add( c );
            }
        }
        catch ( IterationFailedException re )
        {
            if ( ProcessorHelper.wasInsufficientDataOrNoDataInThisStack( re ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    re );
            }
            else
            {
                ProcessorHelper.addException( re );
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
            if ( ProcessorHelper.wasInsufficientDataOrNoDataInThisStack( e ) )
            {
                return new FeatureProcessingResult( feature,
                                                    false,
                                                    e );
            }

            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( errorMessage, e );
        }

        // Generate cached output if available
        if ( processor.hasCachedMetricOutput() )
        {
            try
            {
                processCachedProducts( projectConfigPlus,
                                       processor.getCachedMetricOutput(),
                                       feature,
                                       writers );
            }
            catch ( MetricOutputAccessException e )
            {
                throw new WresProcessingException( errorMessage, e );
            }
        }

        return new FeatureProcessingResult( feature, true, null );
    }


    /**
     * Completes the processing of products, including graphical and numerical products, at the end of a processing 
     * pipeline using the cached {@link MetricOutput} stored in the {@link MetricProcessor}, and in keeping with 
     * the supplied {@link ProjectConfig}.
     * 
     * @param projectConfigPlus the project configuration
     * @param cachedOutput the cached output
     * @param feature the feature being processed
     * @param writers the writers of outputs
     */

    private static void processCachedProducts( ProjectConfigPlus projectConfigPlus,
                                               MetricOutputForProjectByTimeAndThreshold cachedOutput,
                                               Feature feature,
                                               List<Consumer<MetricOutputMapByTimeAndThreshold<?>>> writers )
    {
        if( Objects.isNull( cachedOutput ) )
        {
            LOGGER.warn( "No cached outputs to process. ");
            return;
        }
        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();
        final String featureDescription = ConfigHelper.getFeatureDescription( feature );

        //Generate graphical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.GRAPHIC ) )
        {
            LOGGER.debug( "Beginning to build charts for feature {}...",
                          featureDescription );

            processCachedCharts( projectConfigPlus,
                                 cachedOutput );

            LOGGER.debug( "Finished building charts for feature {}.",
                          featureDescription );
        }

        //Generate numerical output
        if ( configNeedsThisTypeOfOutput( projectConfig,
                                          DestinationType.NUMERIC ) )
        {
            LOGGER.debug( "Beginning to write numeric output for feature {}...",
                          featureDescription );

            try
            {
                CommaSeparatedWriter.writeOutputFiles( projectConfig,
                                                 feature,
                                                 cachedOutput );

            }
            catch ( IOException e )
            {
                throw new WresProcessingException( "While writing output files: ",
                                                   e );
            }

            LOGGER.debug( "Finished writing numeric output for feature {}.",
                          featureDescription );
        }

        LOGGER.info( "Completed processing of feature '{}'.", featureDescription );
    }


    /**
     * Processes all charts for which metric outputs were cached across successive calls to a {@link MetricProcessor}.
     *
     * @param projectConfigPlus the project configuration
     * @param cachedOutput the cached output
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void processCachedCharts( final ProjectConfigPlus projectConfigPlus,
                                             final MetricOutputForProjectByTimeAndThreshold cachedOutput )
    {
        try
        {
            // Process charts for ordinary scores
            if ( cachedOutput.hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
            {
                processChartsForDoubleScoreOutput( projectConfigPlus,
                                                   cachedOutput.getDoubleScoreOutput() );
            }
            // Process charts for scores that comprise durations
            if ( cachedOutput.hasOutput( MetricOutputGroup.DURATION_SCORE ) )
            {
                processChartsForDurationScoreOutput( projectConfigPlus,
                                                     cachedOutput.getDurationScoreOutput() );
            }
            // Process multivector charts
            if ( cachedOutput.hasOutput( MetricOutputGroup.MULTIVECTOR ) )
            {
                processMultiVectorCharts( projectConfigPlus,
                                          cachedOutput.getMultiVectorOutput() );
            }
            // Process box plot charts
            if ( cachedOutput.hasOutput( MetricOutputGroup.BOXPLOT ) )
            {
                processBoxPlotCharts( projectConfigPlus,
                                      cachedOutput.getBoxPlotOutput() );
            }
            // Process paired output
            if ( cachedOutput.hasOutput( MetricOutputGroup.PAIRED ) )
            {
                processPairedOutputByInstantDurationCharts( projectConfigPlus,
                                                            cachedOutput.getPairedOutput() );
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
     * Processes a set of charts associated with {@link DoubleScoreOutput} across multiple metrics, time windows, 
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param scoreResults the metric results
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void processChartsForDoubleScoreOutput( final ProjectConfigPlus projectConfigPlus,
                                                           final MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> scoreResults )
    {
        // Check for results
        if ( Objects.isNull( scoreResults ) )
        {
            LOGGER.warn( "No vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric 
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> e : scoreResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );

            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                Supplier<Map<MetricConstants, ChartEngine>> supplier =
                        getChartSupplierForDoubleScoreOutput( projectConfigPlus,
                                                              destConfig,
                                                              e.getKey().getKey(),
                                                              e.getValue() );
                writeScoreCharts( destConfig,
                                  supplier,
                                  e.getValue().getMetadata() );
            }
        }
    }


    /**
     * Processes a set of charts associated with {@link DurationScoreOutput} across multiple metrics, time windows,
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     * 
     * @param projectConfigPlus the project configuration
     * @param scoreResults the metric results
     * @throws WresProcessingException when an error occurs during processing
     */

    private static void processChartsForDurationScoreOutput( final ProjectConfigPlus projectConfigPlus,
                                                             final MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> scoreResults )
    {
        // Check for results
        if ( Objects.isNull( scoreResults ) )
        {
            LOGGER.warn( "No vector outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();

        // Iterate through each metric
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<DurationScoreOutput>> e : scoreResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );

            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                Supplier<Map<MetricConstants, ChartEngine>> supplier =
                        getChartSupplierForDurationScoreOutput( projectConfigPlus,
                                                                destConfig,
                                                                e.getKey().getKey(),
                                                                e.getValue() );
                writeScoreCharts( destConfig,
                                  supplier,
                                  e.getValue().getMetadata() );
            }
        }
    }


    /**
     * Returns a chart engine supplier from the input.
     *
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param scoreResults the metric results
     * @return a chart engine supplier
     */

    private static Supplier<Map<MetricConstants, ChartEngine>>
            getChartSupplierForDoubleScoreOutput( ProjectConfigPlus projectConfigPlus,
                                                  DestinationConfig destConfig,
                                                  MetricConstants metricId,
                                                  MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> scoreResults )
    {
        return () -> {
            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destConfig, metricId );
            try
            {
                return ChartEngineFactory.buildScoreOutputChartEngine( projectConfigPlus.getProjectConfig(), 
                                                                       scoreResults,
                                                                       DATA_FACTORY,
                                                                       helper.getOutputType(),
                                                                       helper.getTemplateResourceName(),
                                                                       helper.getGraphicsString() );
            }
            catch ( ChartEngineException e )
            {
                throw new WresProcessingException( "Error while generating score charts:", e );
            }
        };
    }


    /**
     * Returns a chart engine supplier from the input.
     *
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param scoreResults the metric results
     * @return a chart engine supplier
     */

    private static Supplier<Map<MetricConstants, ChartEngine>>
            getChartSupplierForDurationScoreOutput( ProjectConfigPlus projectConfigPlus,
                                                    DestinationConfig destConfig,
                                                    MetricConstants metricId,
                                                    MetricOutputMapByTimeAndThreshold<DurationScoreOutput> scoreResults )
    {
        return () ->
        {
            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destConfig, metricId );
            try
            {
                Map<MetricConstants, ChartEngine> returnMe = new EnumMap<>( MetricConstants.class );
                ChartEngine engine = ChartEngineFactory.buildCategoricalDurationScoreChartEngine( projectConfigPlus.getProjectConfig(),
                                                                                                  scoreResults,
                                                                                                  helper.getTemplateResourceName(),
                                                                                                  helper.getGraphicsString() );
                returnMe.put( MetricConstants.MAIN, engine );
                return returnMe;
            }
            catch ( ChartEngineException | XYChartDataSourceException e )
            {
                throw new WresProcessingException( "Error while generating score charts:", e );
            }
        };
    }


    /**
     * Writes a set of charts associated with {@link DoubleScoreOutput} for a single metric and time window,
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param destConfig the destination configuration for the written output
     * @param chartSupplier a supplier of chart engines
     * @param meta the metadata associated with the score results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeScoreCharts( DestinationConfig destConfig,
                                          Supplier<Map<MetricConstants, ChartEngine>> chartSupplier,
                                          MetricOutputMetadata meta )
    {
        // Build charts
        try
        {
            Map<MetricConstants, ChartEngine> engines = chartSupplier.get();
            // Build the outputs
            for ( final Entry<MetricConstants, ChartEngine> nextEntry : engines.entrySet() )
            {

                // Build the output file name
                Path outputImage = ConfigHelper.getOutputPathToWrite( destConfig, meta );

                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartWritingException | IOException e )
        {
            throw new WresProcessingException( "Error while generating vector charts:", e );
        }
    }      


    /**
     * Processes a set of charts associated with {@link MultiVectorOutput} across multiple metrics, time windows,
     * and thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param multiVectorResults the metric results
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    static void processMultiVectorCharts( final ProjectConfigPlus projectConfigPlus,
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
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<MultiVectorOutput>> e : multiVectorResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeMultiVectorCharts( projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }

    
    /**
     * Writes a set of charts associated with {@link MultiVectorOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param multiVectorResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeMultiVectorCharts( ProjectConfigPlus projectConfigPlus,
                                                DestinationConfig destConfig,
                                                MetricConstants metricId,
                                                MetricOutputMapByTimeAndThreshold<MultiVectorOutput> multiVectorResults )
    {
        // Build charts
        try
        {
            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destConfig, metricId );

            final Map<Object, ChartEngine> engines =
                    ChartEngineFactory.buildMultiVectorOutputChartEngine( projectConfigPlus.getProjectConfig(),
                                                                          multiVectorResults,
                                                                          DATA_FACTORY,
                                                                          helper.getOutputType(),
                                                                          helper.getTemplateResourceName(),
                                                                          helper.getGraphicsString() );

            // Build the outputs
            for ( final Entry<Object, ChartEngine> nextEntry : engines.entrySet() )
            {
                // Build the output file name
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only supported temporal pipeline is per lead time
                Path outputImage = null;
                Object append = nextEntry.getKey();
                if ( append instanceof TimeWindow )
                {
                    outputImage = ConfigHelper.getOutputPathToWrite( destConfig,
                                                                     multiVectorResults.getMetadata(),
                                                                     (TimeWindow) append );
                }
                else if ( append instanceof Threshold )
                {
                    outputImage = ConfigHelper.getOutputPathToWrite( destConfig,
                                                                     multiVectorResults.getMetadata(),
                                                                     (Threshold) append );
                }

                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | IOException e )
        {
            throw new WresProcessingException( "Error while generating multi-vector charts:", e );
        }
    }

    
    /**
     * Processes a set of charts associated with {@link BoxPlotOutput} across multiple metrics, time window, and 
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param boxPlotResults the box plot outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    static void processBoxPlotCharts( final ProjectConfigPlus projectConfigPlus,
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
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<BoxPlotOutput>> e : boxPlotResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writeBoxPlotCharts( projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }


    /**
     * Writes a set of charts associated with {@link BoxPlotOutput} for a single metric and time window, 
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param boxPlotResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writeBoxPlotCharts( ProjectConfigPlus projectConfigPlus,
                                            DestinationConfig destConfig,
                                            MetricConstants metricId,
                                            MetricOutputMapByTimeAndThreshold<BoxPlotOutput> boxPlotResults )
    {
        // Build charts
        try
        {
            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destConfig, metricId );

            final Map<Pair<TimeWindow, Threshold>, ChartEngine> engines =
                    ChartEngineFactory.buildBoxPlotChartEngine( projectConfigPlus.getProjectConfig(),
                                                                boxPlotResults,
                                                                helper.getTemplateResourceName(),
                                                                helper.getGraphicsString() );

            // Build the outputs
            for ( final Entry<Pair<TimeWindow, Threshold>, ChartEngine> nextEntry : engines.entrySet() )
            {
                // TODO: adopt a more general naming convention as the pipelines expand
                // For now, the only temporal pipeline is by lead time
                Path outputImage = ConfigHelper.getOutputPathToWrite( destConfig,
                                                                      boxPlotResults.getMetadata(),
                                                                      nextEntry.getKey().getLeft() );

                ChartWriter.writeChart( outputImage, nextEntry.getValue(), destConfig );
            }
        }
        catch ( ChartEngineException
                | ChartWritingException
                | IOException e )
        {
            throw new WresProcessingException( "Error while generating box-plot charts:", e );
        }
    }


    /**
     * Processes a set of charts associated with {@link PairedOutput} across multiple metrics, time window, and
     * thresholds, stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param pairedOutputResults the outputs
     * @throws WresProcessingException if the processing completed unsuccessfully
     */

    static void processPairedOutputByInstantDurationCharts( final ProjectConfigPlus projectConfigPlus,
                                                            final MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> pairedOutputResults )
    {
        // Check for results
        if ( Objects.isNull( pairedOutputResults ) )
        {
            LOGGER.warn( "No box-plot outputs from which to generate charts." );
            return;
        }

        ProjectConfig config = projectConfigPlus.getProjectConfig();
        // Iterate through each metric
        for ( final Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>>> e : pairedOutputResults.entrySet() )
        {
            List<DestinationConfig> destinations =
                    ConfigHelper.getGraphicalDestinations( config );
            // Iterate through each destination
            for ( DestinationConfig destConfig : destinations )
            {
                writePairedOutputByInstantDurationCharts( projectConfigPlus, destConfig, e.getKey().getKey(), e.getValue() );
            }
        }
    }


    /**
     * Writes a set of charts associated with {@link PairedOutput} for a single metric and time window,
     * stored in a {@link MetricOutputMultiMapByTimeAndThreshold}.
     *
     * @param projectConfigPlus the project configuration
     * @param destConfig the destination configuration for the written output
     * @param metricId the metric identifier
     * @param pairedOutputResults the metric results
     * @throws WresProcessingException when an error occurs during writing
     */

    private static void writePairedOutputByInstantDurationCharts( ProjectConfigPlus projectConfigPlus,
                                                                  DestinationConfig destConfig,
                                                                  MetricConstants metricId,
                                                                  MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> pairedOutputResults )
    {
        // Build charts
        try
        {
            GraphicsHelper helper = GraphicsHelper.of( projectConfigPlus, destConfig, metricId );

            final ChartEngine engine = ChartEngineFactory.buildPairedInstantDurationChartEngine( projectConfigPlus.getProjectConfig(),
                                                                                                 pairedOutputResults,
                                                                                                 helper.getTemplateResourceName(),
                                                                                                 helper.getGraphicsString() );

            // Build the output file name
            Path outputImage = ConfigHelper.getOutputPathToWrite( destConfig,
                                                                  pairedOutputResults.getMetadata() );

            ChartWriter.writeChart( outputImage, engine, destConfig );
        }
        catch ( ChartEngineException
                | ChartWritingException
                | IOException e )
        {
            throw new WresProcessingException( "Error while generating box-plot charts:", e );
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
     * Returns true if the given config has one or more of given output type.
     * @param config the config to search
     * @param type the type of output to look for
     * @return true if the output type is present, false otherwise
     */

    static boolean configNeedsThisTypeOfOutput( ProjectConfig config,
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
     * A helper class that builds the parameters required for graphics generation.
     */

    private static class GraphicsHelper
    {

        /**
         * The template resource name.
         */

        private final String templateResourceName;

        /**
         * The graphics string.
         */

        private final String graphicsString;

        /**
         * The output type.
         */

        private final OutputTypeSelection outputType;

        /**
         * Returns a graphics helper.
         *
         * @param projectConfigPlus the project configuration
         * @param destConfig the destination configuration
         * @param metricId the metric identifier
         */

        private static GraphicsHelper of(  ProjectConfigPlus projectConfigPlus,
                                    DestinationConfig destConfig,
                                    MetricConstants metricId )
        {
            return new GraphicsHelper( projectConfigPlus, destConfig, metricId );
        }

        /**
         * Builds a helper.
         *
         * @param projectConfigPlus the project configuration
         * @param destConfig the destination configuration
         * @param metricId the metric identifier
         */

        private GraphicsHelper( ProjectConfigPlus projectConfigPlus,
                                DestinationConfig destConfig,
                                MetricConstants metricId )
        {
            ProjectConfig config = projectConfigPlus.getProjectConfig();
            String graphicsString = projectConfigPlus.getGraphicsStrings().get( destConfig );
            // Build the chart engine
            MetricConfig nextConfig =
                    getNamedConfigOrAllValid( metricId, config );
            // Default to global type parameter
            OutputTypeSelection outputType = OutputTypeSelection.DEFAULT;
            if( Objects.nonNull( destConfig.getOutputType() ) )
            {
                outputType = destConfig.getOutputType();
            }
            String templateResourceName = destConfig.getGraphical().getTemplate();
            if ( Objects.nonNull( nextConfig ) )
            {
                // Local type parameter
                if ( Objects.nonNull( nextConfig.getOutputType() ) )
                {
                    outputType = nextConfig.getOutputType();
                }

                // Override template name with metric specific name.
                if ( Objects.nonNull( nextConfig.getTemplateResourceName() ) )
                {
                    templateResourceName = nextConfig.getTemplateResourceName();
                }
            }
            this.templateResourceName = templateResourceName;
            this.outputType = outputType;
            this.graphicsString = graphicsString;
        }

        /**
         * Returns the output type.
         * @return the output type
         */

        private OutputTypeSelection getOutputType()
        {
            return outputType;
        }

        /**
         * Returns the graphics string.
         * @return the graphics string
         */

        private String getGraphicsString()
        {
            return graphicsString;
        }

        /**
         * Returns the template resource name.
         * @return the template resource name
         */

        private String getTemplateResourceName()
        {
            return templateResourceName;
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
            final Optional<MetricConfig> returnMe = config.getMetrics()
                                                          .getMetric()
                                                          .stream()
                                                          .filter( a -> metric.name().equals( a.getName().name() ) )
                                                          .findFirst();
            return returnMe.isPresent() ? returnMe.get() : null;
        }

    }

    private static List<Exception> exceptionList;
    private static final Object EXCEPTION_LOCK = new Object();

    private static void addException(Exception exception)
    {
        synchronized ( EXCEPTION_LOCK )
        {
            if (ProcessorHelper.exceptionList == null)
            {
                ProcessorHelper.exceptionList = new ArrayList<>(  );
            }

            ProcessorHelper.exceptionList.add( exception );
        }
    }

    public static List<Exception> getEncounteredExceptions()
    {
        synchronized(EXCEPTION_LOCK)
        {
            if (ProcessorHelper.exceptionList == null)
            {
                ProcessorHelper.exceptionList = new ArrayList<>(  );
            }

            return Collections.unmodifiableList(ProcessorHelper.exceptionList);
        }
    }

    private static List<Consumer<MetricOutputMapByTimeAndThreshold<?>>>
    getWriters( ProjectConfig projectConfig )
    {
        List<Consumer<MetricOutputMapByTimeAndThreshold<?>>> result =
                new ArrayList<>( 1 );

        // Make netcdf output if needed
        NetcdfOutputWriter outputWriter = new NetcdfOutputWriter( projectConfig );

        result.add( outputWriter );

        return Collections.unmodifiableList( result );
    }

    private static class ExecutorServices
    {
        private final ExecutorService pairExecutor;
        private final ExecutorService thresholdExecutor;
        private final ExecutorService metricExecutor;

        ExecutorServices( ExecutorService pairExecutor,
                          ExecutorService thresholdExecutor,
                          ExecutorService metricExecutor )
        {
            this.pairExecutor = pairExecutor;
            this.thresholdExecutor = thresholdExecutor;
            this.metricExecutor = metricExecutor;
        }

        ExecutorService getPairExecutor()
        {
            return this.pairExecutor;
        }

        ExecutorService getThresholdExecutor()
        {
            return this.thresholdExecutor;
        }

        ExecutorService getMetricExecutor()
        {
            return this.metricExecutor;
        }
    }
}
