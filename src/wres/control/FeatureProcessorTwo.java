package wres.control;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.FeaturePlus;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.control.ProcessorHelper.ExecutorServices;
import wres.control.ProcessorHelper.SharedWriters;
import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.io.config.ConfigHelper;
import wres.io.project.Project;
import wres.io.retrieval.datashop.SingleValuedPoolGenerator;
import wres.io.retrieval.datashop.UnitMapper;
import wres.util.IterationFailedException;
import wres.io.writing.commaseparated.pairs.PairsWriter;

/**
 * Encapsulates a task (with subtasks) for processing all verification results associated with one {@link FeaturePlus}.
 * 
 * @author james.brown@hydrosolved.com
 * @param the type of left data in the paired data
 * @param the type of right data in the paired data
 */

class FeatureProcessorTwo implements Supplier<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureProcessorTwo.class );

    /**
     * The feature.
     */

    private final FeaturePlus feature;

    /**
     * The project.
     */

    private final Project project;

    /**
     * The resolved project.
     */

    private final ResolvedProject resolvedProject;

    /**
     * The executors services.
     */

    private final ExecutorServices executors;

    /**
     * The shared writers.
     */

    private final SharedWriters sharedWriters;

    /**
     * Error message.
     */

    private final String errorMessage;

    /**
     * Unit mapper.
     */

    private final UnitMapper unitMapper;

    /**
     * Build a processor. 
     * 
     * @param feature the feature to process
     * @param resolvedProject the resolved project
     * @param project the project to use
     * @param unitMapper the unit mapper
     * @param executors the executors for pairs, thresholds, and metrics
     * @param sharedWriters shared writers
     * @throws NullPointerException if any required input is null
     */

    FeatureProcessorTwo( FeaturePlus feature,
                         ResolvedProject resolvedProject,
                         Project project,
                         UnitMapper unitMapper,
                         ExecutorServices executors,
                         SharedWriters sharedWriters )
    {

        Objects.requireNonNull( feature );
        Objects.requireNonNull( resolvedProject );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( sharedWriters );
        Objects.requireNonNull( unitMapper );

        this.feature = feature;
        this.resolvedProject = resolvedProject;
        this.project = project;
        this.executors = executors;
        this.sharedWriters = sharedWriters;
        this.unitMapper = unitMapper;

        // Error message
        String featureDescription = ConfigHelper.getFeatureDescription( this.feature );
        errorMessage = "While processing feature " + featureDescription;
    }

    @Override
    public FeatureProcessingResult get()
    {
        // Report
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Started feature '{}'",
                          ConfigHelper.getFeatureDescription( this.feature.getFeature() ) );
        }

        final ProjectConfig projectConfig = this.resolvedProject.getProjectConfig();
        final ThresholdsByMetric thresholds =
                this.resolvedProject.getThresholdForFeature( this.feature );

        // TODO: do NOT rely on the declared type. Instead, determine it, post-ingest,
        // from the ResolvedProject. See #57301.
        DatasourceType type = projectConfig.getInputs()
                                           .getRight()
                                           .getType();

        try
        {
            // In future, other types of pools may be handled here
            // Pairs that contain ensemble forecasts
            if ( type == DatasourceType.ENSEMBLE_FORECASTS )
            {
//                EnsemblePoolGenerator poolGenerator =
//                        EnsemblePoolGenerator.of( this.project, this.feature.getFeature(), this.unitMapper );
//                List<Supplier<PoolOfPairs<Double, Ensemble>>> pools = poolGenerator.get();
                List<Supplier<PoolOfPairs<Double, Ensemble>>> pools = null;

                // Stand-up the pair writers
                PairsWriter<Double, Ensemble> pairsWriter = null;
                PairsWriter<Double, Ensemble> basePairsWriter = null;
                if ( this.sharedWriters.hasSharedSampleWriters() )
                {
                    pairsWriter = this.sharedWriters.getSampleDataWriters().getEnsembleWriter();
                }
                if ( this.sharedWriters.hasSharedBaselineSampleWriters() )
                {
                    basePairsWriter = this.sharedWriters.getBaselineSampleDataWriters().getEnsembleWriter();
                }

                MetricProcessor<PoolOfPairs<Double, Ensemble>> processor =
                        MetricFactory.ofMetricProcessorForEnsemblePairs( projectConfig,
                                                                         thresholds,
                                                                         this.executors.getThresholdExecutor(),
                                                                         this.executors.getMetricExecutor() );

                return this.processFeature( projectConfig,
                                            processor,
                                            pools,
                                            pairsWriter,
                                            basePairsWriter );
            }
            // All other types
            else
            {
                SingleValuedPoolGenerator poolGenerator =
                        SingleValuedPoolGenerator.of( this.project, this.feature.getFeature(), this.unitMapper );
                List<Supplier<PoolOfPairs<Double, Double>>> pools = poolGenerator.get();

                // Stand-up the pair writers
                PairsWriter<Double, Double> pairsWriter = null;
                PairsWriter<Double, Double> basePairsWriter = null;
                if ( this.sharedWriters.hasSharedSampleWriters() )
                {
                    pairsWriter = this.sharedWriters.getSampleDataWriters().getSingleValuedWriter();
                }
                if ( this.sharedWriters.hasSharedBaselineSampleWriters() )
                {
                    basePairsWriter = this.sharedWriters.getBaselineSampleDataWriters().getSingleValuedWriter();
                }

                MetricProcessor<PoolOfPairs<Double, Double>> processor =
                        MetricFactory.ofMetricProcessorForSingleValuedPairs( projectConfig,
                                                                             thresholds,
                                                                             this.executors.getThresholdExecutor(),
                                                                             this.executors.getMetricExecutor() );

                return this.processFeature( projectConfig,
                                            processor,
                                            pools,
                                            pairsWriter,
                                            basePairsWriter );
            }
        }
        catch ( final MetricParameterException e )
        {
            throw new WresProcessingException( this.errorMessage, e );
        }
    }

    /**
     * Processes a feature.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param projectConfig the project declaration
     * @param processor the metric processor
     * @param pools the data pools
     * @return a processing result
     */

    private <L, R> FeatureProcessingResult processFeature( ProjectConfig projectConfig,
                                                           MetricProcessor<PoolOfPairs<L, R>> processor,
                                                           List<Supplier<PoolOfPairs<L, R>>> pools,
                                                           PairsWriter<L, R> sampleWriter,
                                                           PairsWriter<L, R> baselineSampleWriter )
    {
        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        final List<CompletableFuture<Set<Path>>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // During the pipeline, only write types that are not end-of-pipeline types unless they refer to
        // a format that can be written incrementally
        BiPredicate<StatisticGroup, DestinationType> onlyWriteTheseTypes =
                ( type, format ) -> !processor.getMetricOutputTypesToCache().contains( type )
                                    || ConfigHelper.getIncrementalFormats( projectConfig ).contains( format );

        try
        {
            // Iterate
            for ( Supplier<PoolOfPairs<L, R>> nextInput : pools )
            {
                // Complete all statistics tasks asynchronously:
                // 1. Get some sample data from the database
                // 2. Compute statistics from the sample data
                // 3. Produce outputs from the statistics 
                final CompletableFuture<Set<Path>> statisticsTasks =
                        CompletableFuture.supplyAsync( nextInput, this.executors.getPairExecutor() )
                                         .thenApplyAsync( this.getStatisticsProcessingTask( processor ),
                                                          this.executors.getPairExecutor() )
                                         .thenApplyAsync( metricOutputs -> {
                                             ProduceOutputsFromStatistics outputProcessor =
                                                     ProduceOutputsFromStatistics.of( this.resolvedProject,
                                                                                      onlyWriteTheseTypes,
                                                                                      this.sharedWriters.getStatisticsWriters() );
                                             outputProcessor.accept( metricOutputs );
                                             outputProcessor.close();
                                             return outputProcessor.get();
                                         },
                                                          this.executors.getProductExecutor() );

                // Add the task to the list
                listOfFutures.add( statisticsTasks );

                // Create a task for serializing the sample data
                if ( Objects.nonNull( this.sharedWriters.getSampleDataWriters() ) )
                {
                    CompletableFuture<Set<Path>> sampleDataTask =
                            this.getPairWritingTask( nextInput, false, sampleWriter );

                    listOfFutures.add( sampleDataTask );
                }

                // Create a task for serializing the baseline sample data
                if ( Objects.nonNull( projectConfig.getInputs().getBaseline() )
                     && Objects.nonNull( this.sharedWriters.getBaselineSampleDataWriters() ) )
                {
                    CompletableFuture<Set<Path>> baselineSampleDataTask =
                            this.getPairWritingTask( nextInput,
                                                     true,
                                                     baselineSampleWriter );

                    listOfFutures.add( baselineSampleDataTask );
                }
            }
        }
        catch ( IterationFailedException re )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( "Iteration failed", re );
        }

        // Complete all tasks or one exceptionally
        try
        {
            // Wait for completion of all data slices
            ProcessorHelper.doAllOrException( listOfFutures ).join();
        }
        catch ( CompletionException e )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( this.errorMessage, e );
        }

        // Generate cached output if available
        Set<Path> endOfPipelinePaths = this.generateEndOfPipelineProducts( processor );

        Set<Path> paths = new HashSet<>( endOfPipelinePaths );

        // Unearth the Set<Path> inside listOfFutures now that join() was called
        // above.
        for ( CompletableFuture<Set<Path>> completedFuture : listOfFutures )
        {
            Set<Path> innerPaths = completedFuture.getNow( Collections.emptySet() );
            paths.addAll( innerPaths );
        }

        Set<Path> allPaths = Collections.unmodifiableSet( paths );

        return new FeatureProcessingResult( this.feature.getFeature(),
                                            allPaths );
    }

    /**
     * Returns a task that writes pairs.
     * 
     * @param <L> the left type of data
     * @param <R> the right type of data
     * @param pairSupplier the supplier of paired data to write
     * @param useBaseline is true to write the baseline pairs
     * @param sharedWriters the consumers of paired data for writing
     * @return a task that writes pairs
     */

    private <L, R> CompletableFuture<Set<Path>> getPairWritingTask( Supplier<PoolOfPairs<L, R>> pairSupplier,
                                                                    boolean useBaseline,
                                                                    PairsWriter<L, R> sharedWriters )
    {
        return CompletableFuture.supplyAsync( pairSupplier, this.executors.getProductExecutor() )
                                .thenApplyAsync( sampleData -> {

                                    // Baseline data?
                                    if ( useBaseline )
                                    {
                                        sharedWriters.accept( sampleData.getBaselineData() );
                                    }
                                    else
                                    {
                                        sharedWriters.accept( sampleData );
                                    }

                                    return Set.of( sharedWriters.get() );
                                },
                                                 this.executors.getProductExecutor() );
    }

    /**
     * Returns a function that consumes a {@link PoolOfPairs} and produces {@link StatisticsForProject}.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param processor the metric processor
     * @return a function that consumes a pool and produces statistics
     */

    private <L, R> Function<PoolOfPairs<L, R>, StatisticsForProject>
            getStatisticsProcessingTask( MetricProcessor<PoolOfPairs<L, R>> processor )
    {
        return pool -> {
            Objects.requireNonNull( pool );

            // No data in the composition
            if ( pool.getRawData().isEmpty()
                 && ( !pool.hasBaseline() || pool.getBaselineData().getRawData().isEmpty() ) )
            {
                LOGGER.debug( "Empty pool discovered for {}: no statistics will be produced.", pool.getMetadata() );

                StatisticsForProjectBuilder builder = new StatisticsForProjectBuilder();

                // Empty container
                return builder.build();
            }

            return processor.apply( pool );
        };
    }

    /**
     * Generates products at the end of the processing pipeline.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param processor the processor from which to obtain the inputs for product generation
     * @return a set of paths written
     */

    private <L, R> Set<Path> generateEndOfPipelineProducts( MetricProcessor<PoolOfPairs<L, R>> processor )
    {
        if ( processor.hasCachedMetricOutput() )
        {
            try
            {
                // Determine the cached types
                Set<StatisticGroup> cachedTypes = processor.getCachedMetricOutputTypes();

                // Only process cached types that were not written incrementally
                BiPredicate<StatisticGroup, DestinationType> nowWriteTheseTypes =
                        ( type, format ) -> cachedTypes.contains( type )
                                            && !ConfigHelper.getIncrementalFormats( this.resolvedProject.getProjectConfig() )
                                                            .contains( format );
                try ( // End of pipeline processor
                      ProduceOutputsFromStatistics endOfPipeline =
                              ProduceOutputsFromStatistics.of( this.resolvedProject,
                                                               nowWriteTheseTypes,
                                                               this.sharedWriters.getStatisticsWriters() ) )
                {
                    // Generate output
                    endOfPipeline.accept( processor.getCachedMetricOutput() );
                    return endOfPipeline.get();
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new WresProcessingException( this.errorMessage, e );
            }
        }

        return Collections.emptySet();
    }

}
