package wres.control;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.control.ProcessorHelper.Executors;
import wres.control.ProcessorHelper.SharedWriters;
import wres.datamodel.Ensemble;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.Builder;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.events.Evaluation;
import wres.io.pooling.PoolFactory;
import wres.io.project.Project;
import wres.io.retrieval.UnitMapper;
import wres.util.IterationFailedException;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.statistics.generated.Statistics;

/**
 * Encapsulates a task (with subtasks) for processing all verification results associated with one {@link FeatureTuple}.
 * 
 * @author james.brown@hydrosolved.com
 */

class FeatureProcessor implements Supplier<FeatureProcessingResult>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( FeatureProcessor.class );

    /**
     * The feature.
     */

    private final FeatureTuple feature;

    /**
     * The project.
     */

    private final Project project;

    /**
     * The unit mapper.
     */

    private final UnitMapper unitMapper;

    /**
     * The resolved project.
     */

    private final ResolvedProject resolvedProject;

    /**
     * The evaluation description.
     */

    private final Evaluation evaluation;

    /**
     * The executors services.
     */

    private final Executors executors;

    /**
     * The shared writers.
     */

    private final SharedWriters sharedWriters;

    /**
     * Error message.
     */

    private final String errorMessage;

    /**
     * Build a processor. 
     * 
     * @param evaluation a description of the evaluation
     * @param feature the feature to process
     * @param resolvedProject the resolved project
     * @param project the project to use
     * @param unitMapper the unit mapper
     * @param executors the executors for pairs, thresholds, and metrics
     * @param sharedWriters shared writers
     * @throws NullPointerException if any required input is null
     */

    FeatureProcessor( Evaluation evaluation,
                      FeatureTuple feature,
                      ResolvedProject resolvedProject,
                      Project project,
                      UnitMapper unitMapper,
                      Executors executors,
                      SharedWriters sharedWriters )
    {
        Objects.requireNonNull( feature );
        Objects.requireNonNull( resolvedProject );
        Objects.requireNonNull( unitMapper );
        Objects.requireNonNull( executors );
        Objects.requireNonNull( sharedWriters );
        Objects.requireNonNull( evaluation );

        this.feature = feature;
        this.resolvedProject = resolvedProject;
        this.project = project;
        this.unitMapper = unitMapper;
        this.executors = executors;
        this.sharedWriters = sharedWriters;
        this.evaluation = evaluation;

        // Error message
        errorMessage = "While processing feature " + feature;
    }

    @Override
    public FeatureProcessingResult get()
    {
        // Report
        LOGGER.debug( "Started feature '{}'", this.feature );

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
                List<Supplier<PoolOfPairs<Double, Ensemble>>> pools =
                        PoolFactory.getEnsemblePools( this.evaluation,
                                                      this.project.getDatabase(),
                                                      this.project.getFeaturesCache(),
                                                      this.project,
                                                      this.feature,
                                                      this.unitMapper );

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

                return this.processFeature( this.evaluation,
                                            projectConfig,
                                            processor,
                                            pools,
                                            pairsWriter,
                                            basePairsWriter );
            }
            // All other types
            else
            {
                List<Supplier<PoolOfPairs<Double, Double>>> pools =
                        PoolFactory.getSingleValuedPools( this.evaluation,
                                                          this.project.getDatabase(),
                                                          this.project.getFeaturesCache(),
                                                          this.project,
                                                          this.feature,
                                                          this.unitMapper );

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

                return this.processFeature( this.evaluation,
                                            projectConfig,
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
     * @param evaluation the evaluation
     * @param system settings the system settings
     * @param projectConfig the project declaration
     * @param processor the metric processor
     * @param pools the data pools
     * @param sampleWriter the sample data writer
     * @param baselineSampleWriter the baseline sample data writer
     * @return a processing result
     */

    private <L, R> FeatureProcessingResult processFeature( Evaluation evaluation,
                                                           ProjectConfig projectConfig,
                                                           MetricProcessor<PoolOfPairs<L, R>> processor,
                                                           List<Supplier<PoolOfPairs<L, R>>> pools,
                                                           PairsWriter<L, R> sampleWriter,
                                                           PairsWriter<L, R> baselineSampleWriter )
    {
        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        List<CompletableFuture<Set<Path>>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // The union of statistics types for which statistics were actually produced
        Set<StatisticType> typesProduced = new HashSet<>();

        // Something published?
        AtomicBoolean published = new AtomicBoolean();

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
                                         .thenApplyAsync( this.getStatisticsProcessingTask( processor, projectConfig ),
                                                          this.executors.getPairExecutor() )
                                         .thenApplyAsync( statistics -> {
                                             
                                             boolean success = this.publish( evaluation,
                                                                             statistics,
                                                                             this.getGroupId(),
                                                                             processor.getMetricOutputTypesToCache() );

                                             // Notify that something was published
                                             // This is needed to confirm group completion - cannot complete a message
                                             // group if nothing was published to it.
                                             if( success )
                                             {
                                                 published.set( true );
                                             }
                                             
                                             // Register statistics produced
                                             typesProduced.addAll( statistics.getStatisticTypes() );

                                             return Collections.emptySet();
                                         },
                                                          // Consuming happens in the product thread pool and publishing
                                                          // should happen in a different one because production is
                                                          // flow-controlled with respect to consumption using a naive 
                                                          // blocking approach, which would otherwise risk deadlock. 
                                                          this.executors.getPairExecutor() );

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
            
            // Publish any end of pipeline/cached statistics and notify complete
            if ( published.get() )
            {
                this.publish( evaluation,
                              processor.getCachedMetricOutput(),
                              this.getGroupId(),
                              Collections.emptySet() );

                // Notify consumers that all statistics for this group have been published
                evaluation.markGroupPublicationCompleteReportedSuccess( this.getGroupId() );
            }
        }
        catch ( CompletionException e )
        {
            // Otherwise, chain and propagate the exception up to the top.
            throw new WresProcessingException( this.errorMessage, e );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            
            throw new WresProcessingException( this.errorMessage, e );
        }

        Set<Path> paths = new HashSet<>();

        // Unearth the Set<Path> inside listOfFutures now that join() was called
        // above.
        for ( CompletableFuture<Set<Path>> completedFuture : listOfFutures )
        {
            Set<Path> innerPaths = completedFuture.getNow( Collections.emptySet() );
            paths.addAll( innerPaths );
        }

        Set<Path> allPaths = Collections.unmodifiableSet( paths );

        return new FeatureProcessingResult( this.feature,
                                            allPaths,
                                            !typesProduced.isEmpty() );
    }

    /**
     * Publishes the statistics to an evaluation.
     * 
     * @param evaluation the evaluation
     * @param statistics the statistics
     * @param groupId the statistics group identifier
     * @param a set of statistics types to ignore when publishing because they are cached and published later
     * @return true if something was published, otherwise false
     * @throws EvaluationEventException if the statistics could not be published
     */

    private boolean publish( Evaluation evaluation,
                             StatisticsForProject statistics,
                             String groupId,
                             Set<StatisticType> ignore )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( statistics );
        Objects.requireNonNull( groupId );
        Objects.requireNonNull( ignore );

        boolean returnMe = false;

        try
        {
            Collection<Statistics> publishMe = MessageFactory.parse( statistics, ignore );

            for ( Statistics next : publishMe )
            {
                evaluation.publish( next, groupId );
                returnMe = true;
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new WresProcessingException( "Interrupted while completing evaluation "
                                               + evaluation.getEvaluationId()
                                               + ".",
                                               e );
        }

        return returnMe;
    }

    /**
     * Returns the group identifier for the feature. The identifier is composed of the l/r/b feature names.
     * 
     * @return a group identifier
     */

    private String getGroupId()
    {
        String left = this.feature.getLeftName();
        String right = this.feature.getRightName();
        String baseline = this.feature.getBaselineName();

        String returnMe = left + "-" + right;

        if ( Objects.nonNull( baseline ) )
        {
            returnMe = returnMe + "-" + baseline;
        }

        return returnMe;
    }

    /**
     * Returns a task that writes pairs. Returns an empty set of paths, since pairs are not written per feature. Paths
     * to pairs should be reported for all features, not per feature. See #71874.
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

                                    // #71874: pairs are not written per feature so do not report on the 
                                    // paths to pairs for each feature. Report on the pairs for all features.
                                    return Collections.emptySet();
                                },
                                                 this.executors.getProductExecutor() );
    }

    /**
     * Returns a function that consumes a {@link PoolOfPairs} and produces {@link StatisticsForProject}.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param processor the metric processor
     * @param projectConfig the project declaration
     * @return a function that consumes a pool and produces statistics
     */

    private <L, R> Function<PoolOfPairs<L, R>, StatisticsForProject>
            getStatisticsProcessingTask( MetricProcessor<PoolOfPairs<L, R>> processor,
                                         ProjectConfig projectConfig )
    {
        return pool -> {
            Objects.requireNonNull( pool );

            // No data in the composition
            if ( pool.getRawData().isEmpty()
                 && ( !pool.hasBaseline() || pool.getBaselineData().getRawData().isEmpty() ) )
            {
                LOGGER.debug( "Empty pool discovered for {}: no statistics will be produced.", pool.getMetadata() );

                Builder builder = new Builder();

                // Empty container
                return builder.build();
            }

            StatisticsForProject statistics = processor.apply( pool );
            
            // Compute separate statistics for the baseline?
            if ( pool.hasBaseline() && projectConfig.getInputs().getBaseline().isSeparateMetrics() )
            {
                LOGGER.debug( "Computing separate statistics for the baseline pairs associated with pool {}.",
                              pool.getMetadata() );

                StatisticsForProject baselineStatistics = processor.apply( pool.getBaselineData() );

                try
                {
                    statistics = new Builder().addStatistics( statistics )
                                              .addStatistics( baselineStatistics )
                                              .build();
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();

                    throw new WresProcessingException( this.errorMessage, e );
                }
            }

            return statistics;
        };
    }

}
