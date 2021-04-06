package wres.control;

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
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.tuple.Pair;
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
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.Builder;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.events.Evaluation;
import wres.io.concurrency.Pipelines;
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
                List<Supplier<Pool<Pair<Double, Ensemble>>>> pools =
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

                MetricProcessor<Pool<Pair<Double, Ensemble>>> processor =
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
                List<Supplier<Pool<Pair<Double, Double>>>> pools =
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

                MetricProcessor<Pool<Pair<Double, Double>>> processor =
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
     * @param projectConfig the project declaration
     * @param processor the metric processor
     * @param pools the data pools
     * @param pairsWriter the pairs writer
     * @param basePairsWriter the baseline pairs writer
     * @return a processing result
     */

    private <L, R> FeatureProcessingResult processFeature( Evaluation evaluation,
                                                           ProjectConfig projectConfig,
                                                           MetricProcessor<Pool<Pair<L, R>>> processor,
                                                           List<Supplier<Pool<Pair<L, R>>>> pools,
                                                           PairsWriter<L, R> pairsWriter,
                                                           PairsWriter<L, R> basePairsWriter )
    {
        // Queue the various tasks by time window (time window is the pooling dimension for metric calculation here)
        List<CompletableFuture<Void>> listOfFutures = new ArrayList<>(); //List of futures to test for completion

        // The union of statistics types for which statistics were actually produced
        Set<StatisticType> typesProduced = new HashSet<>();

        LOGGER.debug( "Submitting {} pools in group {} for asynchronous processing.", pools.size(), this.getGroupId() );

        // Something published?
        AtomicBoolean published = new AtomicBoolean();

        try
        {
            for ( Supplier<Pool<Pair<L, R>>> poolSupplier : pools )
            {
                // Behold, the feature processing pipeline
                final CompletableFuture<Void> pipeline =
                        // Retrieve the pairs                
                        CompletableFuture.supplyAsync( poolSupplier, this.executors.getPairExecutor() )
                                         // Write the main pairs, as needed
                                         .thenApply( this.getPairWritingTask( false, pairsWriter, projectConfig ) )
                                         // Write the baseline pairs, as needed
                                         .thenApply( this.getPairWritingTask( true, basePairsWriter, projectConfig ) )
                                         // Compute the statistics
                                         .thenApply( this.getStatisticsProcessingTask( processor, projectConfig ) )
                                         // Publish the statistics for awaiting format consumers
                                         .thenAcceptAsync( statistics -> {

                                             boolean success = this.publish( evaluation,
                                                                             statistics,
                                                                             this.getGroupId(),
                                                                             processor.getMetricOutputTypesToCache() );

                                             // Notify that something was published
                                             // This is needed to confirm group completion - cannot complete a message
                                             // group if nothing was published to it.
                                             if ( success )
                                             {
                                                 published.set( true );
                                             }

                                             // Register statistics produced
                                             typesProduced.addAll( statistics.getStatisticTypes() );
                                         },
                                                           // Consuming happens in the product thread pool and publishing
                                                           // should happen in a different one because production is
                                                           // flow-controlled with respect to consumption using a naive 
                                                           // blocking approach, which would otherwise risk deadlock. 
                                                           this.executors.getPairExecutor() );

                // Add the task to the list
                listOfFutures.add( pipeline );
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
            Pipelines.doAllOrException( listOfFutures ).join();

            // Publish any end of pipeline/cached statistics and notify complete
            if ( processor.hasCachedMetricOutput() )
            {
                this.publish( evaluation,
                              processor.getCachedMetricOutput(),
                              this.getGroupId(),
                              Collections.emptySet() );

                published.set( true );
            }

            // Published? Then mark complete.
            if ( published.get() )
            {
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

        return new FeatureProcessingResult( this.feature,
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

            LOGGER.debug( "Published statistics: {}. Ignored these types: {}.", returnMe, ignore );

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
     * @param useBaseline is true to write the baseline pairs
     * @param sharedWriters the consumers of paired data for writing
     * @param projectConfig the project declaration
     * @return a task that writes pairs
     */

    private <L, R> UnaryOperator<Pool<Pair<L, R>>> getPairWritingTask( boolean useBaseline,
                                                                       PairsWriter<L, R> sharedWriters,
                                                                       ProjectConfig projectConfig )
    {
        return pairs -> {

            if ( Objects.nonNull( sharedWriters ) )
            {
                // Baseline data?
                if ( useBaseline && Objects.nonNull( projectConfig.getInputs().getBaseline() ) )
                {
                    sharedWriters.accept( pairs.getBaselineData() );
                }
                else
                {
                    sharedWriters.accept( pairs );
                }
            }
            // #71874: pairs are not written per feature so do not report on the 
            // paths to pairs for each feature. Report on the pairs for all features.
            return pairs;
        };
    }

    /**
     * Returns a function that consumes a {@link Pool} and produces {@link StatisticsForProject}.
     * 
     * @param <L> the left data type
     * @param <R> the right data type
     * @param processor the metric processor
     * @param projectConfig the project declaration
     * @return a function that consumes a pool and produces statistics
     */

    private <L, R> Function<Pool<Pair<L, R>>, StatisticsForProject>
            getStatisticsProcessingTask( MetricProcessor<Pool<Pair<L, R>>> processor,
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
