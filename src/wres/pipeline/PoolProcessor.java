package wres.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeSeries;
import wres.events.Evaluation;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.pipeline.PoolProcessingResult.Status;
import wres.pipeline.statistics.MetricProcessor;
import wres.statistics.generated.Statistics;

/**
 * Processes a pool of pairs, creating and publishing statistics.
 * 
 * @param <L> the type of left-ish data in the pool of pairs
 * @param <R> the type of right-ish data in the pool of pairs
 * @author James Brown
 */

class PoolProcessor<L, R> implements Supplier<PoolProcessingResult>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolProcessor.class );

    /** The project declaration. */
    private final ProjectConfig projectConfig;

    /** The evaluation. */
    private final Evaluation evaluation;

    /** The pairs writer. */
    private final PairsWriter<L, R> pairsWriter;

    /** The baseline pairs writer. */
    private final PairsWriter<L, R> basePairsWriter;

    /** Monitor. */
    private final EvaluationEvent monitor;

    /** A unique identifier for the group to which this pool belongs for messaging purposes. */
    private final String messageGroupId;

    /** The pool supplier. */
    private final Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier;

    /** The pool request or description. */
    private final PoolRequest poolRequest;

    /** The metric processors, one for each metrics declaration. */
    private final List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors;

    /** The trace count estimator. */
    private final ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator;

    /** A group publication tracker. */
    private final PoolGroupTracker poolGroupTracker;

    /**
     * Builder.
     * 
     * @author James Brown
     * @param <L> the left data type
     * @param <R> the right data type
     */
    static class Builder<L, R>
    {
        /** The project declaration. */
        private ProjectConfig projectConfig;

        /** The evaluation. */
        private Evaluation evaluation;

        /** The pairs writer. */
        private PairsWriter<L, R> pairsWriter;

        /** The baseline pairs writer. */
        private PairsWriter<L, R> basePairsWriter;

        /** Monitor. */
        private EvaluationEvent monitor;

        /** The pool supplier. */
        private Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier;

        /** The pool request or description. */
        private PoolRequest poolRequest;

        /** The metric processors, one for each metrics declaration. */
        private List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors;

        /** The trace count estimator. */
        private ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator;

        /** Group publication tracker. */
        private PoolGroupTracker poolGroupTracker;

        /**
         * @param projectConfig the projectConfig to set
         * @return this builder
         */
        Builder<L, R> setProjectConfig( ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;
            return this;
        }

        /**
         * @param evaluation the evaluation to set
         * @return this builder
         */
        Builder<L, R> setEvaluation( Evaluation evaluation )
        {
            this.evaluation = evaluation;
            return this;
        }

        /**
         * @param pairsWriter the pairsWriter to set
         * @return this builder
         */
        Builder<L, R> setPairsWriter( PairsWriter<L, R> pairsWriter )
        {
            this.pairsWriter = pairsWriter;
            return this;
        }

        /**
         * @param basePairsWriter the basePairsWriter to set
         * @return this builder
         */
        Builder<L, R> setBasePairsWriter( PairsWriter<L, R> basePairsWriter )
        {
            this.basePairsWriter = basePairsWriter;
            return this;
        }

        /**
         * @param monitor the monitor to set
         * @return this builder
         */
        Builder<L, R> setMonitor( EvaluationEvent monitor )
        {
            this.monitor = monitor;
            return this;
        }

        /**
         * @param poolSupplier the poolSupplier to set
         * @return this builder
         */
        Builder<L, R> setPoolSupplier( Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier )
        {
            this.poolSupplier = poolSupplier;
            return this;
        }

        /**
         * @param poolRequest the poolRequest to set
         * @return this builder
         */
        Builder<L, R> setPoolRequest( PoolRequest poolRequest )
        {
            this.poolRequest = poolRequest;
            return this;
        }

        /**
         * @param groupPublicationTracker the group publication tracker
         * @return this builder
         */
        Builder<L, R> setPoolGroupTracker( PoolGroupTracker groupPublicationTracker )
        {
            this.poolGroupTracker = groupPublicationTracker;
            return this;
        }

        /**
         * @param metricProcessor the metricProcessor to set
         * @return this builder
         */
        Builder<L, R> setMetricProcessors( List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors )
        {
            this.metricProcessors = metricProcessors;
            return this;
        }

        /**
         * @param traceCountEstimator the traceCountEstimator to set
         * @return this builder
         */
        Builder<L, R> setTraceCountEstimator( ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator )
        {
            this.traceCountEstimator = traceCountEstimator;
            return this;
        }

        /**
         * @return an instance of a pool processor
         */

        PoolProcessor<L, R> build()
        {
            return new PoolProcessor<>( this );
        }

        /**
         * Package private constructor.
         */
        Builder()
        {
        }
    }

    @Override
    public PoolProcessingResult get()
    {
        // Is the evaluation still alive? If not, do not proceed.
        if ( this.evaluation.isFailed() )
        {
            throw new WresProcessingException( "While processong a pool, discovered that a messaging client has marked "
                                               + "evaluation "
                                               + this.evaluation.getEvaluationId()
                                               + " as failed without the possibility of recovery. Processing of the "
                                               + "pool cannot continue. The pool that encountered the error is: "
                                               + this.poolRequest
                                               + "." );
        }

        // Get the pool
        Pool<TimeSeries<Pair<L, R>>> pool = this.poolSupplier.get();

        // Compute the statistics
        List<StatisticsStore> statistics = this.getStatisticsProcessingTask( this.metricProcessors,
                                                                             this.projectConfig,
                                                                             this.traceCountEstimator )
                                               .apply( pool );
        
        // Publish the statistics
        Status status = this.publish( this.evaluation,
                                      statistics,
                                      this.getMessageGroupId() );

        // Register publication of the pool with the pool group tracker
        this.poolGroupTracker.registerPublication( this.getMessageGroupId(), status == Status.STATISTICS_PUBLISHED );

        // TODO: extract the pair writing to the product writers, i.e., publish the pairs
        // Write the main pairs
        this.getPairWritingTask( false, this.pairsWriter )
            .accept( pool );

        // Write any baseline pairs, as needed
        this.getPairWritingTask( true, this.basePairsWriter )
            .accept( pool );

        return new PoolProcessingResult( this.poolRequest, status );
    }

    /**
     * Publishes the statistics to an evaluation.
     * 
     * @param evaluation the evaluation
     * @param statistics the statistics
     * @param groupId the statistics group identifier
     * @return the status
     * @throws EvaluationEventException if the statistics could not be published
     */

    private Status publish( Evaluation evaluation,
                            List<StatisticsStore> statistics,
                            String groupId )
    {
        Objects.requireNonNull( evaluation, "Cannot publish statistics without an evaluation." );
        Objects.requireNonNull( statistics, "Cannot publish null statistics." );
        Objects.requireNonNull( groupId, "Cannot publish statistics without a group identifier." );

        Status status = Status.STATISTICS_NOT_AVAILABLE;

        try
        {
            for ( StatisticsStore nextStatistics : statistics )
            {
                Collection<Statistics> publishMe = MessageFactory.getStatistics( nextStatistics );

                for ( Statistics next : publishMe )
                {
                    if ( !evaluation.isFailed() )
                    {
                        evaluation.publish( next, groupId );
                        status = Status.STATISTICS_PUBLISHED;
                    }
                    else
                    {
                        status = Status.STATISTICS_AVAILABLE_NOT_PUBLISHED;
                        LOGGER.debug( "Statistics were available for a pool but were not published, because the "
                                      + "evaluation was marked failed. The pool is: {}.",
                                      this.poolRequest );
                    }
                }
            }

            LOGGER.debug( "Statistics publication status: {}.", status );

        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new WresProcessingException( "Interrupted while completing evaluation "
                                               + evaluation.getEvaluationId()
                                               + ".",
                                               e );
        }

        return status;
    }

    /**
     * @return the group identifier
     */

    private String getMessageGroupId()
    {
        return this.messageGroupId;
    }

    /**
     * Returns a task that writes pairs.
     * 
     * @param useBaseline is true to write the baseline pairs
     * @param sharedWriters the consumers of paired data for writing
     * @return a task that writes pairs
     */

    private Consumer<Pool<TimeSeries<Pair<L, R>>>> getPairWritingTask( boolean useBaseline,
                                                                       PairsWriter<L, R> sharedWriters )
    {
        return pairs -> {

            if ( Objects.nonNull( sharedWriters ) )
            {
                // Baseline data?
                if ( useBaseline )
                {
                    if ( pairs.hasBaseline() )
                    {
                        sharedWriters.accept( pairs.getBaselineData() );
                    }
                    else
                    {
                        LOGGER.debug( "No baseline pairs were discovered for pool {}.", pairs.getMetadata() );
                    }
                }
                // Main pairs
                else
                {
                    sharedWriters.accept( pairs );
                }
            }
        };
    }

    /**
     * Returns a function that consumes a {@link Pool} and produces a list of {@link StatisticsStore}. The list 
     * contains one blob of statistics for each metrics declaration.
     * 
     * @param processors the metric processors
     * @param projectConfig the project declaration
     * @param traceCountEstimator a function that estimates trace count, in order to help with monitoring
     * @return a function that consumes a pool and produces one blob of statistics for each processor
     */

    private Function<Pool<TimeSeries<Pair<L, R>>>, List<StatisticsStore>>
            getStatisticsProcessingTask( List<MetricProcessor<Pool<TimeSeries<Pair<L, R>>>>> processors,
                                         ProjectConfig projectConfig,
                                         ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator )
    {
        return pool -> {
            Objects.requireNonNull( pool );

            List<StatisticsStore> returnMe = new ArrayList<>();

            // No data in the composition
            if ( pool.get().isEmpty()
                 && ( !pool.hasBaseline() || pool.getBaselineData().get().isEmpty() ) )
            {
                LOGGER.debug( "Empty pool discovered for {}: no statistics will be produced.", pool.getMetadata() );

                // Empty container
                StatisticsStore empty = new StatisticsStore.Builder().build();
                returnMe.add( empty );

                return returnMe;
            }

            // One blob of statistics for each processor, one processor for each metrics declaration
            for ( MetricProcessor<Pool<TimeSeries<Pair<L, R>>>> processor : processors )
            {
                StatisticsStore nextStatistics = this.getStatistics( projectConfig, processor, pool );
                returnMe.add( nextStatistics );

                int baselineTraceCount = 0;
                if ( pool.hasBaseline() )
                {
                    Pool<TimeSeries<Pair<L, R>>> baseline = pool.getBaselineData();
                    baselineTraceCount = traceCountEstimator.applyAsInt( baseline );
                }

                this.monitor.registerPool( pool, traceCountEstimator.applyAsInt( pool ), baselineTraceCount );
            }

            return Collections.unmodifiableList( returnMe );
        };
    }

    /**
     * Returns the statistics from a processor and pool.
     * @param projectConfig the project declaration
     * @param processor the metrics processor
     * @param pool the pool
     * @return the statistics
     */

    private StatisticsStore getStatistics( ProjectConfig projectConfig,
                                           MetricProcessor<Pool<TimeSeries<Pair<L, R>>>> processor,
                                           Pool<TimeSeries<Pair<L, R>>> pool )
    {
        try
        {
            StatisticsStore statistics = processor.apply( pool );
            StatisticsStore.Builder builder = new StatisticsStore.Builder();
            ThresholdsByMetricAndFeature metrics = processor.getMetrics();
            builder.addStatistics( statistics )
                   .setMinimumSampleSize( metrics.getMinimumSampleSize() );
            
            // Compute separate statistics for the baseline?
            if ( pool.hasBaseline() )
            {
                Pool<TimeSeries<Pair<L, R>>> baseline = pool.getBaselineData();

                if ( projectConfig.getInputs().getBaseline().isSeparateMetrics() )
                {
                    LOGGER.debug( "Computing separate statistics for the baseline pairs associated with pool {}.",
                                  baseline.getMetadata() );

                    StatisticsStore baselineStatistics = processor.apply( baseline );
                    builder.addStatistics( baselineStatistics );
                }
            }

            return builder.build();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new WresProcessingException( "Encountered an error while processing pool " + this.poolRequest
                                               + ".",
                                               e );
        }
    }

    /**
     * Build a pool processor. 
     * 
     * @param builder the builder
     * @throws NullPointerException if any required input is null
     */

    private PoolProcessor( Builder<L, R> builder )
    {
        this.projectConfig = builder.projectConfig;
        this.pairsWriter = builder.pairsWriter;
        this.basePairsWriter = builder.basePairsWriter;
        this.evaluation = builder.evaluation;
        this.monitor = builder.monitor;
        this.poolSupplier = builder.poolSupplier;
        this.poolRequest = builder.poolRequest;
        this.metricProcessors = List.copyOf( builder.metricProcessors ); // Validates nullity
        this.traceCountEstimator = builder.traceCountEstimator;
        this.poolGroupTracker = builder.poolGroupTracker;

        Objects.requireNonNull( this.projectConfig );
        Objects.requireNonNull( this.evaluation );
        Objects.requireNonNull( this.monitor );
        Objects.requireNonNull( this.poolSupplier );
        Objects.requireNonNull( this.poolRequest );
        Objects.requireNonNull( this.traceCountEstimator );
        Objects.requireNonNull( this.poolGroupTracker );

        // Set the message group identifier
        this.messageGroupId = poolGroupTracker.getGroupId( this.poolRequest );

        LOGGER.debug( "Created a PoolProcessor that belongs to message group {}. The pool request is: {}.",
                      this.messageGroupId,
                      this.poolRequest );
    }

}
