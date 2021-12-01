package wres.pipeline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.time.TimeSeries;
import wres.events.Evaluation;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.pipeline.statistics.MetricProcessor;
import wres.statistics.generated.Statistics;

/**
 * Processes a pool of pairs, creating statistics.
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

    /** An error message. */
    private final String errorMessage;

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
        // Get the pool
        Pool<TimeSeries<Pair<L, R>>> pool = this.poolSupplier.get();
        
        // Compute the statistics
        List<StatisticsStore> statistics = this.getStatisticsProcessingTask( this.metricProcessors,
                                                                             this.projectConfig,
                                                                             this.traceCountEstimator )
                                               .apply( pool );

        // Publish the statistics 
        boolean published = this.publish( this.evaluation,
                                          statistics,
                                          this.getMessageGroupId() );

        // Register publication of the pool with the pool group tracker
        this.poolGroupTracker.registerPublication( this.getMessageGroupId(), published );

        // TODO: extract the pair writing to the product writers, i.e., publish the pairs
        // Write the main pairs
        this.getPairWritingTask( false,
                                 this.pairsWriter,
                                 this.projectConfig )
            .apply( pool );

        // Write any baseline pairs, as needed
        this.getPairWritingTask( true,
                                 this.basePairsWriter,
                                 this.projectConfig )
            .apply( pool );

        return new PoolProcessingResult( this.poolRequest, published );
    }

    /**
     * Publishes the statistics to an evaluation.
     * 
     * @param evaluation the evaluation
     * @param statistics the statistics
     * @param groupId the statistics group identifier
     * @return true if something was published, otherwise false
     * @throws EvaluationEventException if the statistics could not be published
     */

    private boolean publish( Evaluation evaluation,
                             List<StatisticsStore> statistics,
                             String groupId )
    {
        Objects.requireNonNull( evaluation, "Cannot publish statistics without an evaluation." );
        Objects.requireNonNull( statistics, "Cannot publish null statistics." );
        Objects.requireNonNull( groupId, "Cannot publish statistics without a group identifier." );

        boolean returnMe = false;

        try
        {
            for ( StatisticsStore nextStatistics : statistics )
            {
                Collection<Statistics> publishMe = MessageFactory.parse( nextStatistics );

                for ( Statistics next : publishMe )
                {
                    evaluation.publish( next, groupId );
                    returnMe = true;
                }
            }

            LOGGER.debug( "Published statistics: {}.", returnMe );

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
     * @return the group identifier
     */

    private String getMessageGroupId()
    {
        return this.messageGroupId;
    }

    /**
     * Returns a task that writes pairs. Returns an empty set of paths, since pairs are not written per feature. Paths
     * to pairs should be reported for all features, not per feature. See #71874.
     * 
     * @param useBaseline is true to write the baseline pairs
     * @param sharedWriters the consumers of paired data for writing
     * @param projectConfig the project declaration
     * @return a task that writes pairs
     */

    private UnaryOperator<Pool<TimeSeries<Pair<L, R>>>> getPairWritingTask( boolean useBaseline,
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
     * Returns a function that consumes a {@link Pool} and produces {@link StatisticsStore}.
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

            // Implement all processing and store the results
            try
            {
                // One blob of statistics for each processor, one processor for each metrics declaration
                for ( MetricProcessor<Pool<TimeSeries<Pair<L, R>>>> processor : processors )
                {
                    StatisticsStore statistics = processor.apply( pool );
                    StatisticsStore.Builder builder = new StatisticsStore.Builder();

                    builder.addStatistics( statistics )
                           .setMinimumSampleSize( processor.getMetrics().getMinimumSampleSize() );

                    // Compute separate statistics for the baseline?
                    int baselineTraceCount = 0;
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

                        baselineTraceCount = traceCountEstimator.applyAsInt( baseline );
                    }

                    StatisticsStore nextStatistics = builder.build();
                    returnMe.add( nextStatistics );

                    this.monitor.registerPool( pool, traceCountEstimator.applyAsInt( pool ), baselineTraceCount );
                }
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();

                throw new WresProcessingException( this.errorMessage, e );
            }

            return Collections.unmodifiableList( returnMe );
        };
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
        this.errorMessage = "Encountered an error while processing pool " + poolRequest + ".";
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
    }

}
