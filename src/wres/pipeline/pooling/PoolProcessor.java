package wres.pipeline.pooling;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well512a;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.SamplingUncertainty;
import wres.datamodel.bootstrap.QuantileCalculator;
import wres.datamodel.bootstrap.StationaryBootstrapResampler;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolRequest;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeSeries;
import wres.events.EvaluationMessager;
import wres.io.writing.csv.pairs.PairsWriter;
import wres.pipeline.EvaluationEvent;
import wres.pipeline.WresProcessingException;
import wres.pipeline.pooling.PoolProcessingResult.Status;
import wres.pipeline.statistics.StatisticsProcessor;
import wres.statistics.generated.Statistics;

/**
 * Processes a pool of pairs, creating and publishing statistics.
 *
 * @param <L> the type of left-ish data in the pool of pairs
 * @param <R> the type of right-ish data in the pool of pairs
 * @author James Brown
 */

public class PoolProcessor<L, R> implements Supplier<PoolProcessingResult>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolProcessor.class );

    /** The evaluation. */
    private final EvaluationMessager evaluation;

    /** The pairs writer. */
    private final PairsWriter<L, R> pairsWriter;

    /** The baseline pairs writer. */
    private final PairsWriter<L, R> basePairsWriter;

    /** Monitor. */
    private final EvaluationEvent monitor;

    /** A unique identifier for the group to which this pool belongs for messaging purposes. */
    private final String messageGroupId;

    /** The pool request or description. */
    private final PoolRequest poolRequest;

    /** The metric processors, one for each set of metrics. */
    private final List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors;

    /** The metric processors for sampling uncertainty calculation, one for each set of metrics. */
    private final List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> samplingUncertaintyMetricProcessors;

    /** The trace count estimator. */
    private final ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator;

    /** A group publication tracker. */
    private final PoolGroupTracker poolGroupTracker;

    /** Are separate metrics required for the baseline? */
    private final boolean separateMetrics;

    /** The sampling uncertainty declaration. */
    private final SamplingUncertainty samplingUncertainty;

    /** The stationary bootstrap block size estimator. */
    private final ToLongFunction<Pool<TimeSeries<Pair<L, R>>>> blockSize;

    /** The sampling uncertainty executor. */
    private final ExecutorService samplingUncertaintyExecutor;

    /** The pool supplier. */
    private Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier;

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

        if ( Objects.isNull( this.poolSupplier ) )
        {
            throw new WresProcessingException( "A pool processor cannot be re-used. An attempt was made to re-use this "
                                               + "processor: "
                                               + this
                                               + "." );
        }

        // Get the pool
        Pool<TimeSeries<Pair<L, R>>> pool = this.poolSupplier.get();

        // Render any potentially expensive state eligible for gc
        this.poolSupplier = null;

        LOGGER.debug( "Created pool {}.", pool.getMetadata() );

        // Create the statistics, generating sampling uncertainty estimates as needed
        List<Statistics> statistics = this.createStatistics( pool, this.samplingUncertainty, this.blockSize );

        // Publish the statistics
        Status status = this.publish( this.evaluation,
                                      statistics,
                                      this.getMessageGroupId() );

        // Register publication of the statistics for this pool/message group
        this.poolGroupTracker.registerPublication( this.getMessageGroupId(),
                                                   status == Status.STATISTICS_PUBLISHED );

        // TODO: extract the pair writing to the product writers, i.e., publish the pairs
        // Write the main pairs
        this.getPairWritingTask( false, this.pairsWriter )
            .accept( pool );

        // Write any baseline pairs, as needed
        this.getPairWritingTask( true, this.basePairsWriter )
            .accept( pool );

        // Any status events?
        List<EvaluationStatusMessage> statusEvents = pool.getMetadata()
                                                         .getEvaluationStatusEvents();

        return new PoolProcessingResult( this.poolRequest, status, statusEvents );
    }

    @Override
    public String toString()
    {
        return "Pool processor for pool: " + this.poolRequest;
    }

    /**
     * Builder.
     *
     * @author James Brown
     * @param <L> the left data type
     * @param <R> the right data type
     */
    public static class Builder<L, R>
    {
        /** The evaluation. */
        private EvaluationMessager evaluation;

        /** The pairs writer. */
        private PairsWriter<L, R> pairsWriter;

        /** The baseline pairs writer. */
        private PairsWriter<L, R> basePairsWriter;

        /** Monitor. */
        private EvaluationEvent monitor;

        /** The pool supplier. */
        private Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier;

        /** The sampling uncertainty declaration. */
        private SamplingUncertainty samplingUncertainty;

        /** The stationary bootstrap block size estimator. */
        private ToLongFunction<Pool<TimeSeries<Pair<L, R>>>> blockSize;

        /** The pool request or description. */
        private PoolRequest poolRequest;

        /** The metric processors, one for each set of metrics. */
        private List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors;

        /** The metric processors for sampling uncertainty calculation, one for each set of metrics. */
        private List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> samplingUncertaintyMetricProcessors;

        /** The trace count estimator. */
        private ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator;

        /** Group publication tracker. */
        private PoolGroupTracker poolGroupTracker;

        /** The sampling uncertainty executor. */
        private ExecutorService samplingUncertaintyExecutor;

        /** Are separate metrics required for the baseline? */
        private boolean separateMetrics;

        /**
         * @param evaluation the evaluation to set
         * @return this builder
         */
        public Builder<L, R> setEvaluation( EvaluationMessager evaluation )
        {
            this.evaluation = evaluation;
            return this;
        }

        /**
         * @param pairsWriter the pair writer to set
         * @return this builder
         */
        public Builder<L, R> setPairsWriter( PairsWriter<L, R> pairsWriter )
        {
            this.pairsWriter = pairsWriter;
            return this;
        }

        /**
         * @param basePairsWriter the baseline pair writer to set
         * @return this builder
         */
        public Builder<L, R> setBasePairsWriter( PairsWriter<L, R> basePairsWriter )
        {
            this.basePairsWriter = basePairsWriter;
            return this;
        }

        /**
         * @param monitor the monitor to set
         * @return this builder
         */
        public Builder<L, R> setMonitor( EvaluationEvent monitor )
        {
            this.monitor = monitor;
            return this;
        }

        /**
         * @param poolSupplier the pool supplier to set
         * @return this builder
         */
        public Builder<L, R> setPoolSupplier( Supplier<Pool<TimeSeries<Pair<L, R>>>> poolSupplier )
        {
            this.poolSupplier = poolSupplier;
            return this;
        }

        /**
         * @param poolRequest the pool request to set
         * @return this builder
         */
        public Builder<L, R> setPoolRequest( PoolRequest poolRequest )
        {
            this.poolRequest = poolRequest;
            return this;
        }

        /**
         * @param groupPublicationTracker the group publication tracker
         * @return this builder
         */
        public Builder<L, R> setPoolGroupTracker( PoolGroupTracker groupPublicationTracker )
        {
            this.poolGroupTracker = groupPublicationTracker;
            return this;
        }

        /**
         * @param metricProcessors the metric processors to set
         * @return this builder
         */
        public Builder<L, R> setMetricProcessors( List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> metricProcessors )
        {
            this.metricProcessors = metricProcessors;
            return this;
        }

        /**
         * @param samplingUncertaintyMetricProcessors the metric processors to set
         * @return this builder
         */
        public Builder<L, R> setSamplingUncertaintyMetricProcessors( List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> samplingUncertaintyMetricProcessors )
        {
            this.samplingUncertaintyMetricProcessors = samplingUncertaintyMetricProcessors;
            return this;
        }

        /**
         * @param traceCountEstimator the trace count estimator to set
         * @return this builder
         */
        public Builder<L, R> setTraceCountEstimator( ToIntFunction<Pool<TimeSeries<Pair<L, R>>>> traceCountEstimator )
        {
            this.traceCountEstimator = traceCountEstimator;
            return this;
        }

        /**
         * @param separateMetrics whether separate metrics are required for the baseline
         * @return this builder
         */
        public Builder<L, R> setSeparateMetricsForBaseline( boolean separateMetrics )
        {
            this.separateMetrics = separateMetrics;
            return this;
        }

        /**
         * @param samplingUncertainty the sampling uncertainty declaration
         * @return this builder
         */
        public Builder<L, R> setSamplingUncertaintyDeclaration( SamplingUncertainty samplingUncertainty )
        {
            this.samplingUncertainty = samplingUncertainty;
            return this;
        }

        /**
         * @param blockSize the bootstrap block size estimator
         * @return this builder
         */
        public Builder<L, R> setSamplingUncertaintyBlockSize( ToLongFunction<Pool<TimeSeries<Pair<L, R>>>> blockSize )
        {
            this.blockSize = blockSize;
            return this;
        }

        /**
         * @param samplingUncertaintyExecutor the sampling uncertainty executor
         * @return this builder
         */
        public Builder<L, R> setSamplingUncertaintyExecutor( ExecutorService samplingUncertaintyExecutor )
        {
            this.samplingUncertaintyExecutor = samplingUncertaintyExecutor;
            return this;
        }

        /**
         * @return an instance of a pool processor
         */

        public PoolProcessor<L, R> build()
        {
            return new PoolProcessor<>( this );
        }
    }

    /**
     * Creates and publishes the statistics for the supplied pool, generating estimates of the sampling uncertainty, as
     * needed.
     * @param pool the pool
     * @param samplingUncertainty the sampling uncertainty declaration
     * @param blockSizeEstimator the stationary bootstrap block size estimator for calculating sampling uncertainty
     * @return the statistics
     */

    private List<Statistics> createStatistics( Pool<TimeSeries<Pair<L, R>>> pool,
                                               SamplingUncertainty samplingUncertainty,
                                               ToLongFunction<Pool<TimeSeries<Pair<L, R>>>> blockSizeEstimator )
    {
        // Compute the statistics
        Function<Pool<TimeSeries<Pair<L, R>>>, List<StatisticsStore>> processor =
                this.getStatisticsProcessingTask( this.metricProcessors,
                                                  this.traceCountEstimator );

        List<Statistics> statistics = new ArrayList<>();
        try
        {
            // Generate the nominal values of the statistics
            List<StatisticsStore> stores = new ArrayList<>( processor.apply( pool ) );

            for ( StatisticsStore nextStore : stores )
            {
                List<Statistics> nextStatistics = MessageFactory.getStatistics( nextStore );
                statistics.addAll( nextStatistics );
            }

            // Generate any sampling uncertainty estimates
            List<Statistics> quantiles =
                    this.getSamplingUncertaintyStatistics( pool,
                                                           samplingUncertainty,
                                                           blockSizeEstimator,
                                                           Collections.unmodifiableList( statistics ) );
            statistics.addAll( quantiles );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();

            throw new WresProcessingException( "Interrupted while completing evaluation "
                                               + this.evaluation.getEvaluationId()
                                               + ".",
                                               e );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Generates sampling uncertainty statistics, as needed.
     *
     * @param pool the pool
     * @param samplingUncertainty the sampling uncertainty declaration
     * @param blockSizeEstimator the stationary bootstrap block size estimator for calculating sampling uncertainty
     * @param nominalStatistics the nominal values of the statistics
     * @return the statistics, if any
     */

    private List<Statistics> getSamplingUncertaintyStatistics( Pool<TimeSeries<Pair<L, R>>> pool,
                                                               SamplingUncertainty samplingUncertainty,
                                                               ToLongFunction<Pool<TimeSeries<Pair<L, R>>>> blockSizeEstimator,
                                                               List<Statistics> nominalStatistics )
    {
        List<Statistics> statistics = new ArrayList<>();

        if ( Objects.nonNull( samplingUncertainty ) )
        {
            // Get the statistics processor for the sampling uncertainties
            Function<Pool<TimeSeries<Pair<L, R>>>, List<StatisticsStore>> processor =
                    this.getStatisticsProcessingTask( this.samplingUncertaintyMetricProcessors,
                                                      this.traceCountEstimator );

            int sampleSize = samplingUncertainty.sampleSize();

            // Estimate the optimal block sizes from the data
            long optimalBlockSize = blockSizeEstimator.applyAsLong( pool );

            // Seed the random number generator and report to aid reproduction
            // TODO: may help to abstract and inject into this class for easier reproduction of entire evaluations
            long seed = System.currentTimeMillis() + System.identityHashCode( this );

            LOGGER.debug( "Estimating the sampling uncertainties with the stationary block bootstrap using a sample "
                          + "size of {}, a block size of {} and a seed of {} for the pool with metadata {}. This may "
                          + "take some time...",
                          sampleSize,
                          optimalBlockSize,
                          seed,
                          pool.getMetadata() );

            // Create the quantile calculators, one for each threshold
            Map<OneOrTwoThresholds, QuantileCalculator> quantileCalculators =
                    this.getQuantileCalculators( nominalStatistics, samplingUncertainty );

            // Create the resampling structure
            RandomGenerator randomGenerator = new Well512a( seed );
            StationaryBootstrapResampler<Pair<L, R>> resampler =
                    StationaryBootstrapResampler.of( pool,
                                                     optimalBlockSize,
                                                     randomGenerator,
                                                     this.samplingUncertaintyExecutor );

            // Iterate the samples and register the statistics for quantile calculation
            for ( int i = 0; i < sampleSize; i++ )
            {
                Pool<TimeSeries<Pair<L, R>>> nextPool = resampler.resample();
                List<StatisticsStore> stores = processor.apply( nextPool );
                this.updateSampleStatistics( stores, quantileCalculators );

                // Log progress every 100 samples
                if ( LOGGER.isDebugEnabled()
                     && i > 1
                     && ( i + 1 ) % 100 == 0 )
                {
                    LOGGER.debug( "Completed resample {} of {} for pool request {}.",
                                  ( i + 1 ),
                                  sampleSize,
                                  this.poolRequest );
                }
            }

            // Add the completed quantiles
            quantileCalculators.values()
                               .forEach( n -> statistics.addAll( n.get() ) );
        }

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Updates the quantile calculators with the supplied statistics
     * @param stores the statistics
     * @param calculators the quantile calculators
     */

    private void updateSampleStatistics( List<StatisticsStore> stores,
                                         Map<OneOrTwoThresholds, QuantileCalculator> calculators )
    {
        try
        {
            // Split the statistics by threshold pool
            Map<OneOrTwoThresholds, Statistics> byPool = new HashMap<>();
            for ( StatisticsStore nextStore : stores )
            {
                List<Statistics> nextStatistics = MessageFactory.getStatistics( nextStore );
                nextStatistics.forEach( n -> byPool.put( this.getThreshold( n ), n ) );
            }

            // Iterate through the pools and increment the corresponding calculators
            for ( Map.Entry<OneOrTwoThresholds, Statistics> nextEntry : byPool.entrySet() )
            {
                OneOrTwoThresholds nextThreshold = nextEntry.getKey();
                QuantileCalculator calculator = calculators.get( nextThreshold );
                Statistics statistics = byPool.get( nextThreshold );
                calculator.add( statistics );
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
    }

    /**
     * Extracts the threshold from the statistics
     * @param statistics the statistics
     * @return the threshold
     */

    private OneOrTwoThresholds getThreshold( Statistics statistics )
    {
        ThresholdOuter eventThreshold = ThresholdOuter.of( statistics.getPool()
                                                                     .getEventThreshold() );
        ThresholdOuter decisionThreshold = null;

        if ( statistics.getPool()
                       .hasDecisionThreshold() )
        {
            decisionThreshold = ThresholdOuter.of( statistics.getPool()
                                                             .getDecisionThreshold() );
        }

        return OneOrTwoThresholds.of( eventThreshold, decisionThreshold );
    }

    /**
     * Creates the quantile calculators from the supplied list of statistics, one for each threshold.
     * @param statistics the statistics
     * @return the quantile calculators
     */

    private Map<OneOrTwoThresholds, QuantileCalculator> getQuantileCalculators( List<Statistics> statistics,
                                                                                SamplingUncertainty samplingUncertainty )
    {
        // Merge any metrics for corresponding thresholds
        BinaryOperator<Statistics> merger = ( first, second ) ->
        {
            Statistics.Builder merged = first.toBuilder();
            merged.mergeFrom( second );
            return merged.build();
        };

        Map<OneOrTwoThresholds, Statistics> merged = statistics.stream()
                                                               .collect( Collectors.toUnmodifiableMap( this::getThreshold,
                                                                                                       Function.identity(),
                                                                                                       merger ) );

        return merged.entrySet()
                     .stream()
                     .collect( Collectors.toUnmodifiableMap( Map.Entry::getKey,
                                                             n -> QuantileCalculator.of( n.getValue(),
                                                                                         samplingUncertainty.sampleSize(),
                                                                                         samplingUncertainty.quantiles(),
                                                                                         true ) ) );
    }

    /**
     * Publishes the statistics to an evaluation.
     *
     * @param evaluation the evaluation
     * @param statistics the statistics
     * @param groupId the statistics group identifier
     * @return the status
     */

    private Status publish( EvaluationMessager evaluation,
                            List<Statistics> statistics,
                            String groupId )
    {
        Objects.requireNonNull( evaluation, "Cannot publish statistics without an evaluation." );
        Objects.requireNonNull( statistics, "Cannot publish null statistics." );
        Objects.requireNonNull( groupId, "Cannot publish statistics without a group identifier." );

        Status status = Status.STATISTICS_NOT_AVAILABLE;

        for ( Statistics nextStatistics : statistics )
        {
            if ( !evaluation.isFailed() )
            {
                evaluation.publish( nextStatistics, groupId );
                status = Status.STATISTICS_PUBLISHED;
            }
            else
            {
                status = Status.STATISTICS_AVAILABLE_NOT_PUBLISHED;
                LOGGER.debug( "Statistics were available for a pool but were not published, because the "
                              + "evaluation was marked failed. The pool is: {}.",
                              this.poolRequest );
                break;
            }
        }

        LOGGER.debug( "Statistics publication status: {}.", status );

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
     * @param traceCountEstimator a function that estimates trace count, in order to help with monitoring
     * @return a function that consumes a pool and produces one blob of statistics for each processor
     */

    private Function<Pool<TimeSeries<Pair<L, R>>>, List<StatisticsStore>>
    getStatisticsProcessingTask( List<StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>>> processors,
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
            for ( StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>> processor : processors )
            {
                StatisticsStore nextStatistics = this.getStatistics( processor, pool );
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
     * @return whether separate metrics are required for the baseline
     */
    private boolean hasSeparateMetricsForBaseline()
    {
        return this.separateMetrics;
    }

    /**
     * Returns the statistics from a processor and pool.
     * @param processor the metrics processor
     * @param pool the pool
     * @return the statistics
     */

    private StatisticsStore getStatistics( StatisticsProcessor<Pool<TimeSeries<Pair<L, R>>>> processor,
                                           Pool<TimeSeries<Pair<L, R>>> pool )
    {
        StatisticsStore statistics = processor.apply( pool );

        // Compute separate statistics for the baseline?
        if ( pool.hasBaseline() )
        {
            Pool<TimeSeries<Pair<L, R>>> baseline = pool.getBaselineData();

            if ( this.hasSeparateMetricsForBaseline() )
            {
                LOGGER.debug( "Computing separate statistics for the baseline pairs associated with pool {}.",
                              baseline.getMetadata() );

                StatisticsStore baselineStatistics = processor.apply( baseline );
                statistics = statistics.combine( baselineStatistics );
            }
        }

        return statistics;
    }

    /**
     * Build a pool processor. 
     *
     * @param builder the builder
     * @throws NullPointerException if any required input is null
     */

    private PoolProcessor( Builder<L, R> builder )
    {
        this.pairsWriter = builder.pairsWriter;
        this.basePairsWriter = builder.basePairsWriter;
        this.evaluation = builder.evaluation;
        this.monitor = builder.monitor;
        this.poolSupplier = builder.poolSupplier;
        this.poolRequest = builder.poolRequest;
        this.metricProcessors = List.copyOf( builder.metricProcessors ); // Validates nullity
        this.samplingUncertaintyMetricProcessors = builder.samplingUncertaintyMetricProcessors;
        this.traceCountEstimator = builder.traceCountEstimator;
        this.poolGroupTracker = builder.poolGroupTracker;
        this.separateMetrics = builder.separateMetrics;
        this.samplingUncertainty = builder.samplingUncertainty;
        this.samplingUncertaintyExecutor = builder.samplingUncertaintyExecutor;
        this.blockSize = builder.blockSize;

        Objects.requireNonNull( this.evaluation );
        Objects.requireNonNull( this.monitor );
        Objects.requireNonNull( this.poolSupplier );
        Objects.requireNonNull( this.poolRequest );
        Objects.requireNonNull( this.traceCountEstimator );
        Objects.requireNonNull( this.poolGroupTracker );

        if ( Objects.nonNull( this.samplingUncertainty ) )
        {
            Objects.requireNonNull( this.samplingUncertaintyMetricProcessors );
            Objects.requireNonNull( this.samplingUncertaintyExecutor );
            Objects.requireNonNull( this.blockSize );
        }

        // Set the message group identifier
        this.messageGroupId = poolGroupTracker.getGroupId( this.poolRequest );

        LOGGER.debug( "Created a PoolProcessor that belongs to message group {}. The pool request is: {}.",
                      this.messageGroupId,
                      this.poolRequest );
    }

}
