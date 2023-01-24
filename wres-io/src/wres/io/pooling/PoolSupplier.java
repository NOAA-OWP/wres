package wres.io.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.Climatology;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.pools.pairs.PairingException;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureCorrelator;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.config.ConfigHelper;
import wres.io.pooling.RescalingEvent.RescalingType;
import wres.io.retrieval.DataAccessException;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.GeometryGroup;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;

/**
 * <p>Supplies a {@link Pool}, which is used to compute one or more verification statistics. The overall 
 * responsibility of the {@link PoolSupplier} is to supply a {@link Pool} on request. This is fulfilled by completing 
 * several smaller activities in sequence, namely:</p> 
 * 
 * <ol>
 * <li>Consuming the (possibly pool-shaped) left/right/baseline data for pairing, which is supplied by retrievers;</li>
 * <li>Rescaling the data, where needed, so that pairs can be formed at the desired time scale;</li>
 * <li>Creating pairs;</li>
 * <li>Trimming pairs to the pool boundaries; and</li>
 * <li>Supplying the pool-shaped pairs.</li>
 * </ol>
 * 
 * <p><b>Implementation notes:</b></p>
 * 
 * <p>This class is thread safe. A pool supplier cannot be re-used, it is "one and done".
 * 
 * @author James Brown
 * @param <L> the type of left value in each pair
 * @param <R> the type of right value in each pair and, where applicable, the type of baseline value
 */

@ThreadSafe
public class PoolSupplier<L, R> implements Supplier<Pool<TimeSeries<Pair<L, R>>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolSupplier.class );

    /** Message re-used several times. */
    private static final String WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR = "While constructing a pool supplier for {}, ";

    /** The climatology, possibly null. */
    private final Supplier<Climatology> climatology;

    /** Left data source. */
    private final Supplier<Stream<TimeSeries<L>>> left;

    /** Right data source. */
    private final Supplier<Stream<TimeSeries<R>>> right;

    /** Baseline data source. Optional. */
    private final Supplier<Stream<TimeSeries<R>>> baseline;

    /** Generator for baseline data source. Optional. */
    private final Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> baselineGenerator;

    /** Pairer. */
    private final TimeSeriesPairer<L, R> pairer;

    /** An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time. */
    private final TimeSeriesCrossPairer<L, R> crossPairer;

    /** Upscaler for left-type data. Optional on construction, but may be exceptional if absent and later required. */
    private final TimeSeriesUpscaler<L> leftUpscaler;

    /** Upscaler for right-type data. Optional on construction, but may be exceptional if later required. */
    private final TimeSeriesUpscaler<R> rightUpscaler;

    /** Upscaler for baseline-type data. Optional on construction, but may be exceptional if later required. */
    private final TimeSeriesUpscaler<R> baselineUpscaler;

    /** The desired time scale. */
    private final TimeScaleOuter desiredTimeScale;

    /** Metadata for the mains pairs. */
    private final PoolMetadata metadata;

    /** Metadata for the baseline pairs. */
    private final PoolMetadata baselineMetadata;

    /** The declaration. */
    private final ProjectConfig projectConfig;

    /** A transformer for left-ish values. */
    private final UnaryOperator<TimeSeries<L>> leftTransformer;

    /** A transformer for right-ish values that can take into account the event context. */
    private final UnaryOperator<TimeSeries<R>> rightTransformer;

    /** A transformer for baseline-ish values that can take into account the event context. */
    private final UnaryOperator<TimeSeries<R>> baselineTransformer;

    /** A function that filters left-ish time-series. */
    private final Predicate<TimeSeries<L>> leftFilter;

    /** A function that filters right-ish time-series. */
    private final Predicate<TimeSeries<R>> rightFilter;

    /** A function that filters baseline-ish time-series. */
    private final Predicate<TimeSeries<R>> baselineFilter;

    /** An offset to apply to the valid times of the left data. */
    private final Duration leftOffset;

    /** An offset to apply to the valid times of the right data. */
    private final Duration rightOffset;

    /** An offset to apply to the valid times of the baseline data. */
    private final Duration baselineOffset;

    /** Frequency with which pairs should be constructed at the desired time scale. */
    private final Duration frequency;

    /** The feature correlator. **/
    private final FeatureCorrelator featureCorrelator;

    /** Set of baseline features for baseline generation, possibly empty. */
    private final Set<Feature> baselineFeatures;

    /** Has the supplier been called before? */
    private final AtomicBoolean done = new AtomicBoolean( false );

    /**
     * Returns a {@link Pool} for metric calculation.
     * 
     * @return a pool of pairs
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     * @throws PoolCreationException if the supplier is called more than once
     */

    @Override
    public Pool<TimeSeries<Pair<L, R>>> get()
    {
        if ( !this.done.getAndSet( true ) )
        {
            return this.createPool();
        }

        throw new PoolCreationException( "Attempted to call a pool supplier more than once, which is not allowed. The "
                                         + "repeated call was made to the supplier of pool: "
                                         + this.getMetadata()
                                         + "." );
    }

    /**
     * Creates a {@link Pool}.
     * 
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     * @return the pool
     */

    private Pool<TimeSeries<Pair<L, R>>> createPool()
    {
        LOGGER.debug( "Creating pool {}.", this.getMetadata() );
        PoolCreationEvent poolMonitor = PoolCreationEvent.of( this.getMetadata() ); // Monitor
        poolMonitor.begin();

        // Left data provided or is climatology the left data?
        Stream<TimeSeries<L>> leftData = this.left.get();

        LOGGER.debug( "Preparing to retrieve time-series data." );

        Stream<TimeSeries<R>> rightData = this.right.get();
        Stream<TimeSeries<R>> baselineData = null;

        // Baseline that is not generated?
        if ( this.hasBaseline() && !this.hasBaselineGenerator() )
        {
            baselineData = this.baseline.get();
        }

        Pool<TimeSeries<Pair<L, R>>> returnMe = null;

        // In this context, a stream can mean an open connection to a resource, such as a database, so it is essential 
        // that each stream is closed on completion. A well-behaved stream will then close any resources on which it 
        // depends.
        Stream<TimeSeries<R>> finalBaselineData = baselineData;
        try ( leftData; rightData; finalBaselineData )
        {
            returnMe = this.createPool( leftData, rightData, baselineData );
        }

        poolMonitor.commit();

        if ( LOGGER.isDebugEnabled() )
        {
            int pairCount = PoolSlicer.getPairCount( returnMe );

            LOGGER.debug( "Finished creating pool {}, which contains {} time-series and {} pairs.",
                          this.getMetadata(),
                          returnMe.get().size(),
                          pairCount );
        }

        return returnMe;
    }

    /**
     * Builder for a {@link PoolSupplier}.
     * 
     * @author James Brown
     * @param <L> the left type of paired value
     * @param <R> the right type of paired value and, where required, the baseline type
     */

    static class Builder<L, R>
    {
        /** The climatology. */
        private Supplier<Climatology> climatology;

        /** Left data source. */
        private Supplier<Stream<TimeSeries<L>>> left;

        /** Right data source. */
        private Supplier<Stream<TimeSeries<R>>> right;

        /** Baseline data source. Optional. */
        private Supplier<Stream<TimeSeries<R>>> baseline;

        /** Generator for baseline data source. Optional. */
        private Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> baselineGenerator;

        /** Pairer. */
        private TimeSeriesPairer<L, R> pairer;

        /** Upscaler for left-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<L> leftUpscaler;

        /** Upscaler for right-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<R> rightUpscaler;

        /** Upscaler for baseline-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<R> baselineUpscaler;

        /** The desired time scale. */
        private TimeScaleOuter desiredTimeScale;

        /** Project declaration. */
        private ProjectConfig projectConfig;

        /** Metadata for the mains pairs. */
        private PoolMetadata metadata;

        /** Metadata for the baseline pairs. */
        private PoolMetadata baselineMetadata;

        /** A function that transforms the left-ish data. */
        private UnaryOperator<TimeSeries<L>> leftTransformer = in -> in;

        /** A function that transforms the right-ish data. */
        private UnaryOperator<TimeSeries<R>> rightTransformer = in -> in;

        /** A function that transforms baseline-ish data. */
        private UnaryOperator<TimeSeries<R>> baselineTransformer = in -> in;

        /** A function that filters left-ish time-series. */
        private Predicate<TimeSeries<L>> leftFilter = in -> true;

        /** A function that filters right-ish time-series. */
        private Predicate<TimeSeries<R>> rightFilter = in -> true;

        /** A function that filters baseline-ish time-series. */
        private Predicate<TimeSeries<R>> baselineFilter = in -> true;

        /** The frequency at which pairs should be produced. */
        private Duration frequency;

        /** An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time. */
        private TimeSeriesCrossPairer<L, R> crossPairer;

        /**
         * @param climatology the climatology to set
         * @return the builder
         */
        Builder<L, R> setClimatology( Supplier<Climatology> climatology )
        {
            this.climatology = climatology;

            return this;
        }

        /**
         * @param left the left to set
         * @return the builder
         */
        Builder<L, R> setLeft( Supplier<Stream<TimeSeries<L>>> left )
        {
            this.left = left;

            return this;
        }

        /**
         * @param right the right to set
         * @return the builder
         */
        Builder<L, R> setRight( Supplier<Stream<TimeSeries<R>>> right )
        {
            this.right = right;

            return this;
        }

        /**
         * @param baseline the baseline to set
         * @return the builder
         */
        Builder<L, R> setBaseline( Supplier<Stream<TimeSeries<R>>> baseline )
        {
            this.baseline = baseline;

            return this;
        }

        /**
         * @param baselineGenerator a baseline generator to set
         * @return the builder
         */
        Builder<L, R> setBaselineGenerator( Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * @param pairer the pairer to set
         * @return the builder
         */
        Builder<L, R> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param crossPairer the cross-pairer to set
         * @return the builder
         */
        Builder<L, R> setCrossPairer( TimeSeriesCrossPairer<L, R> crossPairer )
        {
            this.crossPairer = crossPairer;

            return this;
        }

        /**
         * @param leftUpscaler the leftUpscaler to set
         * @return the builder
         */
        Builder<L, R> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the rightUpscaler to set
         * @return the builder
         */
        Builder<L, R> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param baselineUpscaler the upscaler for baseline values
         * @return the builder
         */
        Builder<L, R> setBaselineUpscaler( TimeSeriesUpscaler<R> baselineUpscaler )
        {
            this.baselineUpscaler = baselineUpscaler;

            return this;
        }

        /**
         * @param desiredTimeScale the desiredTimeScale to set
         * @return the builder
         */
        Builder<L, R> setDesiredTimeScale( TimeScaleOuter desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;

            return this;
        }

        /**
         * @param metadata the metadata to set
         * @return the builder
         */
        Builder<L, R> setMetadata( PoolMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

        /**
         * @param baselineMetadata the baselineMetadata to set
         * @return the builder
         */
        Builder<L, R> setBaselineMetadata( PoolMetadata baselineMetadata )
        {
            this.baselineMetadata = baselineMetadata;

            return this;
        }

        /**
         * @param projectConfig the project declaration to set
         * @return the builder
         */
        Builder<L, R> setProjectDeclaration( ProjectConfig projectConfig )
        {
            this.projectConfig = projectConfig;

            return this;
        }

        /**
         * @param leftTransformer the transformer for left-style data
         * @return the builder
         */
        Builder<L, R> setLeftTransformer( UnaryOperator<TimeSeries<L>> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the transformer for right-style data
         * @return the builder
         */
        Builder<L, R> setRightTransformer( UnaryOperator<TimeSeries<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the transformer for baseline-style data
         * @return the builder
         */
        Builder<L, R> setBaselineTransformer( UnaryOperator<TimeSeries<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

            return this;
        }

        /**
         * @param leftFilter the filter for left-style data
         * @return the builder
         */
        Builder<L, R> setLeftFilter( Predicate<TimeSeries<L>> leftFilter )
        {
            this.leftFilter = leftFilter;

            return this;
        }

        /**
         * @param rightFilter the filter for right-style data
         * @return the builder
         */
        Builder<L, R> setRightFilter( Predicate<TimeSeries<R>> rightFilter )
        {
            this.rightFilter = rightFilter;

            return this;
        }

        /**
         * @param baselineFilter the filter for baseline-style data
         * @return the builder
         */
        Builder<L, R> setBaselineFilter( Predicate<TimeSeries<R>> baselineFilter )
        {
            this.baselineFilter = baselineFilter;

            return this;
        }

        /**
         * @param frequency the frequency at which pairs should be produced.
         * @return the builder
         */
        Builder<L, R> setFrequencyOfPairs( Duration frequency )
        {
            this.frequency = frequency;

            return this;
        }

        /**
         * Builds a {@link PoolSupplier}.
         * 
         * @return a pool supplier
         */

        PoolSupplier<L, R> build()
        {
            return new PoolSupplier<>( this );
        }
    }

    /**
     * Create a pool.
     * @param leftData the left time-series
     * @param rightData the right time-series
     * @param baselineData the baseline time-series
     * @return the pool
     * @throws NullPointerException if any input is null
     */

    private Pool<TimeSeries<Pair<L, R>>> createPool( Stream<TimeSeries<L>> leftData,
                                                     Stream<TimeSeries<R>> rightData,
                                                     Stream<TimeSeries<R>> baselineData )
    {
        LOGGER.debug( "Creating a pool." );

        Objects.requireNonNull( leftData,
                                "Left data is expected for the creation of pool " + this.getMetadata() + "." );
        Objects.requireNonNull( rightData,
                                "Right data is expected for the creation of pool " + this.getMetadata() + "." );

        // Obtain the desired time scale
        TimeScaleOuter desiredTimeScaleToUse = this.getDesiredTimeScale();

        // Get the paired frequency
        Duration pairedFrequency = this.getPairedFrequency();

        // Pooling is organized to minimize memory usage for right-ish datasets, which tend to be larger (e.g., 
        // forecasts). Thus, each right-ish series is transformed per series in a later loop. However, the left-ish 
        // series are brought into memory, so transform them now. See #95488.        
        // Apply any valid time offset to the left-ish data upfront.
        Duration leftValidOffset = this.getValidTimeOffset( LeftOrRightOrBaseline.LEFT );
        List<TimeSeries<L>> transformedLeft = leftData.map( nextSeries -> this.applyValidTimeOffset( nextSeries,
                                                                                                     leftValidOffset,
                                                                                                     LeftOrRightOrBaseline.LEFT ) )
                                                      .toList();

        List<EvaluationStatusMessage> validationEvents = new ArrayList<>();

        // Calculate the main pairs
        TimeSeriesPlusValidation<L, R> mainPairsPlus = this.createPairsPerFeature( transformedLeft.stream(),
                                                                                   rightData,
                                                                                   desiredTimeScaleToUse,
                                                                                   pairedFrequency,
                                                                                   LeftOrRightOrBaseline.RIGHT,
                                                                                   this.getMetadata()
                                                                                       .getTimeWindow() );

        validationEvents.addAll( mainPairsPlus.getEvaluationStatusMessages() );

        // Main pairs indexed by feature tuple
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs = mainPairsPlus.getTimeSeries();

        // Baseline pairs indexed by feature tuple
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs = null;

        // Create the baseline pairs
        if ( this.hasBaseline() )
        {
            Stream<TimeSeries<R>> baselineDataInner = baselineData;

            // Baseline that is generated?
            if ( this.hasBaselineGenerator() )
            {
                basePairs = mainPairsPlus.getGeneratedBaselineTimeSeries();
            }
            // Regular baseline with supplied time-series
            else
            {
                TimeSeriesPlusValidation<L, R> basePairsPlus =
                        this.createPairsPerFeature( transformedLeft.stream(),
                                                    baselineDataInner,
                                                    desiredTimeScaleToUse,
                                                    pairedFrequency,
                                                    LeftOrRightOrBaseline.BASELINE,
                                                    this.baselineMetadata.getTimeWindow() );

                validationEvents.addAll( basePairsPlus.getEvaluationStatusMessages() );
                basePairs = basePairsPlus.getTimeSeries();
            }

            // Cross-pair the main and baseline pairs?
            if ( Objects.nonNull( this.crossPairer ) )
            {
                Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>> cp =
                        this.getCrossPairs( mainPairs, basePairs );

                mainPairs = cp.getLeft();
                basePairs = cp.getRight();
            }
        }

        // Create the pool from the paired time-series
        Pool<TimeSeries<Pair<L, R>>> pool = this.createPoolFromPairs( mainPairs,
                                                                      basePairs,
                                                                      desiredTimeScaleToUse,
                                                                      validationEvents );

        if ( LOGGER.isDebugEnabled() )
        {
            int pairCount = PoolSlicer.getPairCount( pool );

            LOGGER.debug( "Finished creating pool, which contains {} time-series and {} pairs "
                          + "and has this metadata: {}.",
                          pool.get().size(),
                          pairCount,
                          this.getMetadata() );
        }

        return pool;
    }

    /**
     * @param mainPairs the main pairs indexed by feature tuple
     * @param basePairs the baseline pairs, optional, indexed by feature tuple
     * @param desiredTimeScale the desired time scale, optional
     * @param statusMessages and evaluation status messages
     * @return the pool of pairs
     */

    private Pool<TimeSeries<Pair<L, R>>> createPoolFromPairs( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs,
                                                              Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs,
                                                              TimeScaleOuter desiredTimeScale,
                                                              List<EvaluationStatusMessage> statusMessages )
    {
        Objects.requireNonNull( mainPairs );
        Objects.requireNonNull( statusMessages );

        // No data? Then build an empty mini-pool for each feature, as needed
        mainPairs = this.getExistingPoolsOrEmpty( mainPairs, this.getMetadata() );
        basePairs = this.getExistingPoolsOrEmpty( basePairs, this.getBaselineMetadata() );

        Pool.Builder<TimeSeries<Pair<L, R>>> builder = new Pool.Builder<>();

        // Create and set the climatology#
        Climatology nextClimatology = null;
        if ( this.hasClimatology() )
        {
            nextClimatology = this.getClimatology()
                                  .get();
            builder.setClimatology( nextClimatology );
        }

        // Create the mini pools, one per feature
        // TODO: this assumes that an empty main means empty baseline too. Probably need to relax
        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();

            // Pairs can be empty but not null
            if ( Objects.isNull( nextMainPairs ) )
            {
                Set<Feature> availableFeatures = mainPairs.keySet()
                                                          .stream()
                                                          .map( FeatureTuple::getRight )
                                                          .collect( Collectors.toSet() );

                throw new PoolException( "Failed to identify any pairs for feature: " + nextFeature
                                         + ". The available features were: "
                                         + availableFeatures
                                         + "." );
            }

            Pool.Builder<TimeSeries<Pair<L, R>>> nextMiniPoolBuilder = new Pool.Builder<>();

            PoolMetadata nextInnerMainMeta = this.getAdjustedMetadata( this.getMetadata(),
                                                                       nextFeature,
                                                                       desiredTimeScale );

            nextMiniPoolBuilder.setMetadata( nextInnerMainMeta )
                               .addData( nextMainPairs );

            // Set the baseline
            if ( Objects.nonNull( basePairs ) )
            {
                List<TimeSeries<Pair<L, R>>> nextBasePairs = basePairs.get( nextFeature );

                // Pairs can be empty but not null
                if ( Objects.isNull( nextBasePairs ) )
                {
                    Set<Feature> availableFeatures = basePairs.keySet()
                                                              .stream()
                                                              .map( FeatureTuple::getBaseline )
                                                              .collect( Collectors.toSet() );

                    Set<Feature> availableMainFeatures = mainPairs.keySet()
                                                                  .stream()
                                                                  .map( FeatureTuple::getRight )
                                                                  .collect( Collectors.toSet() );

                    throw new PoolException( "Failed to identify any baseline pairs for feature: "
                                             + nextFeature.getBaselineName()
                                             + ". The available features for the baseline pairs were: "
                                             + availableFeatures
                                             + ". The available features for the main pairs were: "
                                             + availableMainFeatures
                                             + "." );
                }

                PoolMetadata nextInnerBaseMeta = this.getAdjustedMetadata( this.getBaselineMetadata(),
                                                                           nextFeature,
                                                                           desiredTimeScale );

                nextMiniPoolBuilder.setMetadataForBaseline( nextInnerBaseMeta )
                                   .addDataForBaseline( nextBasePairs );
            }

            nextMiniPoolBuilder.setClimatology( nextClimatology );

            Pool<TimeSeries<Pair<L, R>>> nextMiniPool = nextMiniPoolBuilder.build();
            builder.addPool( nextMiniPool );
        }

        // Set the metadata, adjusted to include the desired time scale
        wres.statistics.generated.Pool.Builder newMetadataBuilder =
                this.getMetadata()
                    .getPool()
                    .toBuilder();

        if ( Objects.nonNull( desiredTimeScale ) )
        {
            newMetadataBuilder.setTimeScale( desiredTimeScale.getTimeScale() );
        }

        PoolMetadata mainMetadata = PoolMetadata.of( this.getMetadata()
                                                         .getEvaluation(),
                                                     newMetadataBuilder.build(),
                                                     statusMessages );

        builder.setMetadata( mainMetadata );

        if ( this.hasBaseline() )
        {
            wres.statistics.generated.Pool.Builder newBaseMetadataBuilder =
                    this.getBaselineMetadata()
                        .getPool()
                        .toBuilder();
            if ( Objects.nonNull( desiredTimeScale ) )
            {
                newBaseMetadataBuilder.setTimeScale( desiredTimeScale.getTimeScale() );
            }

            PoolMetadata baseMetadata = PoolMetadata.of( this.getMetadata()
                                                             .getEvaluation(),
                                                         newBaseMetadataBuilder.build() );
            builder.setMetadataForBaseline( baseMetadata );
        }

        return builder.build();
    }

    /**
     * Adjusts the input metadata to represent the supplied feature and time scale.
     * @param metadata the input metadata
     * @param feature the feature to use when adjusting the input metadata
     * @param timeScale the time scale
     * @return the adjusted metadata
     */

    private PoolMetadata getAdjustedMetadata( PoolMetadata metadata,
                                              FeatureTuple feature,
                                              TimeScaleOuter timeScale )
    {
        GeometryGroup.Builder geoGroupBuilder = GeometryGroup.newBuilder()
                                                             .addGeometryTuples( feature.getGeometryTuple() )
                                                             .setRegionName( metadata.getFeatureGroup()
                                                                                     .getName() );

        wres.statistics.generated.Pool.Builder builder =
                metadata.getPool()
                        .toBuilder()
                        .clearGeometryTuples()
                        .addGeometryTuples( feature.getGeometryTuple() )
                        .setGeometryGroup( geoGroupBuilder );

        if ( Objects.nonNull( timeScale ) )
        {
            builder.setTimeScale( timeScale.getTimeScale() );
        }

        return PoolMetadata.of( metadata.getEvaluation(),
                                builder.build() );
    }

    /**
     * Returns the existing pools or, if none are present in the input, an empty pool for each feature in the metadata.
     * @param pools the pools
     * @param metadata the pool metadata
     * @return the existing pools or empty pools
     */

    private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>
            getExistingPoolsOrEmpty( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> pools,
                                     PoolMetadata metadata )
    {
        if ( Objects.nonNull( pools ) && pools.isEmpty() )
        {
            LOGGER.debug( "Discovered no pairs. Filling with empty pools." );

            Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> emptyPools = new HashMap<>();
            metadata.getFeatureTuples()
                    .forEach( nextFeature -> emptyPools.put( nextFeature, List.of() ) );

            return emptyPools;
        }

        return pools;
    }

    /**
     * Creates a paired dataset from the input, rescaling the data as needed.
     * 
     * @param left the left data, required
     * @param rightOrBaseline the right or baseline data, required
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency with which to create pairs at the desired time scale
     * @param rightOrBaselineOrientation the orientation of the non-left data, one of 
     *            {@link LeftOrRightOrBaseline#RIGHT} or {@link LeftOrRightOrBaseline#BASELINE}, required
     * @param timeWindow the time window to snip the pairs
     * @return the pairs
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     * @throws NullPointerException if any required input is null
     */

    private TimeSeriesPlusValidation<L, R> createPairsPerFeature( Stream<TimeSeries<L>> left,
                                                                  Stream<TimeSeries<R>> rightOrBaseline,
                                                                  TimeScaleOuter desiredTimeScale,
                                                                  Duration frequency,
                                                                  LeftOrRightOrBaseline rightOrBaselineOrientation,
                                                                  TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( rightOrBaseline );
        Objects.requireNonNull( rightOrBaselineOrientation );

        UnaryOperator<TimeSeries<R>> rightOrBaselineTransformer =
                this.getRightOrBaselineTransformer( rightOrBaselineOrientation );
        Duration rightOrBaselineValidOffset = this.getValidTimeOffset( rightOrBaselineOrientation );

        Predicate<TimeSeries<L>> leftFilterInner = this.getLeftFilter();
        Predicate<TimeSeries<R>> rightOrBaselineFilter = this.getRightOrBaselineFilter( rightOrBaselineOrientation );

        List<EvaluationStatusMessage> validation = new ArrayList<>();

        // The following loop is organized to minimize memory usage for right-ish datasets, which tend to be larger 
        // (e.g., forecasts). It holds in memory the unscaled and untransformed left-ish datasets for proper pairing of
        // all right-ish datasets, but streams the right-ish datasets and holds only one in memory at any given time.
        // See #95488.

        // Unscaled left-ish series for re-use across right-ish series, mapped by feature key. Apply any valid time
        // offset here
        Map<Feature, List<TimeSeries<L>>> leftSeries =
                left.filter( leftFilterInner )
                    .collect( Collectors.groupingBy( next -> next.getMetadata()
                                                                 .getFeature() ) );

        int rightOrBaselineSeriesCount = 0;

        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> pairsPerFeature = new HashMap<>();

        // Generated baseline, if needed
        // Get the baseline generator for all baseline features. Call with respect to a fixed feature set, defined on 
        // construction since consistent calls can be optimized/cached more easily (e.g., cached retrieval)
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaseline = new HashMap<>();
        UnaryOperator<TimeSeries<R>> baselineGeneratorFunction = null;

        if ( this.hasBaselineGenerator() )
        {
            Set<Feature> allBaselineFeatures = this.getBaselineFeatures();
            Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> generatorSupplier = this.getBaselineGenerator();
            baselineGeneratorFunction = generatorSupplier.apply( allBaselineFeatures );
        }

        Transformers<R> rightTransformers = new Transformers<>( rightOrBaselineTransformer,
                                                                baselineGeneratorFunction );

        Iterator<TimeSeries<R>> rightOrBaselineIterator = rightOrBaseline.iterator();

        // Loop over the right or baseline series
        while ( rightOrBaselineIterator.hasNext() )
        {
            TimeSeries<R> nextRightOrBaselineSeries = rightOrBaselineIterator.next();

            // Meets the filter?
            if ( !rightOrBaselineFilter.test( nextRightOrBaselineSeries ) )
            {
                LOGGER.debug( "Ignoring time-series {} because it was not selected by a filter.",
                              nextRightOrBaselineSeries.getMetadata() );
                continue;
            }

            TimeSeries<R> transformedRightOrBaseline = this.applyValidTimeOffset( nextRightOrBaselineSeries,
                                                                                  rightOrBaselineValidOffset,
                                                                                  rightOrBaselineOrientation );

            Feature nextRightOrBaselineFeature = transformedRightOrBaseline.getMetadata()
                                                                           .getFeature();

            Set<Feature> nextLeftFeatures = this.getLeftFeaturesForRightOrBaseline( nextRightOrBaselineFeature,
                                                                                    rightOrBaselineOrientation );

            // Add the pairs for each left feature
            for ( Feature nextLeftFeature : nextLeftFeatures )
            {
                List<TimeSeries<L>> nextLeftSeries = leftSeries.get( nextLeftFeature );

                List<TimeSeriesPlusValidation<L, R>> nextPairedSeries =
                        this.createPairsPerLeftSeries( nextLeftSeries,
                                                       transformedRightOrBaseline,
                                                       desiredTimeScale,
                                                       frequency,
                                                       rightOrBaselineOrientation,
                                                       timeWindow,
                                                       rightTransformers );

                this.addPairsForFeature( pairsPerFeature,
                                         nextPairedSeries,
                                         TimeSeriesPlusValidation::getTimeSeries,
                                         validation );

                // Bubble up the rescaled/filtered right time-series for baseline generation, if needed
                if ( this.hasBaselineGenerator() )
                {
                    this.addPairsForFeature( generatedBaseline,
                                             nextPairedSeries,
                                             TimeSeriesPlusValidation::getGeneratedBaselineTimeSeries,
                                             validation );
                }
            }

            rightOrBaselineSeriesCount++;
        }

        // Log the number of time-series available for pairing and the number of paired time-series created
        if ( LOGGER.isDebugEnabled() )
        {
            PoolMetadata metaToReport = this.getMetadata();

            if ( rightOrBaselineOrientation == LeftOrRightOrBaseline.BASELINE )
            {
                metaToReport = this.baselineMetadata;
            }

            LOGGER.debug( "While creating pool {}, discovered {} {} time-series and {} {} time-series from "
                          + "which to create pairs. Created {} paired time-series from these inputs.",
                          metaToReport,
                          leftSeries.size(),
                          LeftOrRightOrBaseline.LEFT,
                          rightOrBaselineSeriesCount,
                          rightOrBaselineOrientation,
                          pairsPerFeature.size() );
        }

        return new TimeSeriesPlusValidation<>( pairsPerFeature, generatedBaseline, validation );
    }

    /**
     * Adds the pairs for the next left and right/baseline feature.
     * 
     * @param cache the cache to update
     * @param pairsToCache the next pairs to cache
     * @param pairGetter a function that extracts the required pairs from the pairsToCache
     * @param validation the validation to update
     */

    private void addPairsForFeature( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> cache,
                                     List<TimeSeriesPlusValidation<L, R>> pairsToCache,
                                     Function<TimeSeriesPlusValidation<L, R>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>> pairGetter,
                                     List<EvaluationStatusMessage> validation )
    {
        // Add to the results
        for ( TimeSeriesPlusValidation<L, R> nextSeries : pairsToCache )
        {
            List<EvaluationStatusMessage> statusEvents = nextSeries.getEvaluationStatusMessages();
            validation.addAll( statusEvents );

            Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> pairs = pairGetter.apply( nextSeries );

            // Transpose to the overall cache contained in the pairsPerFeature
            for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextPairedSeries : pairs.entrySet() )
            {
                FeatureTuple nextFeature = nextPairedSeries.getKey();
                List<TimeSeries<Pair<L, R>>> nextPairsList = nextPairedSeries.getValue();

                // Strip any empty series
                nextPairsList = nextPairsList.stream()
                                             .filter( next -> !next.getEvents().isEmpty() )
                                             .toList();

                List<TimeSeries<Pair<L, R>>> nextList = cache.get( nextFeature );
                if ( Objects.isNull( nextList ) )
                {
                    nextList = new ArrayList<>();
                    cache.put( nextFeature, nextList );
                }

                nextList.addAll( nextPairsList );
            }
        }
    }

    /**
     * Creates a time-series of pairs for each left series.
     * @param leftSeries the left time-series, which may be null
     * @param rightOrBaselineSeries the right time-series
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency of the pairs
     * @param rightOrBaselineOrientation the orientation of the pairs
     * @param timeWindow the time window
     * @param rightTransformers the transformers for right-ish data
     * @return the pairs plus validation
     * @throws NullPointerException if any required input is null
     */

    private List<TimeSeriesPlusValidation<L, R>> createPairsPerLeftSeries( List<TimeSeries<L>> leftSeries,
                                                                           TimeSeries<R> rightOrBaselineSeries,
                                                                           TimeScaleOuter desiredTimeScale,
                                                                           Duration frequency,
                                                                           LeftOrRightOrBaseline rightOrBaselineOrientation,
                                                                           TimeWindowOuter timeWindow,
                                                                           Transformers<R> rightTransformers )
    {
        Objects.requireNonNull( rightOrBaselineSeries );
        Objects.requireNonNull( rightOrBaselineOrientation );

        if ( Objects.isNull( leftSeries ) )
        {
            LOGGER.debug( "Unable to find any left time-series to pair with a right time-series whose metadata was {}.",
                          rightOrBaselineSeries.getMetadata() );

            return Collections.emptyList();
        }

        List<TimeSeriesPlusValidation<L, R>> returnMe = new ArrayList<>();

        for ( TimeSeries<L> nextLeftSeries : leftSeries )
        {
            TimeSeriesPlusValidation<L, R> pairsPlus = this.createSeriesPairs( nextLeftSeries,
                                                                               rightOrBaselineSeries,
                                                                               desiredTimeScale,
                                                                               frequency,
                                                                               timeWindow,
                                                                               rightOrBaselineOrientation,
                                                                               rightTransformers );

            returnMe.add( pairsPlus );

            if ( LOGGER.isTraceEnabled() )
            {
                // One time-series of pairs, by definition
                TimeSeries<Pair<L, R>> pairs = pairsPlus.getTimeSeries()
                                                        .values()
                                                        .iterator()
                                                        .next()
                                                        .get( 0 );

                if ( pairs.getEvents().isEmpty() )
                {
                    LOGGER.trace( "Found zero pairs while intersecting time-series {} with time-series {}.",
                                  nextLeftSeries.getMetadata(),
                                  rightOrBaselineSeries.getMetadata() );
                }
            }
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns a time-series of pairs from a left and right or baseline series, rescaling as needed.
     * 
     * @param left the left time-series
     * @param rightOrBaseline the right or baseline time-series
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency with which to create pairs at the desired time scale
     * @param timeWindow the time window to snip the pairs
     * @param orientation the orientation of the non-left data, one of {@link LeftOrRightOrBaseline#RIGHT} or 
     *            {@link LeftOrRightOrBaseline#BASELINE}
     * @param rightTransformers the right-ish transformers
     * @return a paired time-series
     * @throws NullPointerException if the left, rightOrBaseline or timeWindow is null
     */

    private TimeSeriesPlusValidation<L, R> createSeriesPairs( TimeSeries<L> left,
                                                              TimeSeries<R> rightOrBaseline,
                                                              TimeScaleOuter desiredTimeScale,
                                                              Duration frequency,
                                                              TimeWindowOuter timeWindow,
                                                              LeftOrRightOrBaseline orientation,
                                                              Transformers<R> rightTransformers )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( rightOrBaseline );
        Objects.requireNonNull( timeWindow );

        // Desired unit
        String desiredUnit = this.getMetadata()
                                 .getMeasurementUnit()
                                 .getUnit();

        // Snip the left data to the right with a buffer on the lower bound, if required. This greatly improves
        // performance for a very long left series, which is quite typical
        Duration period = this.getPeriodFromTimeScale( desiredTimeScale );

        TimeSeries<L> scaledLeft = TimeSeriesSlicer.snip( left, rightOrBaseline, period, Duration.ZERO );

        TimeSeries<R> scaledRight = rightOrBaseline;
        boolean upscaleLeft = Objects.nonNull( desiredTimeScale ) && !desiredTimeScale.equals( left.getTimeScale() );
        boolean upscaleRight = Objects.nonNull( desiredTimeScale )
                               && !desiredTimeScale.equals( rightOrBaseline.getTimeScale() );

        List<EvaluationStatusMessage> statusEvents = new ArrayList<>();

        // Get the end times for paired values if upscaling is required. If upscaling both the left and right, the 
        // superset of intersecting times is thinned to a regular sequence that depends on the desired time scale and 
        // frequency
        SortedSet<Instant> endsAt = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( scaledLeft,
                                                                                            scaledRight,
                                                                                            timeWindow,
                                                                                            desiredTimeScale,
                                                                                            frequency );

        // Upscale left?
        if ( upscaleLeft )
        {
            RescalingEvent rescalingMonitor = RescalingEvent.of( RescalingType.UPSCALED, // Monitor
                                                                 LeftOrRightOrBaseline.LEFT,
                                                                 left.getMetadata() );

            rescalingMonitor.begin();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Upscaling {} time-series {} from {} to {}.",
                              LeftOrRightOrBaseline.LEFT,
                              left.hashCode(),
                              left.getTimeScale(),
                              desiredTimeScale );
            }

            TimeSeriesUpscaler<L> leftUp = this.getLeftUpscaler();
            // #92892
            RescaledTimeSeriesPlusValidation<L> upscaledLeft = leftUp.upscale( scaledLeft,
                                                                               desiredTimeScale,
                                                                               endsAt,
                                                                               desiredUnit );

            scaledLeft = upscaledLeft.getTimeSeries();
            statusEvents.addAll( upscaledLeft.getValidationEvents() );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Finished upscaling {} time-series {} from {} to {}, which produced a "
                              + "new time-series, {}.",
                              LeftOrRightOrBaseline.LEFT,
                              left.hashCode(),
                              left.getTimeScale(),
                              desiredTimeScale,
                              scaledLeft.hashCode() );
            }

            // Log any warnings
            PoolSupplier.logScaleValidationWarnings( left, upscaledLeft.getValidationEvents() );

            rescalingMonitor.commit();
        }

        // Upscale right?
        if ( upscaleRight )
        {
            RescalingEvent rescalingMonitor = RescalingEvent.of( RescalingType.UPSCALED, // Monitor
                                                                 orientation,
                                                                 rightOrBaseline.getMetadata() );

            rescalingMonitor.begin();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Upscaling {} time-series {} from {} to {}.",
                              orientation,
                              rightOrBaseline.hashCode(),
                              rightOrBaseline.getTimeScale(),
                              desiredTimeScale );
            }

            TimeSeriesUpscaler<R> rightUp = this.getRightOrBaselineUpscaler( orientation );
            RescaledTimeSeriesPlusValidation<R> upscaledRight = rightUp.upscale( rightOrBaseline,
                                                                                 desiredTimeScale,
                                                                                 endsAt,
                                                                                 desiredUnit );

            scaledRight = upscaledRight.getTimeSeries();
            statusEvents.addAll( upscaledRight.getValidationEvents() );

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Finished upscaling {} time-series {} from {} to {}, which produced a "
                              + "new time-series, {}.",
                              orientation,
                              rightOrBaseline.hashCode(),
                              rightOrBaseline.getTimeScale(),
                              desiredTimeScale,
                              scaledRight.hashCode() );
            }

            // Log any warnings
            PoolSupplier.logScaleValidationWarnings( rightOrBaseline, upscaledRight.getValidationEvents() );

            rescalingMonitor.commit();
        }

        // Transform the rescaled values (e.g., this could contain unit transformations, among others)
        TimeSeries<L> scaledAndTransformedLeft = this.getLeftTransformer()
                                                     .apply( scaledLeft );

        TimeSeries<R> scaledAndTransformedRight = rightTransformers.rightTransformer()
                                                                   .apply( scaledRight );

        // Create the pairs, if any
        TimeSeries<Pair<L, R>> pairs = this.getPairer()
                                           .pair( scaledAndTransformedLeft, scaledAndTransformedRight );

        // Snip the pairs to the pool boundary
        TimeSeries<Pair<L, R>> snippedPairs = TimeSeriesSlicer.snip( pairs, timeWindow );

        // Log the pairing 
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While pairing {} time-series {}, "
                          + "which contained {} values, "
                          + "with {} time-series {},"
                          + " which contained {} values: "
                          + "created {} pairs at the desired time scale of {}.",
                          LeftOrRightOrBaseline.LEFT,
                          scaledAndTransformedLeft.getMetadata(),
                          scaledAndTransformedLeft.getEvents().size(),
                          orientation,
                          scaledAndTransformedRight.getMetadata(),
                          scaledAndTransformedRight.getEvents().size(),
                          snippedPairs.getEvents().size(),
                          desiredTimeScale );
        }

        // Get the feature tuple context in which these pairs are required. There may be more than one tuple. For 
        // example, the same left/right pairs may appear in multiple baseline contexts
        Set<FeatureTuple> featureTuples = this.getFeatureTuplesForFeaturePair( left.getMetadata()
                                                                                   .getFeature(),
                                                                               rightOrBaseline.getMetadata()
                                                                                              .getFeature(),
                                                                               orientation );

        // The pairs, packed by feature tuple
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> pairsToSave =
                featureTuples.stream()
                             .map( next -> Map.entry( next,
                                                      List.of( snippedPairs ) ) )
                             .collect( Collectors.toMap( Map.Entry::getKey,
                                                         Map.Entry::getValue ) );

        // Do we also need generated baseline pairs using the right as a template?
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaseline = new HashMap<>();
        if ( this.hasBaselineGenerator() && orientation == LeftOrRightOrBaseline.RIGHT )
        {
            generatedBaseline = this.getGeneratedBaselinePairs( rightTransformers,
                                                                featureTuples,
                                                                scaledAndTransformedLeft,
                                                                scaledAndTransformedRight );
        }

        return new TimeSeriesPlusValidation<>( pairsToSave,
                                               generatedBaseline,
                                               statusEvents );
    }

    /**
     * Creates generated baseline pairs.
     * 
     * @param rightTransformers the transformers for right-ish data
     * @param featureTuples the tuples containing the baseline features whose data sources will be used for generation
     * @param leftSeries the left series
     * @param templateSeries the right-ish series from which to generate the baseline
     * @return the generated baseline pairs
     */

    private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>
            getGeneratedBaselinePairs( Transformers<R> rightTransformers,
                                       Set<FeatureTuple> featureTuples,
                                       TimeSeries<L> leftSeries,
                                       TimeSeries<R> templateSeries )
    {
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaseline = new HashMap<>();

        for ( FeatureTuple nextFeature : featureTuples )
        {
            Feature baselineFeature = nextFeature.getBaseline();

            // Generate the baseline series
            TimeSeries<R> nextGenerated = this.getGeneratedBaseline( rightTransformers.baselineGenerator(),
                                                                     templateSeries,
                                                                     baselineFeature );

            // Apply the transformer (e.g., unit mapper)
            TimeSeries<R> nextTransformed = rightTransformers.rightTransformer()
                                                             .apply( nextGenerated );

            // Create the pairs
            TimeSeries<Pair<L, R>> generatedPairs = this.getPairer()
                                                        .pair( leftSeries, nextTransformed );

            generatedBaseline.put( nextFeature, List.of( generatedPairs ) );
        }

        return Collections.unmodifiableMap( generatedBaseline );
    }

    /**
     * Generates a baseline for a template right-ish time-series using the baseline-ish feature name for the baseline 
     * source data.
     * @param baselineGenerator the baseline generator
     * @param templateSeries the template time-series for baseline generation
     * @param baselineFeature the baseline feature whose source data will be used for baseline generation
     * @return the generated baseline time-series
     */

    private TimeSeries<R> getGeneratedBaseline( UnaryOperator<TimeSeries<R>> baselineGenerator,
                                                TimeSeries<R> templateSeries,
                                                Feature baselineFeature )
    {
        // Insert the baseline feature name into the template because the generation process uses the baseline
        // source data
        TimeSeries<R> nextTemplateAdjusted = this.adjustSeriesFeature( templateSeries, baselineFeature );
        return baselineGenerator.apply( nextTemplateAdjusted );
    }

    /**
     * Discovers the feature tuples associated with the prescribed feature pair and orientation.
     * 
     * @param leftFeature the left feature
     * @param rightOrBaselineFeature the right or baseline feature
     * @param rightOrBaselineOrientation the right or baseline orientation of the rightOrBaselineFeature
     * @return the feature tuples discovered
     */

    private Set<FeatureTuple> getFeatureTuplesForFeaturePair( Feature leftFeature,
                                                              Feature rightOrBaselineFeature,
                                                              LeftOrRightOrBaseline rightOrBaselineOrientation )
    {
        // Find the feature-tuple context for the pairs to add. The pairs may appear for more than one feature tuple. 
        // For example, the same left/right pairs may appear in N baseline contexts
        Set<FeatureTuple> nextFeatures = null;

        if ( rightOrBaselineOrientation == LeftOrRightOrBaseline.RIGHT )
        {
            nextFeatures = this.getFeatureCorrelator()
                               .getFeatureTuplesForLeftFeature( leftFeature )
                               .stream()
                               .filter( next -> rightOrBaselineFeature.equals( next.getRight() ) )
                               .collect( Collectors.toUnmodifiableSet() );
        }
        else
        {
            nextFeatures = this.getFeatureCorrelator()
                               .getFeatureTuplesForLeftFeature( leftFeature )
                               .stream()
                               .filter( next -> rightOrBaselineFeature.equals( next.getBaseline() ) )
                               .collect( Collectors.toUnmodifiableSet() );
        }

        return nextFeatures;
    }

    /**
     * Performs cross-pairing of the inputs
     * @param mainPairs the main pairs
     * @param basePairs the baseline pairs
     * @return the cross-pairs
     */

    private Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>>
            getCrossPairs( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs,
                           Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs )
    {
        LOGGER.debug( "Conducting cross-pairing of {} and {}.",
                      this.getMetadata(),
                      this.baselineMetadata );

        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairsCrossed = new HashMap<>();
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairsCrossed = new HashMap<>();

        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();
            if ( basePairs.containsKey( nextFeature ) )
            {
                List<TimeSeries<Pair<L, R>>> nextBasePairs = basePairs.get( nextFeature );
                CrossPairs<L, R> crossPairs = this.crossPairer.apply( nextMainPairs, nextBasePairs );
                mainPairsCrossed.put( nextFeature, crossPairs.getMainPairs() );
                basePairsCrossed.put( nextFeature, crossPairs.getBaselinePairs() );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "When conducting cross-pairing, could not locate baseline pairs for feature {} among "
                              + "these features: {}.",
                              nextFeature,
                              basePairs.keySet() );
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            Map<FeatureTuple, Integer> counts = mainPairsCrossed.entrySet()
                                                                .stream()
                                                                .collect( Collectors.toMap( Map.Entry::getKey,
                                                                                            next -> next.getValue()
                                                                                                        .size() ) );
            LOGGER.debug( "Discovered the following counts of cross-pairs per feature: {}.", counts );
        }

        return Pair.of( mainPairsCrossed, basePairsCrossed );
    }

    /**
     * Discovers the left features associated with the specified right or baseline feature.
     * @param rightOrBaselineFeature the left feature
     * @param isRight is true if the right feature is required, false for the baseline
     * @return the left features for the specified right or baseline feature
     */

    private Set<Feature> getLeftFeaturesForRightOrBaseline( Feature rightOrBaselineFeature, LeftOrRightOrBaseline lrb )
    {
        Set<Feature> correlated = null;

        if ( lrb == LeftOrRightOrBaseline.RIGHT )
        {
            correlated = this.getFeatureCorrelator()
                             .getLeftForRightFeature( rightOrBaselineFeature );
        }
        else
        {
            correlated = this.getFeatureCorrelator()
                             .getLeftForBaselineFeature( rightOrBaselineFeature );
        }

        LOGGER.debug( "Correlated LEFT feature {} with {} features {}.", rightOrBaselineFeature, lrb, correlated );

        return correlated;
    }

    /**
     * @param lrb the orientation of the required valid time offset
     * @return the offset
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private Duration getValidTimeOffset( LeftOrRightOrBaseline lrb )
    {
        switch ( lrb )
        {
            case LEFT:
                return this.leftOffset;
            case RIGHT:
                return this.rightOffset;
            case BASELINE:
                return this.baselineOffset;
            default:
                throw new IllegalArgumentException( "Unexpected orientation for transformer: " + lrb
                                                    + ". "
                                                    + "Expected one of "
                                                    + Set.of( LeftOrRightOrBaseline.LEFT,
                                                              LeftOrRightOrBaseline.RIGHT,
                                                              LeftOrRightOrBaseline.BASELINE ) );
        }
    }

    /**
     * Returns the period from the prescribed time scale or <code>null</code> if the time scale is <code>null</code>.
     * 
     * @return the period associated with the time scale or null
     */

    private Duration getPeriodFromTimeScale( TimeScaleOuter timeScale )
    {
        Duration period = null;

        if ( Objects.nonNull( timeScale ) )
        {
            period = TimeScaleOuter.getOrInferPeriodFromTimeScale( timeScale );
        }

        return period;
    }

    /**
     * Adjusts the metadata associated with the input series to use the prescribed feature.
     * @param <T> the time-series event value type
     * @param timeSeries the time-series to adjust
     * @param feature the feature to use
     * @return the adjusted time-series
     */

    private <T> TimeSeries<T> adjustSeriesFeature( TimeSeries<T> timeSeries, Feature feature )
    {
        UnaryOperator<TimeSeriesMetadata> metaMapper =
                innerMetadata -> timeSeries.getMetadata()
                                           .toBuilder()
                                           .setFeature( feature )
                                           .build();

        // Create the template from the right-ish data and insert the baseline feature name
        return TimeSeriesSlicer.transform( timeSeries, Function.identity(), metaMapper );
    }

    /**
     * @return the climatology or null if none exists
     */

    private Supplier<Climatology> getClimatology()
    {
        return this.climatology;
    }

    /**
     * @return true if the supplier contains a baseline, otherwise false
     */

    private boolean hasBaseline()
    {
        return Objects.nonNull( this.baseline ) || Objects.nonNull( this.baselineGenerator );
    }

    /**
     * @return whether a climatological data source is available
     */

    private boolean hasClimatology()
    {
        return Objects.nonNull( this.climatology );
    }

    /**
     * Returns the upscaler for left values. Throws an exception if not available, because this is an internal call and 
     * is only requested when necessary.
     * 
     * @return the upscaler for left values
     * @throws NullPointerException if the upscaler is not available
     */

    private TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        Objects.requireNonNull( this.leftUpscaler );

        return this.leftUpscaler;
    }

    /**
     * Returns the upscaler for right or baseline values. Throws an exception if not available, because this is 
     * an internal call and is only requested when necessary.
     * 
     * @param lrb the orientation required
     * @return the upscaler for right or baseline values
     * @throws NullPointerException if the upscaler is not available
     */

    private TimeSeriesUpscaler<R> getRightOrBaselineUpscaler( LeftOrRightOrBaseline lrb )
    {
        if ( lrb == LeftOrRightOrBaseline.RIGHT )
        {
            Objects.requireNonNull( this.rightUpscaler );

            return this.rightUpscaler;
        }

        Objects.requireNonNull( this.baselineUpscaler );

        return this.baselineUpscaler;
    }

    /**
     * @return the transformer for left-ish data
     */

    private UnaryOperator<TimeSeries<L>> getLeftTransformer()
    {
        return this.leftTransformer;
    }

    /**
     * @return the transformer for right-ish data
     */

    private UnaryOperator<TimeSeries<R>> getRightTransformer()
    {
        return this.rightTransformer;
    }

    /**
     * @return the transformer for baseline-ish data
     */

    private UnaryOperator<TimeSeries<R>> getBaselineTransformer()
    {
        return this.baselineTransformer;
    }

    /**
     * @return the filter for left-ish data
     */

    private Predicate<TimeSeries<L>> getLeftFilter()
    {
        return this.leftFilter;
    }

    /**
     * @return the filter for right-ish data
     */

    private Predicate<TimeSeries<R>> getRightFilter()
    {
        return this.rightFilter;
    }

    /**
     * @return the filter for baseline-ish data
     */

    private Predicate<TimeSeries<R>> getBaselineFilter()
    {
        return this.baselineFilter;
    }

    /**
     * @param lrb the orientation of the required transformer
     * @return the transformer
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private UnaryOperator<TimeSeries<R>> getRightOrBaselineTransformer( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case RIGHT:
                return this.getRightTransformer();
            case BASELINE:
                return this.getBaselineTransformer();
            default:
                throw new IllegalArgumentException( "Unexpected orientation for transformer: " + lrb
                                                    + ". "
                                                    + "Expected "
                                                    + LeftOrRightOrBaseline.RIGHT
                                                    + " or "
                                                    + LeftOrRightOrBaseline.BASELINE
                                                    + "." );
        }
    }

    /**
     * @param lrb the orientation of the required filter
     * @return the filter
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private Predicate<TimeSeries<R>> getRightOrBaselineFilter( LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( lrb );

        switch ( lrb )
        {
            case RIGHT:
                return this.getRightFilter();
            case BASELINE:
                return this.getBaselineFilter();
            default:
                throw new IllegalArgumentException( "Unexpected orientation for filter: " + lrb
                                                    + ". "
                                                    + "Expected "
                                                    + LeftOrRightOrBaseline.RIGHT
                                                    + " or "
                                                    + LeftOrRightOrBaseline.BASELINE
                                                    + "." );
        }
    }

    /**
     * Returns the pairer.
     * 
     * @return the pairer
     */

    private TimeSeriesPairer<L, R> getPairer()
    {
        return this.pairer;
    }

    /**
     * @return the metadata.
     */

    private PoolMetadata getMetadata()
    {
        return this.metadata;
    }

    /**
     * @return the baseline metadata.
     */

    private PoolMetadata getBaselineMetadata()
    {
        return this.baselineMetadata;
    }

    /**
     * Returns the frequency at which to create pairs. By default this is equal to the 
     * {@link TimeScaleOuter#getPeriod()} associated with the {@link #desiredTimeScale}, where defined, otherwise the 
     * value supplied on construction, which normally corresponds to the <code>frequency</code> associated with the 
     * {@link DesiredTimeScaleConfig}. Otherwise <code>null</code>.
     * 
     * @return the frequency or null if undefined
     */

    private Duration getPairedFrequency()
    {
        Duration returnMe = null;

        if ( Objects.nonNull( this.frequency ) )
        {
            returnMe = this.frequency;
        }
        else if ( Objects.nonNull( this.desiredTimeScale ) && this.desiredTimeScale.hasPeriod() )
        {
            returnMe = this.desiredTimeScale.getPeriod();
        }

        return returnMe;
    }

    /**
     * @return the feature correlator
     */

    private FeatureCorrelator getFeatureCorrelator()
    {
        return this.featureCorrelator;
    }

    /**
     * @return the baseline features, possibly empty.
     */

    private Set<Feature> getBaselineFeatures()
    {
        return this.baselineFeatures;
    }

    /**
     * Logs the validation events of type {@link EventType#WARN} associated with rescaling.
     * 
     * TODO: these warnings could probably be consolidated and the context information improved. May need to add 
     * more complete metadata information to the times-series.
     * 
     * @param context the context for the warnings
     * @param scaleValidationEvents the scale validation events
     */

    private static void logScaleValidationWarnings( TimeSeries<?> context,
                                                    List<EvaluationStatusMessage> scaleValidationEvents )
    {
        Objects.requireNonNull( scaleValidationEvents );

        // Any warnings? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isWarnEnabled() )
        {
            Set<EvaluationStatusMessage> warnEvents = scaleValidationEvents.stream()
                                                                           .filter( a -> a.getStatusLevel() == StatusLevel.WARN )
                                                                           .collect( Collectors.toSet() );
            if ( !warnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    ";
                warnEvents.stream().forEach( e -> message.add( spacer + e.toString() ) );

                LOGGER.warn( "While rescaling time-series with metadata {}, encountered {} validation "
                             + "warnings, as follows: {}{}",
                             context.getMetadata(),
                             warnEvents.size(),
                             System.lineSeparator(),
                             message );
            }
        }

        // Any user-facing debug-level events? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isDebugEnabled() )
        {
            Set<EvaluationStatusMessage> debugWarnEvents = scaleValidationEvents.stream()
                                                                                .filter( a -> a.getStatusLevel() == StatusLevel.DEBUG )
                                                                                .collect( Collectors.toSet() );
            if ( !debugWarnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    ";
                debugWarnEvents.stream().forEach( e -> message.add( spacer + e.toString() ) );

                LOGGER.debug( "While rescaling time-series with metadata {}, encountered {} detailed validation "
                              + "warnings, as follows: {}{}",
                              context.getMetadata(),
                              debugWarnEvents.size(),
                              System.lineSeparator(),
                              message );
            }
        }

    }

    /**
     * Adds a prescribed offset to the valid time of the prescribed time-series.
     * 
     * @param <T> the time-series event value type
     * @param toTransform the time-series to transform
     * @param offset the offset to add
     * @param lrb the side of data to help with logging
     * @return the adjusted time-series
     * @throws NullPointerException if the input is time-series is null
     */

    private <T> TimeSeries<T> applyValidTimeOffset( TimeSeries<T> toTransform,
                                                    Duration offset,
                                                    LeftOrRightOrBaseline lrb )
    {
        Objects.requireNonNull( toTransform );

        TimeSeries<T> transformed = toTransform;

        // Transform valid times?
        if ( Objects.nonNull( offset )
             && !Duration.ZERO.equals( offset ) )
        {
            // Log the time shift
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Applying a valid time offset of {} to {} time-series.", offset, lrb );
            }

            transformed = TimeSeriesSlicer.applyOffsetToValidTimes( transformed, offset );
        }

        return transformed;
    }

    /**
     * @return the inputs declaration or null
     */

    private Inputs getInputs()
    {
        Inputs inputs = null;
        if ( Objects.nonNull( this.projectConfig ) )
        {
            inputs = this.projectConfig.getInputs();
        }

        return inputs;
    }

    /**
     * @return the desired time scale
     */

    private TimeScaleOuter getDesiredTimeScale()
    {
        return this.desiredTimeScale;
    }

    /**
     * @return true if there is a baseline generator, false otherwise
     */

    private boolean hasBaselineGenerator()
    {
        return Objects.nonNull( this.baselineGenerator );
    }

    /**
     * @return the baseline generator or null.
     */

    private Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> getBaselineGenerator()
    {
        return this.baselineGenerator;
    }

    /**
     * Hidden constructor.  
     * 
     * @param builder the builder
     * @throws NullPointerException if a required input is null
     * @throws IllegalArgumentException if some input is inconsistent
     */

    private PoolSupplier( Builder<L, R> builder )
    {
        // Set
        this.left = builder.left;
        this.right = builder.right;
        this.baseline = builder.baseline;
        this.pairer = builder.pairer;
        this.leftUpscaler = builder.leftUpscaler;
        this.rightUpscaler = builder.rightUpscaler;
        this.baselineUpscaler = builder.baselineUpscaler;
        this.desiredTimeScale = builder.desiredTimeScale;
        this.metadata = builder.metadata;
        this.baselineMetadata = builder.baselineMetadata;
        this.baselineGenerator = builder.baselineGenerator;
        this.projectConfig = builder.projectConfig;
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.frequency = builder.frequency;
        this.crossPairer = builder.crossPairer;
        this.leftFilter = builder.leftFilter;
        this.rightFilter = builder.rightFilter;
        this.baselineFilter = builder.baselineFilter;
        this.climatology = builder.climatology;

        // Set any time offsets required
        Map<LeftOrRightOrBaseline, Duration> offsets = this.getValidTimeOffsets( this.getInputs() );
        this.leftOffset = offsets.get( LeftOrRightOrBaseline.LEFT );
        this.rightOffset = offsets.get( LeftOrRightOrBaseline.RIGHT );
        this.baselineOffset = offsets.get( LeftOrRightOrBaseline.BASELINE );
        this.featureCorrelator = FeatureCorrelator.of( this.metadata.getFeatureTuples() );

        if ( !offsets.isEmpty() && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered these time offsets by data type {}.",
                          this.metadata,
                          offsets );
        }

        if ( this.hasBaseline() )
        {
            this.baselineFeatures = this.getBaselineMetadata()
                                        .getFeatureTuples()
                                        .stream()
                                        .map( FeatureTuple::getBaseline )
                                        .collect( Collectors.toUnmodifiableSet() );
        }
        else
        {
            this.baselineFeatures = Collections.emptySet();
        }

        // Validate
        String messageStart = "Cannot build the pool supplier: ";
        Objects.requireNonNull( this.left, messageStart + "add a left data source." );
        Objects.requireNonNull( this.right, messageStart + "add a right data source." );
        Objects.requireNonNull( this.pairer, messageStart + "add a pairer, in order to pair the data." );
        Objects.requireNonNull( this.metadata, messageStart + "add the metadata for the main pairs." );
        Objects.requireNonNull( this.leftTransformer, messageStart + "add a transformer for the left data." );
        Objects.requireNonNull( this.rightTransformer, messageStart + "add a transformer for the right data." );
        Objects.requireNonNull( this.baselineTransformer, messageStart + "add a transformer for the baseline data." );
        Objects.requireNonNull( this.leftFilter, messageStart + "add a filter for the left data." );
        Objects.requireNonNull( this.rightFilter, messageStart + "add a filter for the right data." );
        Objects.requireNonNull( this.baselineFilter, messageStart + "add a filter for the baseline data." );

        if ( Objects.isNull( this.desiredTimeScale ) && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered that the desired time scale was undefined.",
                          this.metadata );
        }

        if ( Objects.nonNull( this.frequency ) && frequency.isNegative() )
        {
            throw new IllegalArgumentException( messageStart + "the paired frequency must be null or positive: "
                                                + this.frequency
                                                + "." );
        }

        if ( Objects.isNull( this.projectConfig ) && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered that the project declaration was undefined.",
                          this.metadata );
        }

        // If adding a baseline, baseline metadata is needed. If not, it should not be supplied
        if ( ( Objects.isNull( this.baseline )
               && Objects.isNull( this.baselineGenerator ) ) != Objects.isNull( this.baselineMetadata ) )
        {
            throw new IllegalArgumentException( messageStart + "cannot add a baseline retriever without baseline "
                                                + "metadata and vice versa." );
        }

        // Cannot supply two baseline sources
        if ( Objects.nonNull( this.baseline ) && Objects.nonNull( this.baselineGenerator ) )
        {
            throw new IllegalArgumentException( messageStart + "cannot add a baseline data source and a baseline "
                                                + "generator to the same pool: only one is required." );
        }
    }

    /**
     * Inspects the input declaration for any valid time offsets and returns them when discovered.
     * 
     * @param inputsConfig
     * @return the valid time offsets, if any
     */

    private Map<LeftOrRightOrBaseline, Duration> getValidTimeOffsets( Inputs inputsConfig )
    {
        Map<LeftOrRightOrBaseline, Duration> returnMe = new EnumMap<>( LeftOrRightOrBaseline.class );

        if ( Objects.nonNull( inputsConfig ) )
        {
            if ( Objects.nonNull( inputsConfig.getLeft().getTimeShift() ) )
            {
                Duration leftOff = ConfigHelper.getTimeShift( inputsConfig.getLeft() );
                returnMe.put( LeftOrRightOrBaseline.LEFT, leftOff );
            }

            if ( Objects.nonNull( inputsConfig.getRight().getTimeShift() ) )
            {
                Duration rightOff = ConfigHelper.getTimeShift( inputsConfig.getRight() );
                returnMe.put( LeftOrRightOrBaseline.RIGHT, rightOff );
            }

            if ( Objects.nonNull( inputsConfig.getBaseline() )
                 && Objects.nonNull( inputsConfig.getBaseline().getTimeShift() ) )
            {
                Duration baselineOff = ConfigHelper.getTimeShift( inputsConfig.getLeft() );
                returnMe.put( LeftOrRightOrBaseline.BASELINE, baselineOff );
            }
        }

        // Add benign offsets
        returnMe.putIfAbsent( LeftOrRightOrBaseline.LEFT, Duration.ZERO );
        returnMe.putIfAbsent( LeftOrRightOrBaseline.RIGHT, Duration.ZERO );
        if ( Objects.nonNull( inputsConfig )
             && Objects.nonNull( inputsConfig.getBaseline() ) )
        {
            returnMe.putIfAbsent( LeftOrRightOrBaseline.BASELINE, Duration.ZERO );
        }

        return returnMe;
    }

    /**
     * A value class for storing time series plus their associated validation events.
     * 
     * @author James Brown
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     */
    private static class TimeSeriesPlusValidation<L, R>
    {
        /** The paired time-series. */
        private final Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> series;

        /** The generated baseline time-series, where applicable. */
        private final Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaselineSeries;

        /** The status events. */
        private final List<EvaluationStatusMessage> statusEvents;

        /**
         * @return the time series
         */
        private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> getTimeSeries()
        {
            return this.series;
        }

        /**
         * @return the generated baseline time series if available
         */
        private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> getGeneratedBaselineTimeSeries()
        {
            return this.generatedBaselineSeries;
        }

        /**
         * @return the evaluation status events
         */
        private List<EvaluationStatusMessage> getEvaluationStatusMessages()
        {
            return this.statusEvents;
        }

        /**
         * @param series the time series
         * @param generatedBaselineSeries the optional series for generated baselines
         * @param validation the validation events
         */
        private TimeSeriesPlusValidation( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> series,
                                          Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaselineSeries,
                                          List<EvaluationStatusMessage> statusEvents )
        {
            this.series = Collections.unmodifiableMap( series );
            this.generatedBaselineSeries = Collections.unmodifiableMap( generatedBaselineSeries );
            this.statusEvents = Collections.unmodifiableList( statusEvents );
        }
    }

    /** Record class to bundle two right-ish transformers. */
    private record Transformers<R> ( UnaryOperator<TimeSeries<R>> rightTransformer,
                                     UnaryOperator<TimeSeries<R>> baselineGenerator )
    {
    }
}