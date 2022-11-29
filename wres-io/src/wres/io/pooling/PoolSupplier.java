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
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.Climatology;
import wres.datamodel.messages.MessageFactory;
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
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.FeatureGroup;
import wres.config.generated.FeatureService;
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

    /** Climatological data source at the desired time scale. */
    private final Supplier<Stream<TimeSeries<L>>> climatology;

    /** Mapper from the left-type of climatological data to a double-type. */
    private final ToDoubleFunction<L> climatologyMapper;

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

    /** The left features mapped against the right features as keys. **/
    private final Map<Feature, Feature> leftFeaturesByRight;

    /** The left features mapped against the baseline features as keys. **/
    private final Map<Feature, Feature> leftFeaturesByBaseline;

    /** The right or baseline features mapped against the left features as keys. In general, the map values are 
     * baseline features. However, they are right features when using a generated baseline because generated baselines 
     * use the right-ish data as a template.**/
    private final Map<Feature, Feature> baselineOrRightFeaturesByLeft;

    /** The baseline features mapped against the left features as keys. **/
    private final Map<Feature, Feature> baselineFeaturesByLeft;

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
        Stream<TimeSeries<L>> leftData;

        if ( Objects.nonNull( this.left ) )
        {
            leftData = this.left.get();
        }
        else
        {
            leftData = this.climatology.get();
        }

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
        /** Climatological data source. Optional. */
        private Supplier<Stream<TimeSeries<L>>> climatology;

        /** Climatology mapper. */
        private ToDoubleFunction<L> climatologyMapper;

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
         * @param climatologyMapper the mapper from the climatological type to a double type
         * @return the builder
         */
        Builder<L, R> setClimatology( Supplier<Stream<TimeSeries<L>>> climatology,
                                      ToDoubleFunction<L> climatologyMapper )
        {
            this.climatology = climatology;
            this.climatologyMapper = climatologyMapper;

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
                                                      .collect( Collectors.toList() );

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

        // Pairs indexed by left/right feature
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs = mainPairsPlus.getTimeSeries();

        // Pairs indexed by left/baseline feature
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs = null;

        // Create the baseline pairs, unless they rely on a generator, which must be created in a feature-specific way
        // using the main pairs
        if ( this.hasBaseline() )
        {
            Stream<TimeSeries<R>> baselineDataInner = baselineData;

            // Baseline that is generated?
            if ( this.hasBaselineGenerator() )
            {
                baselineDataInner = this.createBaseline( this.baselineGenerator, mainPairs );
            }

            TimeSeriesPlusValidation<L, R> basePairsPlus = this.createPairsPerFeature( transformedLeft.stream(),
                                                                                       baselineDataInner,
                                                                                       desiredTimeScaleToUse,
                                                                                       pairedFrequency,
                                                                                       LeftOrRightOrBaseline.BASELINE,
                                                                                       this.baselineMetadata.getTimeWindow() );

            validationEvents.addAll( basePairsPlus.getEvaluationStatusMessages() );

            basePairs = basePairsPlus.getTimeSeries();

            // Cross-pair?
            if ( Objects.nonNull( this.crossPairer ) )
            {
                Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>> cp =
                        this.getCrossPairs( mainPairs, basePairs );

                mainPairs = cp.getLeft();
                basePairs = cp.getRight();
            }
        }

        // Create the pool from the raw data
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
     * @param mainPairs the main pairs
     * @param basePairs the baseline pairs, optional
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

        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairsToUse = mainPairs;

        // No data? Then build an empty mini-pool for each feature
        if ( mainPairs.isEmpty() )
        {
            LOGGER.debug( "Discovered no pairs. Filling with empty pools." );
            Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> emptyPools = new HashMap<>();
            this.getMetadata()
                .getFeatureTuples()
                .forEach( nextFeature -> emptyPools.put( nextFeature, List.of() ) );
            mainPairsToUse = emptyPools;
        }

        Pool.Builder<TimeSeries<Pair<L, R>>> builder = new Pool.Builder<>();
        builder.setClimatology( this.getClimatology() );

        // Create the mini pools, one per feature
        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairsToUse.entrySet() )
        {
            FeatureTuple nextLeftRightFeature = nextEntry.getKey();
            Feature leftFeature = nextLeftRightFeature.getLeft();

            // For now, use the complete left-right-baseline tuple for both the main and baseline pools. However, this
            // is an awkward abstraction - the pool metadata should really deal with only left-right descriptions, 
            // since there is separate metadata for left-right and left-baseline pairs. Once fixed, the geometries will 
            // be pairs rather than tuples in most contexts, including the pool metadata
            GeometryTuple geoTuple = this.getGeometryTuple( nextLeftRightFeature );
            FeatureTuple fullTuple = FeatureTuple.of( geoTuple );

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();

            Pool.Builder<TimeSeries<Pair<L, R>>> nextMiniPoolBuilder = new Pool.Builder<>();

            wres.statistics.generated.Pool.Builder newInnerMainMetaBuilder =
                    this.getMetadata()
                        .getPool()
                        .toBuilder()
                        .clearGeometryTuples()
                        .addGeometryTuples( geoTuple )
                        .setGeometryGroup( GeometryGroup.newBuilder()
                                                        // Default region name is the short name of the tuple
                                                        .setRegionName( fullTuple.toStringShort() )
                                                        .addGeometryTuples( geoTuple ) );

            PoolMetadata nextInnerMainMeta = PoolMetadata.of( this.getMetadata()
                                                                  .getEvaluation(),
                                                              newInnerMainMetaBuilder.build() );

            // Set the time scale
            nextInnerMainMeta = PoolMetadata.of( nextInnerMainMeta, desiredTimeScale );

            nextMiniPoolBuilder.setMetadata( nextInnerMainMeta )
                               .addData( nextMainPairs );

            // Set the baseline
            if ( this.hasBaseline() )
            {
                Feature baselineFeature = this.getBaselineFeature( leftFeature, this.hasBaselineGenerator() );

                GeometryTuple nextBaselineGeometry =
                        MessageFactory.getGeometryTuple( leftFeature, baselineFeature, null );
                FeatureTuple nextBaselineTuple = FeatureTuple.of( nextBaselineGeometry );

                List<TimeSeries<Pair<L, R>>> nextBasePairs = basePairs.get( nextBaselineTuple );

                // Pairs can be empty but not null
                if ( Objects.isNull( nextBasePairs ) )
                {
                    Set<Feature> availableFeatures = basePairs.keySet()
                                                              .stream()
                                                              .map( FeatureTuple::getRight )
                                                              .collect( Collectors.toSet() );

                    throw new PoolException( "Failed to identify any baseline pairs for feature: " + baselineFeature
                                             + ". The available features were: "
                                             + availableFeatures
                                             + "." );
                }

                wres.statistics.generated.Pool.Builder newInnerBaseMetaBuilder =
                        this.baselineMetadata.getPool()
                                             .toBuilder()
                                             .clearGeometryTuples()
                                             .addGeometryTuples( geoTuple )
                                             .setGeometryGroup( GeometryGroup.newBuilder()
                                                                             // Default region name is the short name 
                                                                             // of the tuple
                                                                             .setRegionName( fullTuple.toStringShort() )
                                                                             .addGeometryTuples( geoTuple ) );

                PoolMetadata nextInnerBaseMeta = PoolMetadata.of( this.baselineMetadata.getEvaluation(),
                                                                  newInnerBaseMetaBuilder.build() );

                // Set the time scale
                nextInnerBaseMeta = PoolMetadata.of( nextInnerBaseMeta, desiredTimeScale );

                nextMiniPoolBuilder.setMetadataForBaseline( nextInnerBaseMeta )
                                   .addDataForBaseline( nextBasePairs );
            }

            // Set the climatology
            Climatology nextClimatology = this.getClimatology();
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
     * Generates a left-right-baseline tuple from a left-right tuple if a baseline exists.
     * @param leftRightTuple the left-right tuple
     * @return the left-right-baseline tuple
     */

    private GeometryTuple getGeometryTuple( FeatureTuple leftRightTuple )
    {
        if ( this.hasBaseline() )
        {
            Feature leftFeature = leftRightTuple.getLeft();
            Feature rightFeature = leftRightTuple.getRight();
            Feature baselineFeature = this.getBaselineFeature( leftFeature, this.hasBaselineGenerator() );
            return MessageFactory.getGeometryTuple( leftFeature, rightFeature, baselineFeature );
        }

        return leftRightTuple.getGeometryTuple();
    }

    /**
     * Creates a paired dataset from the input, rescaling the left/right data as needed.
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
        Iterator<TimeSeries<R>> rightOrBaselineIterator = rightOrBaseline.iterator();

        // Loop over the right-ish series
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

            // Find the left-ish series
            Feature nextRightOrBaselineFeature = transformedRightOrBaseline.getMetadata()
                                                                           .getFeature();

            Feature nextLeftFeature = this.getLeftFeatureForRightOrBaseline( nextRightOrBaselineFeature,
                                                                             rightOrBaselineOrientation );

            List<TimeSeries<L>> nextLeftSeries = leftSeries.get( nextLeftFeature );

            List<TimeSeriesPlusValidation<L, R>> nextPairs = this.createPairsPerLeftSeries( nextLeftSeries,
                                                                                            transformedRightOrBaseline,
                                                                                            desiredTimeScale,
                                                                                            frequency,
                                                                                            rightOrBaselineOrientation,
                                                                                            timeWindow,
                                                                                            rightOrBaselineTransformer );

            // Get the correct feature name for the right or baseline side, handling generated baselines as needed
            Feature adjustedRightOrBaselineFeature = this.getRightOrBaselineFeature( nextLeftFeature,
                                                                                     nextRightOrBaselineFeature,
                                                                                     this.hasBaselineGenerator(),
                                                                                     rightOrBaselineOrientation );

            // Add to the results
            for ( TimeSeriesPlusValidation<L, R> nextSeries : nextPairs )
            {
                List<EvaluationStatusMessage> statusEvents = nextSeries.getEvaluationStatusMessages();
                validation.addAll( statusEvents );

                // Only one series here because that is what we paired above
                TimeSeries<Pair<L, R>> pairs = nextSeries.getTimeSeries()
                                                         .values()
                                                         .iterator()
                                                         .next()
                                                         .get( 0 );

                if ( !pairs.getEvents().isEmpty() )
                {
                    // Identify the pairs with the left/right geometry pair. See #105812                   
                    GeometryTuple nextGeometry = MessageFactory.getGeometryTuple( nextLeftFeature,
                                                                                  adjustedRightOrBaselineFeature,
                                                                                  null );
                    FeatureTuple nextFeature = FeatureTuple.of( nextGeometry );

                    List<TimeSeries<Pair<L, R>>> nextList = pairsPerFeature.get( nextFeature );
                    if ( Objects.isNull( nextList ) )
                    {
                        nextList = new ArrayList<>();
                        pairsPerFeature.put( nextFeature, nextList );
                    }

                    nextList.add( pairs );
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

        return new TimeSeriesPlusValidation<>( pairsPerFeature, validation );
    }

    /**
     * Creates a time-series of pairs for each left series.
     * @param leftSeries the left time-series, which may be null
     * @param rightOrBaselineSeries the right time-series
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency of the pairs
     * @param rightOrBaselineOrientation the orientation of the pairs
     * @param timeWindow the time window
     * @param rightOrBaselineTransformer the transformer for the right series
     * @return the pairs plus validation
     * @throws NullPointerException if any required input is null
     */

    private List<TimeSeriesPlusValidation<L, R>> createPairsPerLeftSeries( List<TimeSeries<L>> leftSeries,
                                                                           TimeSeries<R> rightOrBaselineSeries,
                                                                           TimeScaleOuter desiredTimeScale,
                                                                           Duration frequency,
                                                                           LeftOrRightOrBaseline rightOrBaselineOrientation,
                                                                           TimeWindowOuter timeWindow,
                                                                           UnaryOperator<TimeSeries<R>> rightOrBaselineTransformer )
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
                                                                               rightOrBaselineTransformer,
                                                                               desiredTimeScale,
                                                                               frequency,
                                                                               timeWindow,
                                                                               rightOrBaselineOrientation );

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
     * @param rightOrBaselineTransformer the transformer for right or baseline-ish data
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency with which to create pairs at the desired time scale
     * @param timeWindow the time window to snip the pairs
     * @param orientation the orientation of the non-left data, one of {@link LeftOrRightOrBaseline#RIGHT} or 
     *            {@link LeftOrRightOrBaseline#BASELINE}
     * @return a paired time-series
     * @throws NullPointerException if the left, rightOrBaseline or timeWindow is null
     */

    private TimeSeriesPlusValidation<L, R> createSeriesPairs( TimeSeries<L> left,
                                                              TimeSeries<R> rightOrBaseline,
                                                              UnaryOperator<TimeSeries<R>> rightOrBaselineTransformer,
                                                              TimeScaleOuter desiredTimeScale,
                                                              Duration frequency,
                                                              TimeWindowOuter timeWindow,
                                                              LeftOrRightOrBaseline orientation )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( rightOrBaseline );
        Objects.requireNonNull( timeWindow );

        // Desired unit
        String desiredUnit = this.getMetadata()
                                 .getMeasurementUnit()
                                 .getUnit();

        // Create the feature tuple
        GeometryTuple geometryTuple = MessageFactory.getGeometryTuple( left.getMetadata()
                                                                           .getFeature(),
                                                                       rightOrBaseline.getMetadata()
                                                                                      .getFeature(),
                                                                       null );
        FeatureTuple featureTuple = FeatureTuple.of( geometryTuple );

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

        TimeSeries<R> scaledAndTransformedRight = rightOrBaselineTransformer.apply( scaledRight );

        // Create the pairs, if any
        TimeSeries<Pair<L, R>> pairs = this.getPairer()
                                           .pair( scaledAndTransformedLeft, scaledAndTransformedRight );

        // Snip the pairs to the pool boundary
        TimeSeries<Pair<L, R>> snippedPairs = this.snip( pairs, timeWindow );

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

        return new TimeSeriesPlusValidation<>( featureTuple, snippedPairs, statusEvents );
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
            FeatureTuple leftRightFeature = nextEntry.getKey();
            Feature leftFeature = leftRightFeature.getLeft();
            Feature baselineFeature = this.getBaselineFeature( leftFeature, false );

            GeometryTuple nextBaselineGeometry = MessageFactory.getGeometryTuple( leftFeature, baselineFeature, null );
            FeatureTuple nextBaselineTuple = FeatureTuple.of( nextBaselineGeometry );

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();
            if ( basePairs.containsKey( nextBaselineTuple ) )
            {
                List<TimeSeries<Pair<L, R>>> nextBasePairs = basePairs.get( nextBaselineTuple );
                CrossPairs<L, R> crossPairs = this.crossPairer.apply( nextMainPairs, nextBasePairs );
                mainPairsCrossed.put( leftRightFeature, crossPairs.getMainPairs() );
                basePairsCrossed.put( nextBaselineTuple, crossPairs.getBaselinePairs() );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "When conducting cross-pairing, unable to locate baseline pairs for feature {} among "
                              + "these features: {}.",
                              nextBaselineTuple,
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
     * Discovers the left feature associated with the specified right or baseline feature. Assumes one left feature per
     * one right or baseline feature.
     * @param rightOrBaselineFeature the left feature
     * @param isRight is true if the right feature is required, false for the baseline
     * @return the left feature for the specified right or baseline feature
     */

    private Feature getLeftFeatureForRightOrBaseline( Feature rightOrBaselineFeature, LeftOrRightOrBaseline lrb )
    {
        Feature correlated = null;

        if ( lrb == LeftOrRightOrBaseline.RIGHT )
        {
            correlated = this.leftFeaturesByRight.get( rightOrBaselineFeature );

            if ( Objects.isNull( correlated ) )
            {
                throw new IllegalStateException( "Unable to find a left feature corresponding to the right feature "
                                                 + rightOrBaselineFeature
                                                 + ". The available pairs of left/right features were: "
                                                 + this.leftFeaturesByRight
                                                 + "." );
            }
        }
        else
        {
            correlated = this.leftFeaturesByBaseline.get( rightOrBaselineFeature );

            if ( Objects.isNull( correlated ) )
            {
                throw new IllegalStateException( "Unable to find a left feature corresponding to the baseline feature "
                                                 + rightOrBaselineFeature
                                                 + ". The available pairs of left/right features were: "
                                                 + this.leftFeaturesByBaseline
                                                 + "." );
            }
        }

        LOGGER.debug( "Correlated LEFT feature {} with {} feature {}.", rightOrBaselineFeature, lrb, correlated );

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
     * Creates a baseline dataset from a generator function using the right side of the input pairs as the source to
     * mimic and the left feature as the feature name.
     * 
     * @param generator the feature-specific baseline generator function
     * @param pairs the pairs whose right side will be used to create the baseline
     * @return a list of generated baseline time-series
     */

    private Stream<TimeSeries<R>> createBaseline( Function<Set<Feature>, UnaryOperator<TimeSeries<R>>> generator,
                                                  Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> pairs )
    {
        LOGGER.debug( "Creating baseline time-series with a generator function." );

        List<TimeSeries<R>> returnMe = new ArrayList<>();

        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : pairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();
            List<TimeSeries<Pair<L, R>>> nextRight = nextEntry.getValue();

            // Get the left feature name
            Feature nextFeatureKey = nextFeature.getLeft();

            // Get the corresponding baseline feature name, which is the name associated with the baseline-ish data in 
            // this context, not the data used as a template for baseline generation, which is the paired data and 
            // currently uses the left-ish feature name in the time-series metadata
            Feature baselineFeatureKey = this.getBaselineFeature( nextFeatureKey, true );
            UnaryOperator<TimeSeries<R>> nextGenerator = generator.apply( Set.of( baselineFeatureKey ) );

            for ( TimeSeries<Pair<L, R>> nextPairs : nextRight )
            {
                UnaryOperator<TimeSeriesMetadata> metaMapper = metadata -> nextPairs.getMetadata()
                                                                                    .toBuilder()
                                                                                    .setFeature( baselineFeatureKey )
                                                                                    .build();

                // Create the template from the right-ish data and insert the baseline feature name
                TimeSeries<R> nextTemplate = TimeSeriesSlicer.transform( nextPairs, Pair::getRight, metaMapper );
                TimeSeries<R> generated = nextGenerator.apply( nextTemplate );
                returnMe.add( generated );
            }
        }

        LOGGER.debug( "Finished creating baseline time-series with a generator function." );

        return returnMe.stream();
    }

    /**
     * Creates the climatological data as needed.
     * 
     * @return the climatological data or null if no climatology is defined
     */

    private Climatology getClimatology()
    {
        if ( Objects.isNull( this.climatology ) )
        {
            return null;
        }

        Function<L, Double> mapper = value -> this.climatologyMapper.applyAsDouble( value );
        List<TimeSeries<Double>> climData = this.climatology.get()
                                                            .map( next -> TimeSeriesSlicer.transform( next,
                                                                                                      mapper,
                                                                                                      null ) )
                                                            .collect( Collectors.toList() );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered {} climatological time-series for pool {}.",
                          climData.size(),
                          this.getMetadata() );
        }

        return Climatology.of( climData );
    }

    /**
     * Returns true if the supplier includes a baseline, otherwise false.
     * 
     * @return true if the supplier contains a baseline, otherwise false.
     */

    private boolean hasBaseline()
    {
        return Objects.nonNull( this.baseline ) || Objects.nonNull( this.baselineGenerator );
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
     * A helper that returns the baseline feature associated with the prescribed left feature. If the specified flag is
     * {@code false}, looks inside the {@link #baselineOrRightFeaturesByLeft}, otherwise looks in the 
     * {@link #baselineFeaturesByLeft}.
     * 
     * @param leftFeature the left feature, not null
     * @param isSourceForGeneratedBaseline is true if referencing the source data associated with a generated baseline
     * @return the baseline feature name
     */

    private Feature getBaselineFeature( Feature leftFeature, boolean isSourceForGeneratedBaseline )
    {
        Objects.requireNonNull( leftFeature );

        // Strictly the baseline-ish feature name? Yes when the context is the source data for a generated baseline 
        // because source data is always referenced by its true feature side. However, in all other cases the feature is
        // whatever is set as the baseline name on creation of the PoolSupplier. For a generated baseline, this will be 
        // the right-ish feature name because right-ish data is used as the template for generating a baseline. 
        // Otherwise, it will be the baseline feature.
        if ( isSourceForGeneratedBaseline )
        {
            return this.baselineFeaturesByLeft.get( leftFeature );
        }

        return this.baselineOrRightFeaturesByLeft.get( leftFeature );
    }

    /**
     * A helper that returns the input right feature or the result of {@link #getBaselineFeature(Feature, boolean)} 
     * when the orientation is baseline.
     * 
     * @param leftFeature the left feature
     * @param rightOrBaselineFeature the right or baseline feature
     * @param isSourceForGeneratedBaseline is true if referencing the source data associated with a generated baseline
     * @param rightOrBaselineOrientation the orientation of the data, one of right or baseline
     * @return the right or baseline feature name
     */

    private Feature getRightOrBaselineFeature( Feature leftFeature,
                                               Feature rightOrBaselineFeature,
                                               boolean isSourceForGeneratedBaseline,
                                               LeftOrRightOrBaseline rightOrBaselineOrientation )
    {
        Objects.requireNonNull( leftFeature );

        Feature adjustedRightOrBaselineFeature = rightOrBaselineFeature;
        if ( rightOrBaselineOrientation == LeftOrRightOrBaseline.BASELINE )
        {
            adjustedRightOrBaselineFeature = this.getBaselineFeature( leftFeature,
                                                                      isSourceForGeneratedBaseline );
        }

        return adjustedRightOrBaselineFeature;
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
     * Snips the input pairs to the pool boundary. Only filters lead durations with respect to reference times with 
     * the type {@link ReferenceTimeType#T0}.
     * 
     * @param toSnip the pairs to snip
     * @param snipTo the time window to use when snipping
     * @return the snipped pairs
     */

    private TimeSeries<Pair<L, R>> snip( TimeSeries<Pair<L, R>> toSnip, TimeWindowOuter snipTo )
    {
        Objects.requireNonNull( toSnip );

        TimeSeries<Pair<L, R>> returnMe = toSnip;

        if ( Objects.nonNull( snipTo ) )
        {

            // Snip datetimes first, because lead durations are only snipped with respect to 
            // the ReferenceTimeType.T0
            TimeWindow inner = MessageFactory.getTimeWindow( snipTo.getEarliestReferenceTime(),
                                                             snipTo.getLatestReferenceTime(),
                                                             snipTo.getEarliestValidTime(),
                                                             snipTo.getLatestValidTime() );
            TimeWindowOuter partialSnip = TimeWindowOuter.of( inner );

            LOGGER.debug( "Snipping paired time-series {} to the pool boundaries of {}.",
                          toSnip.hashCode(),
                          partialSnip );

            returnMe = TimeSeriesSlicer.filter( returnMe, partialSnip );


            // For all other reference time types, filter the datetimes only
            if ( toSnip.getReferenceTimes().containsKey( ReferenceTimeType.T0 )
                 && !snipTo.bothLeadDurationsAreUnbounded() )
            {
                LOGGER.debug( "Additionally snipping paired time-series {} to lead durations ({},{}] for the reference "
                              + "time type of {}.",
                              toSnip.hashCode(),
                              snipTo.getEarliestLeadDuration(),
                              snipTo.getLatestLeadDuration(),
                              ReferenceTimeType.T0 );

                returnMe = TimeSeriesSlicer.filter( returnMe,
                                                    snipTo,
                                                    Set.of( ReferenceTimeType.T0 ) );
            }

        }

        return returnMe;
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
     * Hidden constructor.  
     * 
     * @param builder the builder
     * @throws NullPointerException if a required input is null
     * @throws IllegalArgumentException if some input is inconsistent
     */

    private PoolSupplier( Builder<L, R> builder )
    {
        // Set
        this.climatology = builder.climatology;
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
        this.climatologyMapper = builder.climatologyMapper;
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

        // Set any time offsets required
        Map<LeftOrRightOrBaseline, Duration> offsets = this.getValidTimeOffsets( this.getInputs() );
        this.leftOffset = offsets.get( LeftOrRightOrBaseline.LEFT );
        this.rightOffset = offsets.get( LeftOrRightOrBaseline.RIGHT );
        this.baselineOffset = offsets.get( LeftOrRightOrBaseline.BASELINE );
        this.leftFeaturesByRight = this.getFeatureMap( this.metadata, FeatureTuple::getRight, FeatureTuple::getLeft );
        this.leftFeaturesByBaseline = this.getFeatureMap( this.baselineMetadata,
                                                          FeatureTuple::getBaseline,
                                                          FeatureTuple::getLeft );

        this.baselineFeaturesByLeft = this.getFeatureMap( this.baselineMetadata,
                                                          FeatureTuple::getLeft,
                                                          FeatureTuple::getBaseline );

        // Baseline generator present? Then the baseline feature names are based on the right-ish template data
        // This is unnecessarily awkward - see #105812
        if ( this.hasBaselineGenerator() )
        {
            this.baselineOrRightFeaturesByLeft = this.getFeatureMap( this.baselineMetadata,
                                                                     FeatureTuple::getLeft,
                                                                     FeatureTuple::getRight );
        }
        else
        {
            this.baselineOrRightFeaturesByLeft = this.getFeatureMap( this.baselineMetadata,
                                                                     FeatureTuple::getLeft,
                                                                     FeatureTuple::getBaseline );
        }

        if ( !offsets.isEmpty() && LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered these time offsets by data type {}.",
                          this.metadata,
                          offsets );
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

        if ( Objects.isNull( this.climatology ) != Objects.isNull( this.climatologyMapper ) )
        {
            throw new IllegalArgumentException( messageStart + "cannot add a climatological data source without a "
                                                + "mapper to double values and vice versa." );
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
     * @param metadata the metadata whose features should be mapped
     * @return a mapping of features
     */

    private Map<Feature, Feature> getFeatureMap( PoolMetadata metadata,
                                                 Function<? super FeatureTuple, ? extends Feature> keyMapper,
                                                 Function<? super FeatureTuple, ? extends Feature> valueMapper )
    {
        if ( Objects.isNull( metadata ) )
        {
            return Collections.emptyMap();
        }

        return metadata.getFeatureTuples()
                       .stream()
                       .collect( Collectors.toMap( keyMapper, valueMapper ) );
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
     * @return true if feature groups are declared, false for only implicit/singleton groups, regardless of batching
     */

    private boolean hasDeclaredFeatureGroups()
    {
        if ( Objects.isNull( this.projectConfig ) )
        {
            return false;
        }

        FeatureService service = this.projectConfig.getPair()
                                                   .getFeatureService();

        // Explicit feature groups or implicitly declared groups via a feature service
        return !this.projectConfig.getPair()
                                  .getFeatureGroup()
                                  .isEmpty()
               || ( Objects.nonNull( service ) && service.getGroup()
                                                         .stream()
                                                         .anyMatch( FeatureGroup::isPool ) );
    }

    /**
     * A smaller value class for storing time series plus their associated validation events.
     * @author James Brown
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     */
    private static class TimeSeriesPlusValidation<L, R>
    {
        /** The paired time-series. */
        private final Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> series;
        /** The status events. */
        private List<EvaluationStatusMessage> statusEvents;

        /**
         * @return the time series
         */
        private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> getTimeSeries()
        {
            return this.series;
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
         * @param validation the validation events
         */
        private TimeSeriesPlusValidation( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> series,
                                          List<EvaluationStatusMessage> statusEvents )
        {
            this.series = Collections.unmodifiableMap( series );
            this.statusEvents = Collections.unmodifiableList( statusEvents );
        }

        /**
         * @param feature the feature
         * @param series the time series
         * @param validation the validation events
         */
        private TimeSeriesPlusValidation( FeatureTuple feature,
                                          TimeSeries<Pair<L, R>> series,
                                          List<EvaluationStatusMessage> statusEvents )
        {
            Objects.requireNonNull( feature );
            Objects.requireNonNull( series );

            this.series = Map.of( feature, List.of( series ) );
            this.statusEvents = Collections.unmodifiableList( statusEvents );
        }
    }

}