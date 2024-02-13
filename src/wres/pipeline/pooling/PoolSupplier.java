package wres.pipeline.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
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

import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.CrossPairScope;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.Climatology;
import wres.datamodel.baselines.BaselineGenerator;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.pools.Pool;
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
import wres.datamodel.units.NoSuchUnitConversionException;
import wres.pipeline.pooling.PoolRescalingEvent.RescalingType;
import wres.io.retrieving.DataAccessException;
import wres.statistics.generated.GeometryGroup;

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
 * @param <R> the type of right and baseline-ish value in each pair
 */

@ThreadSafe
public class PoolSupplier<L, R, B> implements Supplier<Pool<TimeSeries<Pair<L, R>>>>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( PoolSupplier.class );

    /** Repeated message string. */
    private static final String WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR = "While constructing a pool supplier for {}, ";

    /** Repeated message string. */
    private static final String EXPECTED = "Expected ";

    /** Generator for baseline data source. Optional. */
    private final Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator;

    /** Pairer. */
    private final TimeSeriesPairer<L, R> pairer;

    /** An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time. */
    private final TimeSeriesCrossPairer<Pair<L, R>> crossPairer;

    /** The cross-pairing declaration, if any. */
    private final CrossPair crossPair;

    /** Upscaler for left-type data. Optional on construction, but may be exceptional if absent and later required. */
    private final TimeSeriesUpscaler<L> leftUpscaler;

    /** Upscaler for right-type data. Optional on construction, but may be exceptional if later required. */
    private final TimeSeriesUpscaler<R> rightUpscaler;

    /** Upscaler for baseline-type data. Optional on construction, but may be exceptional if later required. */
    private final TimeSeriesUpscaler<R> baselineUpscaler;

    /** The desired timescale. */
    private final TimeScaleOuter desiredTimeScale;

    /** Metadata for the mains pairs. */
    private final PoolMetadata metadata;

    /** Metadata for the baseline pairs. */
    private final PoolMetadata baselineMetadata;

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

    /** A function that filters missing left-ish values. */
    private final Predicate<L> leftMissingFilter;

    /** A function that filters missing right-ish values. */
    private final Predicate<R> rightMissingFilter;

    /** A function that filters missing baseline-ish values. */
    private final Predicate<R> baselineMissingFilter;

    /** Any time offset to apply to the left-ish valid times. */
    private final Duration leftTimeShift;

    /** Any time offset to apply to the right-ish valid times. */
    private final Duration rightTimeShift;

    /** Any time offset to apply to the baseline-ish valid times. */
    private final Duration baselineTimeShift;

    /** Frequency with which pairs should be constructed at the desired time-scale. */
    private final Duration frequency;

    /** The feature correlator. **/
    private final FeatureCorrelator featureCorrelator;

    /** Set of baseline features for baseline generation, possibly empty. */
    private final Set<Feature> baselineFeatures;

    /** Has the supplier been called before? */
    private final AtomicBoolean done = new AtomicBoolean( false );

    /** A shim to map from a baseline-ish dataset to a right-ish dataset.
     * TODO: remove this shim when pools support different types of right and baseline data. */
    private final Function<TimeSeries<B>, TimeSeries<R>> baselineShim;

    /** Left data source. */
    private Supplier<Stream<TimeSeries<L>>> left;

    /** Right data source. */
    private Supplier<Stream<TimeSeries<R>>> right;

    /** Baseline data source. Optional. */
    private Supplier<Stream<TimeSeries<B>>> baseline;

    /** The climatology, possibly null. */
    private Supplier<Climatology> climatology;

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
        Stream<TimeSeries<B>> baselineData = null;

        // Baseline that is not generated?
        if ( this.hasBaseline() && !this.hasBaselineGenerator() )
        {
            baselineData = this.baseline.get();
        }

        Pool<TimeSeries<Pair<L, R>>> returnMe;

        // In this context, a stream can mean an open connection to a resource, such as a database, so it is essential 
        // that each stream is closed on completion. A well-behaved stream will then close any resources on which it 
        // depends.
        Stream<TimeSeries<B>> finalBaselineData = baselineData;
        try ( leftData; rightData; finalBaselineData )
        {
            returnMe = this.createPool( leftData, rightData, finalBaselineData );
        }

        poolMonitor.commit();

        if ( LOGGER.isDebugEnabled() )
        {
            int pairCount = PoolSlicer.getEventCount( returnMe );

            LOGGER.debug( "Finished creating pool {}, which contains {} time-series and {} pairs.",
                          this.getMetadata(),
                          returnMe.get().size(),
                          pairCount );
        }

        // Render any potentially expensive state eligible for gc, since this instance is one-and-done
        this.left = null;
        this.right = null;
        this.baseline = null;
        this.climatology = null;

        return returnMe;
    }

    /**
     * Builder for a {@link PoolSupplier}.
     *
     * @author James Brown
     * @param <L> the left type of paired value
     * @param <R> the right type of paired value
     * @param <B> the baseline type supplied to generate the baseline pairs of (L,R) type
     */

    static class Builder<L, R, B>
    {
        /** The climatology. */
        private Supplier<Climatology> climatology;

        /** Left data source. */
        private Supplier<Stream<TimeSeries<L>>> left;

        /** Right data source. */
        private Supplier<Stream<TimeSeries<R>>> right;

        /** Baseline data source. Optional. */
        private Supplier<Stream<TimeSeries<B>>> baseline;

        /** Generator for baseline data source. Optional. */
        private Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator;

        /** Pairer. */
        private TimeSeriesPairer<L, R> pairer;

        /** Upscaler for left-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<L> leftUpscaler;

        /** Upscaler for right-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<R> rightUpscaler;

        /** Upscaler for baseline-type data. Optional on construction, but may be exceptional if later required. */
        private TimeSeriesUpscaler<R> baselineUpscaler;

        /** The desired time-scale. */
        private TimeScaleOuter desiredTimeScale;

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

        /** A function that filters missing left-ish values. */
        private Predicate<L> leftMissingFilter = notMissing -> true;

        /** A function that filters missing right-ish values. */
        private Predicate<R> rightMissingFilter = notMissing -> true;

        /** A function that filters missing baseline-ish values. */
        private Predicate<R> baselineMissingFilter = notMissing -> true;

        /** The time offset to apply to the left-ish valid times. */
        private Duration leftTimeShift = Duration.ZERO;

        /** The time offset to apply to the right-ish valid times. */
        private Duration rightTimeShift = Duration.ZERO;

        /** The time offset to apply to the baseline-ish valid times. */
        private Duration baselineTimeShift = Duration.ZERO;

        /** The frequency at which pairs should be produced. */
        private Duration frequency;

        /** An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time. */
        private TimeSeriesCrossPairer<Pair<L, R>> crossPairer;

        /** The cross-pairing declaration, if any. */
        private CrossPair crossPair;

        /** A shim to map from a baseline-ish dataset to a right-ish dataset. */
        private Function<TimeSeries<B>, TimeSeries<R>> baselineShim;

        /**
         * @param climatology the climatology to set
         * @return the builder
         */
        Builder<L, R, B> setClimatology( Supplier<Climatology> climatology )
        {
            this.climatology = climatology;

            return this;
        }

        /**
         * @param left the left to set
         * @return the builder
         */
        Builder<L, R, B> setLeft( Supplier<Stream<TimeSeries<L>>> left )
        {
            this.left = left;

            return this;
        }

        /**
         * @param right the right to set
         * @return the builder
         */
        Builder<L, R, B> setRight( Supplier<Stream<TimeSeries<R>>> right )
        {
            this.right = right;

            return this;
        }

        /**
         * @param baseline the baseline to set
         * @return the builder
         */
        Builder<L, R, B> setBaseline( Supplier<Stream<TimeSeries<B>>> baseline )
        {
            this.baseline = baseline;

            return this;
        }

        /**
         * @param baselineGenerator a baseline generator to set
         * @return the builder
         */
        Builder<L, R, B> setBaselineGenerator( Function<Set<Feature>, BaselineGenerator<R>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * @param pairer the pairer to set
         * @return the builder
         */
        Builder<L, R, B> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param crossPairer the cross-pairer to set
         * @param crossPair the cross-pairing declaration to set
         * @return the builder
         */
        Builder<L, R, B> setCrossPairer( TimeSeriesCrossPairer<Pair<L, R>> crossPairer,
                                         CrossPair crossPair )
        {
            this.crossPairer = crossPairer;
            this.crossPair = crossPair;

            return this;
        }

        /**
         * @param leftUpscaler the leftUpscaler to set
         * @return the builder
         */
        Builder<L, R, B> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the rightUpscaler to set
         * @return the builder
         */
        Builder<L, R, B> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param baselineUpscaler the upscaler for baseline values
         * @return the builder
         */
        Builder<L, R, B> setBaselineUpscaler( TimeSeriesUpscaler<R> baselineUpscaler )
        {
            this.baselineUpscaler = baselineUpscaler;

            return this;
        }

        /**
         * @param desiredTimeScale the desiredTimeScale to set
         * @return the builder
         */
        Builder<L, R, B> setDesiredTimeScale( TimeScaleOuter desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;

            return this;
        }

        /**
         * @param metadata the metadata to set
         * @return the builder
         */
        Builder<L, R, B> setMetadata( PoolMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

        /**
         * @param baselineMetadata the baselineMetadata to set
         * @return the builder
         */
        Builder<L, R, B> setBaselineMetadata( PoolMetadata baselineMetadata )
        {
            this.baselineMetadata = baselineMetadata;

            return this;
        }

        /**
         * @param leftTransformer the transformer for left-style data
         * @return the builder
         */
        Builder<L, R, B> setLeftTransformer( UnaryOperator<TimeSeries<L>> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the transformer for right-style data
         * @return the builder
         */
        Builder<L, R, B> setRightTransformer( UnaryOperator<TimeSeries<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the transformer for baseline-style data
         * @return the builder
         */
        Builder<L, R, B> setBaselineTransformer( UnaryOperator<TimeSeries<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

            return this;
        }

        /**
         * @param leftFilter the filter for left-style data
         * @return the builder
         */
        Builder<L, R, B> setLeftFilter( Predicate<TimeSeries<L>> leftFilter )
        {
            this.leftFilter = leftFilter;

            return this;
        }

        /**
         * @param rightFilter the filter for right-style data
         * @return the builder
         */
        Builder<L, R, B> setRightFilter( Predicate<TimeSeries<R>> rightFilter )
        {
            this.rightFilter = rightFilter;

            return this;
        }

        /**
         * @param baselineFilter the filter for baseline-style data
         * @return the builder
         */
        Builder<L, R, B> setBaselineFilter( Predicate<TimeSeries<R>> baselineFilter )
        {
            this.baselineFilter = baselineFilter;

            return this;
        }

        /**
         * @param leftMissingFilter the filter for left-style data
         * @return the builder
         */
        Builder<L, R, B> setLeftMissingFilter( Predicate<L> leftMissingFilter )
        {
            this.leftMissingFilter = leftMissingFilter;

            return this;
        }

        /**
         * @param rightMissingFilter the filter for right-style data
         * @return the builder
         */
        Builder<L, R, B> setRightMissingFilter( Predicate<R> rightMissingFilter )
        {
            this.rightMissingFilter = rightMissingFilter;

            return this;
        }

        /**
         * @param baselineMissingFilter the filter for baseline-style data
         * @return the builder
         */
        Builder<L, R, B> setBaselineMissingFilter( Predicate<R> baselineMissingFilter )
        {
            this.baselineMissingFilter = baselineMissingFilter;

            return this;
        }

        /**
         * @param leftTimeShift the time shift for left-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setLeftTimeShift( Duration leftTimeShift )
        {
            if ( Objects.nonNull( leftTimeShift ) )
            {
                this.leftTimeShift = leftTimeShift;
            }

            return this;
        }

        /**
         * @param rightTimeShift the time shift for right-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setRightTimeShift( Duration rightTimeShift )
        {
            if ( Objects.nonNull( rightTimeShift ) )
            {
                this.rightTimeShift = rightTimeShift;
            }

            return this;
        }

        /**
         * @param baselineTimeShift the time shift for baseline-ish valid times
         * @return the builder
         */
        Builder<L, R, B> setBaselineTimeShift( Duration baselineTimeShift )
        {
            if ( Objects.nonNull( baselineTimeShift ) )
            {
                this.baselineTimeShift = baselineTimeShift;
            }

            return this;
        }

        /**
         * @param frequency the frequency at which pairs should be produced.
         * @return the builder
         */
        Builder<L, R, B> setPairFrequency( Duration frequency )
        {
            this.frequency = frequency;

            return this;
        }

        /**
         * Sets the baseline shim to map from baseline-ish to right-ish data.
         * @param baselineShim the baseline shim
         * @return the builder
         */

        Builder<L, R, B> setBaselineShim( Function<TimeSeries<B>, TimeSeries<R>> baselineShim )
        {
            this.baselineShim = baselineShim;

            return this;
        }

        /**
         * Builds a {@link PoolSupplier}.
         *
         * @return a pool supplier
         */

        PoolSupplier<L, R, B> build()
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
                                                     Stream<TimeSeries<B>> baselineData )
    {
        LOGGER.debug( "Creating a pool." );

        Objects.requireNonNull( leftData,
                                "Left data is expected for the creation of pool " + this.getMetadata() + "." );
        Objects.requireNonNull( rightData,
                                "Right data is expected for the creation of pool " + this.getMetadata() + "." );

        // Obtain the desired timescale
        TimeScaleOuter desiredTimeScaleToUse = this.getDesiredTimeScale();

        // Get the paired frequency
        Duration pairedFrequency = this.getPairedFrequency();

        // Pooling is organized to minimize memory usage for right-ish datasets, which tend to be larger (e.g., 
        // forecasts). Thus, each right-ish series is transformed per series in a later loop. However, the left-ish 
        // series are brought into memory, so transform them now. See #95488.        
        // Apply any valid time offset to the left-ish data upfront.
        Duration leftValidOffset = this.getLeftTimeShift();
        UnaryOperator<TimeSeries<L>> mapper = nextSeries -> this.applyValidTimeOffset( nextSeries,
                                                                                       leftValidOffset,
                                                                                       DatasetOrientation.LEFT );
        List<TimeSeries<L>> transformedLeft = leftData.map( mapper )
                                                      .toList();

        // Calculate the main pairs
        TimeSeriesPlusValidation<L, R> mainPairsPlus = this.createPairsPerFeature( transformedLeft.stream(),
                                                                                   rightData,
                                                                                   desiredTimeScaleToUse,
                                                                                   pairedFrequency,
                                                                                   DatasetOrientation.RIGHT,
                                                                                   this.getMetadata()
                                                                                       .getTimeWindow(),
                                                                                   this.getRightTimeShift() );

        List<EvaluationStatusMessage> validationEvents = new ArrayList<>( mainPairsPlus.getEvaluationStatusMessages() );

        // Main pairs indexed by feature tuple
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs = mainPairsPlus.getTimeSeries();

        // Baseline pairs indexed by feature tuple
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs = null;

        // Create the baseline pairs
        if ( this.hasBaseline() )
        {
            // Baseline that is generated?
            if ( this.hasBaselineGenerator() )
            {
                basePairs = mainPairsPlus.getGeneratedBaselineTimeSeries();
            }
            // Regular baseline with supplied time-series
            else
            {
                Stream<TimeSeries<R>> shimmed = baselineData.map( this.getBaselineShim() );
                TimeSeriesPlusValidation<L, R> basePairsPlus =
                        this.createPairsPerFeature( transformedLeft.stream(),
                                                    shimmed,
                                                    desiredTimeScaleToUse,
                                                    pairedFrequency,
                                                    DatasetOrientation.BASELINE,
                                                    this.baselineMetadata.getTimeWindow(),
                                                    this.getBaselineTimeShift() );

                validationEvents.addAll( basePairsPlus.getEvaluationStatusMessages() );
                basePairs = basePairsPlus.getTimeSeries();
            }
        }

        // Cross-pair the main/baseline pairs and/or various sub-pools?
        if ( Objects.nonNull( this.getCrossPairer() ) )
        {
            Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>>
                    cp = this.getCrossPairs( this.getCrossPairer(), this.getCrossPair(), mainPairs, basePairs );

            mainPairs = cp.getLeft();
            basePairs = cp.getRight();
        }

        // Create the pool from the paired time-series
        Pool<TimeSeries<Pair<L, R>>> pool = this.createPoolFromPairs( mainPairs,
                                                                      basePairs,
                                                                      desiredTimeScaleToUse,
                                                                      validationEvents );

        if ( LOGGER.isDebugEnabled() )
        {
            int pairCount = PoolSlicer.getEventCount( pool );

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
     * @param desiredTimeScale the desired timescale, optional
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

        // No pairs for any features? Then build an empty mini-pool for each feature, as needed
        mainPairs = this.getExistingPoolsOrEmpty( mainPairs, this.getMetadata() );
        basePairs = this.getExistingPoolsOrEmpty( basePairs, this.getBaselineMetadata() );

        Pool.Builder<TimeSeries<Pair<L, R>>> builder = new Pool.Builder<>();

        // Create and set the climatology
        Climatology nextClimatology = null;
        if ( this.hasClimatology() )
        {
            nextClimatology = this.getClimatology()
                                  .get();
            builder.setClimatology( nextClimatology );
        }

        // Create the mini pools, one per feature
        // TODO: this assumes that an empty main means empty baseline too. Probably need to relax because baseline
        // statistics can be requested independently of right statistics
        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();

            // If there are no pairs for this feature, warn
            if ( this.areNoPairs( nextMainPairs ) )
            {
                Set<Feature> availableFeatures = mainPairs.keySet()
                                                          .stream()
                                                          .filter( next -> !next.equals( nextFeature ) )
                                                          .map( FeatureTuple::getRight )
                                                          .collect( Collectors.toSet() );

                LOGGER.warn( "When evaluating a pool for time window {}, failed to identify any pairs for feature: {}. "
                             + "Pairs were available for these features: {}.",
                             this.getMetadata().getTimeWindow(),
                             nextFeature.getRight(),
                             availableFeatures );
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

                // If there are no baseline pairs for this feature, warn
                if ( this.areNoPairs( nextBasePairs ) )
                {
                    Set<Feature> availableFeatures = basePairs.keySet()
                                                              .stream()
                                                              .filter( next -> !next.equals( nextFeature ) )
                                                              .map( FeatureTuple::getBaseline )
                                                              .collect( Collectors.toSet() );

                    LOGGER.warn( "When evaluating a pool for time window {}, failed to identify any baseline pairs for "
                                 + "feature: {}. Baseline pairs were available for these features: {}.",
                                 this.getMetadata().getTimeWindow(),
                                 nextFeature.getBaseline(),
                                 availableFeatures );
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

        // Set the metadata, adjusted to include the desired timescale
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
     * @param pairs the pairs to check
     * @return true if there are no pairs, otherwise false
     */
    private boolean areNoPairs( List<TimeSeries<Pair<L, R>>> pairs )
    {
        if ( Objects.isNull( pairs ) )
        {
            return true;
        }

        return pairs.stream()
                    .mapToInt( next -> next.getEvents()
                                           .size() )
                    .sum() == 0;
    }

    /**
     * Adjusts the input metadata to represent the supplied feature and timescale.
     * @param metadata the input metadata
     * @param feature the feature to use when adjusting the input metadata
     * @param timeScale the timescale
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
     * @param desiredTimeScale the desired time-scale
     * @param frequency the frequency with which to create pairs at the desired time scale
     * @param rightOrBaselineOrientation the orientation of the non-left data, one of 
     *            {@link DatasetOrientation#RIGHT} or {@link DatasetOrientation#BASELINE}, required
     * @param timeWindow the time window to snip the pairs
     * @param timeShift the valid time shift to apply
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
                                                                  DatasetOrientation rightOrBaselineOrientation,
                                                                  TimeWindowOuter timeWindow,
                                                                  Duration timeShift )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( rightOrBaseline );
        Objects.requireNonNull( rightOrBaselineOrientation );

        UnaryOperator<TimeSeries<R>> rightOrBaselineTransformer =
                this.getRightOrBaselineTransformer( rightOrBaselineOrientation );

        Predicate<TimeSeries<L>> leftFilterInner = this.getLeftFilter();
        Predicate<TimeSeries<R>> rightOrBaselineFilter = this.getRightOrBaselineFilter( rightOrBaselineOrientation );
        Predicate<R> rightOrBaselineMissingFilter = this.getRightOrBaselineMissingFilter( rightOrBaselineOrientation );

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
        Function<TimeSeries<?>, TimeSeries<R>> baselineGeneratorFunction = null;

        if ( this.hasBaselineGenerator() )
        {
            Set<Feature> allBaselineFeatures = this.getBaselineFeatures();
            Function<Set<Feature>, BaselineGenerator<R>> generatorSupplier =
                    this.getBaselineGenerator();
            baselineGeneratorFunction = generatorSupplier.apply( allBaselineFeatures );
        }

        Transformers<R> rightOrBaselineTransformers = new Transformers<>( rightOrBaselineTransformer,
                                                                          rightOrBaselineMissingFilter,
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
                                                                                  timeShift,
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
                                                       rightOrBaselineTransformers );

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

            if ( rightOrBaselineOrientation == DatasetOrientation.BASELINE )
            {
                metaToReport = this.baselineMetadata;
            }

            LOGGER.debug( "While creating pool {}, discovered {} {} time-series and {} {} time-series from "
                          + "which to create pairs. Created {} paired time-series from these inputs.",
                          metaToReport,
                          leftSeries.size(),
                          DatasetOrientation.LEFT,
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
     * @param rightOrBaselineTransformers the transformers for right-ish data
     * @return the pairs plus validation
     * @throws NullPointerException if any required input is null
     */

    private List<TimeSeriesPlusValidation<L, R>> createPairsPerLeftSeries( List<TimeSeries<L>> leftSeries,
                                                                           TimeSeries<R> rightOrBaselineSeries,
                                                                           TimeScaleOuter desiredTimeScale,
                                                                           Duration frequency,
                                                                           DatasetOrientation rightOrBaselineOrientation,
                                                                           TimeWindowOuter timeWindow,
                                                                           Transformers<R> rightOrBaselineTransformers )
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
                                                                               rightOrBaselineTransformers );

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
     * @param desiredTimeScale the desired timescale
     * @param frequency the frequency with which to create pairs at the desired timescale
     * @param timeWindow the time window to snip the pairs
     * @param orientation the orientation of the non-left data, one of {@link DatasetOrientation#RIGHT} or
     *            {@link DatasetOrientation#BASELINE}
     * @param rightOrBaselineTransformers the right-ish transformers
     * @return a paired time-series
     * @throws NullPointerException if the left, rightOrBaseline or timeWindow is null
     */

    private TimeSeriesPlusValidation<L, R> createSeriesPairs( TimeSeries<L> left,
                                                              TimeSeries<R> rightOrBaseline,
                                                              TimeScaleOuter desiredTimeScale,
                                                              Duration frequency,
                                                              TimeWindowOuter timeWindow,
                                                              DatasetOrientation orientation,
                                                              Transformers<R> rightOrBaselineTransformers )
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

        // No events on one or both sides? Stop here.
        if ( scaledLeft.getEvents()
                       .isEmpty() || scaledRight.getEvents()
                                                .isEmpty() )
        {
            LOGGER.debug( "Skipping the creation of series pairs because one or more time-series had no events to "
                          + "pair." );

            // Return the empty pairs
            return new TimeSeriesPlusValidation<>( Map.of(),
                                                   Map.of(),
                                                   List.of() );
        }

        boolean upscaleLeft = Objects.nonNull( desiredTimeScale )
                              && Objects.nonNull( left.getTimeScale() )
                              && TimeScaleOuter.isRescalingRequired( left.getTimeScale(), desiredTimeScale )
                              && !desiredTimeScale.equals( left.getTimeScale() );

        boolean upscaleRight = Objects.nonNull( desiredTimeScale )
                               && Objects.nonNull( rightOrBaseline.getTimeScale() )
                               && TimeScaleOuter.isRescalingRequired( rightOrBaseline.getTimeScale(),
                                                                      desiredTimeScale );

        List<EvaluationStatusMessage> statusEvents = new ArrayList<>();

        // Intersecting end times for upscaling
        SortedSet<Instant> endsAt = this.getEndTimesForUpscalingIfNeeded( upscaleLeft,
                                                                          upscaleRight,
                                                                          scaledLeft,
                                                                          scaledRight,
                                                                          timeWindow,
                                                                          desiredTimeScale,
                                                                          frequency );

        // Upscale left?
        if ( upscaleLeft )
        {
            PoolRescalingEvent rescalingMonitor = PoolRescalingEvent.of( RescalingType.UPSCALED, // Monitor
                                                                         DatasetOrientation.LEFT,
                                                                         left.getMetadata() );

            rescalingMonitor.begin();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Upscaling {} time-series {} from {} to {}.",
                              DatasetOrientation.LEFT,
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
                              DatasetOrientation.LEFT,
                              left.hashCode(),
                              left.getTimeScale(),
                              desiredTimeScale,
                              scaledLeft.hashCode() );
            }

            // Log any warnings
            RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( left, upscaledLeft.getValidationEvents() );

            rescalingMonitor.commit();
        }

        // Upscale right?
        if ( upscaleRight )
        {
            PoolRescalingEvent rescalingMonitor = PoolRescalingEvent.of( RescalingType.UPSCALED, // Monitor
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
            RescaledTimeSeriesPlusValidation.logScaleValidationWarnings( rightOrBaseline,
                                                                         upscaledRight.getValidationEvents() );

            rescalingMonitor.commit();
        }

        // Transform the rescaled values (e.g., this could contain unit transformations, among others)
        TimeSeries<L> scaledAndTransformedLeft = this.getLeftTransformer()
                                                     .apply( scaledLeft );

        TimeSeries<R> scaledAndTransformedRight = rightOrBaselineTransformers.rightTransformer()
                                                                             .apply( scaledRight );

        // Remove any missing values
        TimeSeries<L> filteredLeft = TimeSeriesSlicer.filter( scaledAndTransformedLeft,
                                                              this.getLeftMissingFilter() );

        TimeSeries<R> filteredRight = TimeSeriesSlicer.filter( scaledAndTransformedRight,
                                                               rightOrBaselineTransformers.rightMissingFilter() );

        // Create the pairs, if any
        TimeSeries<Pair<L, R>> pairs = this.getPairer()
                                           .pair( filteredLeft, filteredRight );

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
                          DatasetOrientation.LEFT,
                          scaledAndTransformedLeft.getMetadata(),
                          scaledAndTransformedLeft.getEvents()
                                                  .size(),
                          orientation,
                          scaledAndTransformedRight.getMetadata(),
                          scaledAndTransformedRight.getEvents()
                                                   .size(),
                          snippedPairs.getEvents()
                                      .size(),
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

        // Do we also need to generate baseline pairs using the right as a template?
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaseline =
                this.getGeneratedBaseline( orientation,
                                           rightOrBaselineTransformers,
                                           featureTuples,
                                           scaledAndTransformedLeft,
                                           scaledAndTransformedRight );

        return new TimeSeriesPlusValidation<>( pairsToSave,
                                               generatedBaseline,
                                               statusEvents );
    }

    /**
     * Returns the times at which upscaled values should end, if upscaling is needed.
     * @param upscaleLeft whether to upscale the let-ish time-series
     * @param upscaleRight whether to upscae the right-ish time-series
     * @param scaledLeft the left-ish time-series
     * @param scaledRight the right-ish time-series
     * @param timeWindow the time window
     * @param desiredTimeScale the desired timescale
     * @param frequency the frequency
     * @return the end times for upscaling
     */

    private SortedSet<Instant> getEndTimesForUpscalingIfNeeded( boolean upscaleLeft,
                                                                boolean upscaleRight,
                                                                TimeSeries<L> scaledLeft,
                                                                TimeSeries<R> scaledRight,
                                                                TimeWindowOuter timeWindow,
                                                                TimeScaleOuter desiredTimeScale,
                                                                Duration frequency )
    {
        // Get the end times for paired values if upscaling is required. If upscaling both the left and right, the
        // superset of intersecting times is thinned to a regular sequence that depends on the desired timescale and
        // frequency
        SortedSet<Instant> endsAt = new TreeSet<>();
        if ( upscaleLeft || upscaleRight )
        {
            endsAt = TimeSeriesSlicer.getRegularSequenceOfIntersectingTimes( scaledLeft,
                                                                             scaledRight,
                                                                             timeWindow,
                                                                             desiredTimeScale,
                                                                             frequency );
        }

        return Collections.unmodifiableSortedSet( endsAt );
    }

    /**
     * Creates a generated baseline dataset, as needed.
     * @param orientation the dataset orientation
     * @param rightOrBaselineTransformers the right or baseline transformers
     * @param featureTuples the features
     * @param scaledAndTransformedLeft the scaled and transformed left-ish time-series
     * @param scaledAndTransformedRight the scaled and transformed right-ish time-series
     * @return the generated baseline time-series, if needed
     */

    private Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> getGeneratedBaseline( DatasetOrientation orientation,
                                                                                  Transformers<R> rightOrBaselineTransformers,
                                                                                  Set<FeatureTuple> featureTuples,
                                                                                  TimeSeries<L> scaledAndTransformedLeft,
                                                                                  TimeSeries<R> scaledAndTransformedRight )
    {
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaseline = new HashMap<>();
        if ( this.hasBaselineGenerator()
             && orientation == DatasetOrientation.RIGHT )
        {
            generatedBaseline = this.getGeneratedBaselinePairs( rightOrBaselineTransformers,
                                                                featureTuples,
                                                                scaledAndTransformedLeft,
                                                                scaledAndTransformedRight );
        }

        return Collections.unmodifiableMap( generatedBaseline );
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

    private TimeSeries<R> getGeneratedBaseline( Function<TimeSeries<?>, TimeSeries<R>> baselineGenerator,
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
                                                              DatasetOrientation rightOrBaselineOrientation )
    {
        // Find the feature-tuple context for the pairs to add. The pairs may appear for more than one feature tuple. 
        // For example, the same left/right pairs may appear in N baseline contexts
        Set<FeatureTuple> nextFeatures;

        if ( rightOrBaselineOrientation == DatasetOrientation.RIGHT )
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
     * Performs cross-pairing of the main and baseline datasets.
     * @param crossPairer the cross-pairer function
     * @param crossPair the cross-pairing declaration
     * @param mainPairs the main pairs
     * @param basePairs the baseline pairs
     * @return the cross-pairs
     */

    private Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>>
    getCrossPairs( TimeSeriesCrossPairer<Pair<L, R>> crossPairer,
                   CrossPair crossPair,
                   Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs,
                   Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs )
    {
        // Cross-pairing across all features?
        if ( crossPair.scope() == CrossPairScope.ACROSS_FEATURES )
        {
            return this.getCrossPairsFull( crossPairer, mainPairs, basePairs );
        }

        // Cross-pairing per feature
        return this.getCrossPairsPartial( crossPairer, mainPairs, basePairs );
    }

    /**
     * Performs cross-pairing of the main and baseline datasets for each feature separately.
     *
     * @see #getCrossPairsFull(TimeSeriesCrossPairer, Map, Map)
     * @param crossPairer the cross-pairer function
     * @param mainPairs the main pairs
     * @param basePairs the baseline pairs
     * @return the cross-pairs
     */

    private Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>>
    getCrossPairsPartial( TimeSeriesCrossPairer<Pair<L, R>> crossPairer,
                          Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs,
                          Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs )
    {
        LOGGER.debug( "Conducting cross-pairing of {} and {}.",
                      this.getMetadata(),
                      this.getBaselineMetadata() );

        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairsCrossed = new HashMap<>();
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairsCrossed = new HashMap<>();

        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();

            List<TimeSeries<Pair<L, R>>> nextMainPairs = nextEntry.getValue();
            if ( basePairs.containsKey( nextFeature ) )
            {
                List<TimeSeries<Pair<L, R>>> nextBasePairs = basePairs.get( nextFeature );
                CrossPairs<Pair<L, R>> crossPairs = crossPairer.apply( nextMainPairs, nextBasePairs );
                mainPairsCrossed.put( nextFeature, crossPairs.getFirstPairs() );
                basePairsCrossed.put( nextFeature, crossPairs.getSecondPairs() );
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
     * Performs cross-pairing of the main and baseline datasets for all features together. In other words, only those
     * times that are present in both the main and baseline datasets across all features will be retained.
     *
     * @see #getCrossPairsPartial(TimeSeriesCrossPairer, Map, Map)
     * @param crossPairer the cross-pairer function
     * @param mainPairs the main pairs
     * @param basePairs the baseline pairs
     * @return the cross-pairs
     */

    private Pair<Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>, Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>>>
    getCrossPairsFull( TimeSeriesCrossPairer<Pair<L, R>> crossPairer,
                       Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainPairs,
                       Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> basePairs )
    {
        List<List<TimeSeries<Pair<L, R>>>> combined = new ArrayList<>( mainPairs.values() );
        // Add the base pairs, if available
        if ( Objects.nonNull( basePairs ) )
        {
            combined.addAll( basePairs.values() );
        }

        List<TimeSeries<Pair<L, R>>> first = combined.get( 0 );
        for ( int i = 1; i < combined.size(); i++ )
        {
            List<TimeSeries<Pair<L, R>>> next = combined.get( i );
            first = crossPairer.apply( first, next )
                               .getFirstPairs();
        }

        // Cross-pair all main pairs against the first set of pairs
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> mainFinal = new HashMap<>();
        for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : mainPairs.entrySet() )
        {
            FeatureTuple nextFeature = nextEntry.getKey();
            List<TimeSeries<Pair<L, R>>> nextPairs = nextEntry.getValue();
            CrossPairs<Pair<L, R>> crossPaired = crossPairer.apply( nextPairs, first );
            mainFinal.put( nextFeature, crossPaired.getFirstPairs() );
        }
        mainFinal = Collections.unmodifiableMap( mainFinal );

        // Cross-pair all baseline pairs against the first set of pairs
        Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> baseFinal = null;
        if ( Objects.nonNull( basePairs ) )
        {
            baseFinal = new HashMap<>();
            for ( Map.Entry<FeatureTuple, List<TimeSeries<Pair<L, R>>>> nextEntry : basePairs.entrySet() )
            {
                FeatureTuple nextFeature = nextEntry.getKey();
                List<TimeSeries<Pair<L, R>>> nextPairs = nextEntry.getValue();
                CrossPairs<Pair<L, R>> crossPaired = crossPairer.apply( nextPairs, first );
                baseFinal.put( nextFeature, crossPaired.getFirstPairs() );
            }
            baseFinal = Collections.unmodifiableMap( baseFinal );
        }

        return Pair.of( mainFinal, baseFinal );
    }

    /**
     * Discovers the left features associated with the specified right or baseline feature.
     * @param rightOrBaselineFeature the left feature
     * @param lrb the orientation of the feature, right or baseline
     * @return the left features for the specified right or baseline feature
     */

    private Set<Feature> getLeftFeaturesForRightOrBaseline( Feature rightOrBaselineFeature, DatasetOrientation lrb )
    {
        Set<Feature> correlated;

        if ( lrb == DatasetOrientation.RIGHT )
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
     * @return the left time shift
     */

    private Duration getLeftTimeShift()
    {
        return this.leftTimeShift;
    }

    /**
     * @return the right time shift
     */

    private Duration getRightTimeShift()
    {
        return this.rightTimeShift;
    }

    /**
     * @return the baseline time shift
     */

    private Duration getBaselineTimeShift()
    {
        return this.baselineTimeShift;
    }

    /**
     * Returns the period from the prescribed timescale or <code>null</code> if the timescale is <code>null</code>.
     *
     * @return the period associated with the timescale or null
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

    private TimeSeriesUpscaler<R> getRightOrBaselineUpscaler( DatasetOrientation lrb )
    {
        if ( lrb == DatasetOrientation.RIGHT )
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
     * @return the missing filter for left-ish data
     */

    private Predicate<L> getLeftMissingFilter()
    {
        return this.leftMissingFilter;
    }

    /**
     * @return the missing filter for right-ish data
     */

    private Predicate<R> getRightMissingFilter()
    {
        return this.rightMissingFilter;
    }

    /**
     * @return the missing filter for baseline-ish data
     */

    private Predicate<R> getBaselineMissingFilter()
    {
        return this.baselineMissingFilter;
    }

    /**
     * @param lrb the orientation of the required transformer
     * @return the transformer
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private UnaryOperator<TimeSeries<R>> getRightOrBaselineTransformer( DatasetOrientation lrb )
    {
        Objects.requireNonNull( lrb );

        return switch ( lrb )
        {
            case RIGHT -> this.getRightTransformer();
            case BASELINE -> this.getBaselineTransformer();
            default -> throw new IllegalArgumentException( "Unexpected orientation for transformer: " + lrb
                                                           + ". "
                                                           + EXPECTED
                                                           + DatasetOrientation.RIGHT
                                                           + " or "
                                                           + DatasetOrientation.BASELINE
                                                           + "." );
        };
    }

    /**
     * @param lrb the orientation of the required filter
     * @return the filter
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private Predicate<TimeSeries<R>> getRightOrBaselineFilter( DatasetOrientation lrb )
    {
        Objects.requireNonNull( lrb );

        return switch ( lrb )
        {
            case RIGHT -> this.getRightFilter();
            case BASELINE -> this.getBaselineFilter();
            default -> throw new IllegalArgumentException( "Unexpected orientation for filter: " + lrb
                                                           + ". "
                                                           + EXPECTED
                                                           + DatasetOrientation.RIGHT
                                                           + " or "
                                                           + DatasetOrientation.BASELINE
                                                           + "." );
        };
    }

    /**
     * @param lrb the orientation of the required filter
     * @return the filter
     * @throws NullPointerException if the orientation is null
     * @throws IllegalArgumentException if the orientation is unexpected
     */

    private Predicate<R> getRightOrBaselineMissingFilter( DatasetOrientation lrb )
    {
        Objects.requireNonNull( lrb );

        return switch ( lrb )
        {
            case RIGHT -> this.getRightMissingFilter();
            case BASELINE -> this.getBaselineMissingFilter();
            default -> throw new IllegalArgumentException( "Unexpected orientation for missing filter: " + lrb
                                                           + ". "
                                                           + EXPECTED
                                                           + DatasetOrientation.RIGHT
                                                           + " or "
                                                           + DatasetOrientation.BASELINE
                                                           + "." );
        };
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
     * @return the cross-pairer
     */

    private TimeSeriesCrossPairer<Pair<L, R>> getCrossPairer()
    {
        return this.crossPairer;
    }

    /**
     * @return the cross-pairer declaration
     */

    private CrossPair getCrossPair()
    {
        return this.crossPair;
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
     * Returns the frequency at which to create pairs. Either the explicitly declared pair frequency or the period
     * associated with the desired time-scale.
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
                                                    DatasetOrientation lrb )
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

    private Function<Set<Feature>, BaselineGenerator<R>> getBaselineGenerator()
    {
        return this.baselineGenerator;
    }

    /**
     * A baseline shim to map from baseline-ish to right-ish data.
     * @return the baseline shim
     */

    private Function<TimeSeries<B>, TimeSeries<R>> getBaselineShim()
    {
        return this.baselineShim;
    }

    /**
     * Hidden constructor.  
     *
     * @param builder the builder
     * @throws NullPointerException if a required input is null
     * @throws IllegalArgumentException if some input is inconsistent
     */

    private PoolSupplier( Builder<L, R, B> builder )
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
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.frequency = builder.frequency;
        this.crossPairer = builder.crossPairer;
        this.crossPair = builder.crossPair;
        this.leftFilter = builder.leftFilter;
        this.rightFilter = builder.rightFilter;
        this.baselineFilter = builder.baselineFilter;
        this.leftMissingFilter = builder.leftMissingFilter;
        this.rightMissingFilter = builder.rightMissingFilter;
        this.baselineMissingFilter = builder.baselineMissingFilter;
        this.climatology = builder.climatology;
        this.leftTimeShift = builder.leftTimeShift;
        this.rightTimeShift = builder.rightTimeShift;
        this.baselineTimeShift = builder.baselineTimeShift;
        this.baselineShim = builder.baselineShim;

        // Set any time offsets required
        this.featureCorrelator = FeatureCorrelator.of( this.metadata.getFeatureTuples() );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered these time offsets by data type: left={}, right={} and baseline={}.",
                          this.metadata,
                          this.leftTimeShift,
                          this.rightTimeShift,
                          this.baselineTimeShift );
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
        Objects.requireNonNull( this.baselineTransformer, messageStart + "add a transformer for the baseline "
                                                          + "data." );
        Objects.requireNonNull( this.leftFilter, messageStart + "add a filter for the left data." );
        Objects.requireNonNull( this.rightFilter, messageStart + "add a filter for the right data." );
        Objects.requireNonNull( this.baselineFilter, messageStart + "add a filter for the baseline data." );
        Objects.requireNonNull( this.leftMissingFilter, messageStart + "add a filter for missing left data." );
        Objects.requireNonNull( this.rightMissingFilter, messageStart + "add a filter for missing right data." );
        Objects.requireNonNull( this.baselineMissingFilter, messageStart + "add a filter for missing baseline "
                                                            + "data." );

        if ( Objects.isNull( this.baselineGenerator ) )
        {
            Objects.requireNonNull( this.baselineShim, "add a baseline shim when there is no baseline "
                                                       + "generator." );
        }

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

        // If adding a baseline, baseline metadata is needed. If not, it should not be supplied
        if ( ( Objects.isNull( this.baseline )
               && Objects.isNull( this.baselineGenerator ) ) != Objects.isNull( this.baselineMetadata ) )
        {
            throw new IllegalArgumentException( messageStart + "cannot add a baseline retriever without baseline "
                                                + "metadata and vice versa." );
        }

        // Cannot supply two baseline sources
        if ( Objects.nonNull( this.baseline )
             && Objects.nonNull( this.baselineGenerator ) )
        {
            throw new IllegalArgumentException( messageStart + "cannot add a baseline data source and a baseline "
                                                + "generator to the same pool: only one is required." );
        }

        // The cross pairing function and declaration should both be present or absent
        if ( Objects.nonNull( this.crossPairer ) != Objects.nonNull( this.crossPair ) )
        {
            throw new IllegalArgumentException( messageStart + "either set the cross-pairing function and declaration "
                                                + "or set neither." );
        }
    }

    /**
     * A value class for storing time series plus their associated validation events.
     *
     * @author James Brown
     * @param <L> the left-ish data type
     * @param <R> the right-ish data type
     * @param series  The paired time-series.
     * @param generatedBaselineSeries  The generated baseline time-series, where applicable.
     * @param statusEvents  The status events.
     */
    private record TimeSeriesPlusValidation<L, R>( Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> series,
                                                   Map<FeatureTuple, List<TimeSeries<Pair<L, R>>>> generatedBaselineSeries,
                                                   List<EvaluationStatusMessage> statusEvents )
    {
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
         * @param statusEvents the validation events
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

    /** Record class to bundle right-ish transformers and filters. */
    private record Transformers<R>( UnaryOperator<TimeSeries<R>> rightTransformer,
                                    Predicate<R> rightMissingFilter,
                                    Function<TimeSeries<?>, TimeSeries<R>> baselineGenerator )
    {
    }
}