package wres.io.pooling;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.ThreadSafe;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.pools.pairs.PairingException;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.ScaleValidationEvent;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.ScaleValidationEvent.EventType;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
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
import wres.io.retrieval.NoSuchUnitConversionException;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.LeftOrRightOrBaseline;

/**
 * <p>Supplies a {@link PoolOfPairs}, which is used to compute one or more verification statistics. The overall 
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
 * <p>This class is thread safe.
 * 
 * @author james.brown@hydrosolved.com
 * @param <L> the type of left value in each pair
 * @param <R> the type of right value in each pair and, where applicable, the type of baseline value
 */

@ThreadSafe
public class PoolSupplier<L, R> implements Supplier<Pool<Pair<L, R>>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolSupplier.class );

    /**
     * If <code>true</code>, when conducting upscaling, target periods that increment regularly, <code>false</code>
     * to include all possible pairs. In this context, regular means back-to-back or overlapping with a fixed 
     * frequency, but not all possible pairs, which may include pairs that do not increment with a regular frequency,
     * but are nevertheless valid pairs.
     */

    private static final boolean REGULAR_PAIRS = true;

    /**
     * Message re-used several times.
     */

    private static final String WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR = "While constructing a pool supplier for {}, ";

    /**
     * Set of reference times for observation-like time-series.
     */

    private static final Set<ReferenceTimeType> OBSERVATION_REFERENCE_TIME_TYPE =
            Set.of( ReferenceTimeType.LATEST_OBSERVATION );

    /**
     * Climatological data source at the desired time scale.
     */

    private final Supplier<Stream<TimeSeries<L>>> climatology;

    /**
     * Mapper from the left-type of climatological data to a double-type.
     */

    private final ToDoubleFunction<L> climatologyMapper;

    /**
     * Left data source.
     */

    private final Supplier<Stream<TimeSeries<L>>> left;

    /**
     * Right data source.
     */

    private final Supplier<Stream<TimeSeries<R>>> right;

    /**
     * Baseline data source. Optional.
     */

    private final Supplier<Stream<TimeSeries<R>>> baseline;

    /**
     * Generator for baseline data source. Optional.
     */

    private final UnaryOperator<TimeSeries<R>> baselineGenerator;

    /**
     * Pairer.
     */

    private final TimeSeriesPairer<L, R> pairer;

    /**
     * An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time.
     */

    private final TimeSeriesCrossPairer<L, R> crossPairer;

    /**
     * Upscaler for left-type data. Optional on construction, but may be exceptional if later required.
     */

    private final TimeSeriesUpscaler<L> leftUpscaler;

    /**
     * Upscaler for right-type data. Optional on construction, but may be exceptional if later required.
     */

    private final TimeSeriesUpscaler<R> rightUpscaler;

    /**
     * The desired time scale.
     */

    private final TimeScaleOuter desiredTimeScale;

    /**
     * Metadata for the mains pairs.
     */

    private final PoolMetadata metadata;

    /**
     * Metadata for the baseline pairs.
     */

    private final PoolMetadata baselineMetadata;

    /**
     * The inputs declaration, which is used to help compute the desired time scale, if required.
     */

    private final Inputs inputs;

    /**
     * A function that transforms according to value. Used to apply value constraints to the left-style data.
     */

    private final UnaryOperator<L> leftTransformer;

    /**
     * A function that transforms right-ish data and may consider the encapsulating event.
     */

    private final UnaryOperator<Event<R>> rightTransformer;

    /**
     * A function that transforms baseline-ish data and may consider the encapsulating event.
     */

    private final UnaryOperator<Event<R>> baselineTransformer;

    /**
     * An offset to apply to the valid times of the left data.
     */

    private final Duration leftOffset;

    /**
     * An offset to apply to the valid times of the right data.
     */

    private final Duration rightOffset;

    /**
     * An offset to apply to the valid times of the baseline data.
     */

    private final Duration baselineOffset;

    /**
     * Frequency with which pairs should be constructed at the desired time scale.
     */

    private final Duration frequency;

    /**
     * Returns a {@link Pool} for metric calculation.
     * 
     * @return a pool of pairs
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     */

    @Override
    public Pool<Pair<L, R>> get()
    {
        return this.createPool();
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

    private Pool<Pair<L, R>> createPool()
    {
        LOGGER.debug( "Creating pool {}.", this.metadata );
        PoolCreationEvent poolMonitor = PoolCreationEvent.of( this.metadata ); // Monitor
        poolMonitor.begin();

        PoolOfPairs.Builder<L, R> builder = new PoolOfPairs.Builder<>();

        // Left data provided or is climatology the left data?
        Stream<TimeSeries<L>> cStream;

        RetrievalEvent leftEvent = RetrievalEvent.of( LeftOrRightOrBaseline.LEFT, this.metadata ); // Monitor
        leftEvent.begin();

        if ( Objects.nonNull( this.left ) )
        {
            cStream = this.left.get();
        }
        else
        {
            cStream = this.climatology.get();
        }

        leftEvent.commit();

        List<TimeSeries<L>> leftData = cStream.collect( Collectors.toList() );
        
        RetrievalEvent rightEvent = RetrievalEvent.of( LeftOrRightOrBaseline.RIGHT, this.metadata ); // Monitor
        rightEvent.begin();
        List<TimeSeries<R>> rightData = this.right.get()
                                                  .collect( Collectors.toList() );
        rightEvent.commit();

        List<TimeSeries<R>> baselineData = null;

        // Baseline that is not generated?
        if ( this.hasBaseline() && Objects.isNull( this.baselineGenerator ) )
        {
            RetrievalEvent baselineEvent = RetrievalEvent.of( LeftOrRightOrBaseline.BASELINE, // Monitor
                                                              this.baselineMetadata );
            baselineData = this.baseline.get()
                                        .collect( Collectors.toList() );
            baselineEvent.commit();
        }

        // Apply any time offsets immediately, in order to simplify further evaluation,
        // which is then in the target time system
        leftData = this.applyValidTimeOffset( leftData, this.leftOffset, LeftOrRightOrBaseline.LEFT );
        rightData = this.applyValidTimeOffset( rightData, this.rightOffset, LeftOrRightOrBaseline.RIGHT );
        baselineData = this.applyValidTimeOffset( baselineData, this.baselineOffset, LeftOrRightOrBaseline.BASELINE );

        // Obtain the desired time scale. If this is unavailable, use the Least Common Scale.
        TimeScaleOuter desiredTimeScaleToUse =
                this.getDesiredTimeScale( leftData, rightData, baselineData, this.inputs );

        // Set the metadata, adjusted to include the desired time scale
        PoolMetadata sampleMetadata = PoolMetadata.of( this.metadata, desiredTimeScaleToUse );
        builder.setMetadata( sampleMetadata );

        // The left data is most likely to contain a large set of observations, such as climatology
        // Snipping a large observation-like dataset helps with performance and does not affect accuracy
        // For now, only apply to the left side, as this is most likely to contain observation-like data that extends
        // far beyond the bounds of the right data
        leftData = this.snip( leftData, rightData );

        // Consolidate any observation-like time-series as these values can be shared/combined (e.g., when rescaling)
        leftData = this.consolidateTimeSeriesWithZeroReferenceTimes( leftData );
        rightData = this.consolidateTimeSeriesWithZeroReferenceTimes( rightData );
        baselineData = this.consolidateTimeSeriesWithZeroReferenceTimes( baselineData );

        // Get the paired frequency
        Duration pairedFrequency = this.getPairedFrequency();

        List<TimeSeries<Pair<L, R>>> mainPairs = this.createPairs( leftData,
                                                                   rightData,
                                                                   this.getRightTransformer(),
                                                                   desiredTimeScaleToUse,
                                                                   pairedFrequency,
                                                                   LeftOrRightOrBaseline.RIGHT,
                                                                   sampleMetadata.getTimeWindow() );

        // Create the baseline pairs
        if ( this.hasBaseline() )
        {
            PoolMetadata baselineSampleMetadata = PoolMetadata.of( this.baselineMetadata,
                                                                   desiredTimeScaleToUse );

            builder.setMetadataForBaseline( baselineSampleMetadata );

            LOGGER.debug( "Adding pairs for baseline to pool {}, which have metadata {}.",
                          this.metadata,
                          baselineSampleMetadata );

            // Baseline that is generated?
            if ( Objects.nonNull( this.baselineGenerator ) )
            {
                baselineData = this.createBaseline( this.baselineGenerator, mainPairs );
            }

            List<TimeSeries<Pair<L, R>>> basePairs = this.createPairs( leftData,
                                                                       baselineData,
                                                                       this.getBaselineTransformer(),
                                                                       desiredTimeScaleToUse,
                                                                       pairedFrequency,
                                                                       LeftOrRightOrBaseline.BASELINE,
                                                                       baselineSampleMetadata.getTimeWindow() );

            // Cross-pair?
            if ( Objects.nonNull( this.crossPairer ) )
            {
                LOGGER.debug( "Conducting cross-pairing of {} and {}.", this.metadata, this.baselineMetadata );

                CrossPairs<L, R> crossPairs = this.crossPairer.apply( mainPairs, basePairs );
                mainPairs = crossPairs.getMainPairs();
                basePairs = crossPairs.getBaselinePairs();
            }

            // Add baseline the pairs to the builder
            basePairs.forEach( builder::addTimeSeriesForBaseline );
        }

        // Add the main pairs to the builder
        mainPairs.forEach( builder::addTimeSeries );

        VectorOfDoubles clim = this.getClimatology();
        builder.setClimatology( clim );

        // Create the pairs
        PoolOfPairs<L, R> returnMe = builder.build();

        LOGGER.debug( "Finished creating pool {}, which contains {} pairs.",
                      this.metadata,
                      returnMe.getRawData().size() );

        poolMonitor.commit();

        return returnMe;
    }

    /**
     * Builder for a {@link PoolSupplier}.
     * 
     * @author james.brown@hydrosolved.com
     * @param <L> the left type of paired value
     * @param <R> the right type of paired value and, where required, the baseline type
     */

    static class Builder<L, R>
    {

        /**
         * Climatological data source. Optional.
         */

        private Supplier<Stream<TimeSeries<L>>> climatology;

        /**
         * Climatology mapper.
         */

        private ToDoubleFunction<L> climatologyMapper;

        /**
         * Left data source.
         */

        private Supplier<Stream<TimeSeries<L>>> left;

        /**
         * Right data source.
         */

        private Supplier<Stream<TimeSeries<R>>> right;

        /**
         * Baseline data source. Optional.
         */

        private Supplier<Stream<TimeSeries<R>>> baseline;

        /**
         * Generator for baseline data source. Optional.
         */

        private UnaryOperator<TimeSeries<R>> baselineGenerator;

        /**
         * Pairer.
         */

        private TimeSeriesPairer<L, R> pairer;

        /**
         * Upscaler for left-type data. Optional on construction, but may be exceptional if later required.
         */

        private TimeSeriesUpscaler<L> leftUpscaler;

        /**
         * Upscaler for right-type data. Optional on construction, but may be exceptional if later required.
         */

        private TimeSeriesUpscaler<R> rightUpscaler;

        /**
         * The desired time scale.
         */

        private TimeScaleOuter desiredTimeScale;

        /**
         * Metadata for the mains pairs.
         */

        private PoolMetadata metadata;

        /**
         * Metadata for the baseline pairs.
         */

        private PoolMetadata baselineMetadata;

        /**
         * Inputs declaration for the pool.
         */

        private Inputs inputs;

        /**
         * A function that transforms according to value. Used to apply value constraints to the left-style data.
         */

        private UnaryOperator<L> leftTransformer;

        /**
         * A function that transforms right-ish data and may consider the encapsulating event.
         */

        private UnaryOperator<Event<R>> rightTransformer;

        /**
         * A function that transforms baseline-ish data and may consider the encapsulating event.
         */

        private UnaryOperator<Event<R>> baselineTransformer;

        /**
         * The frequency at which pairs should be produced.
         */

        private Duration frequency;

        /**
         * An optional cross-pairer to ensure that the main pairs and baseline pairs are coincident in time.
         */

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
        Builder<L, R> setBaselineGenerator( UnaryOperator<TimeSeries<R>> baselineGenerator )
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
         * @param inputs the inputs declaration to set
         * @return the builder
         */
        Builder<L, R> setInputsDeclaration( Inputs inputs )
        {
            this.inputs = inputs;

            return this;
        }

        /**
         * @param leftTransformer the transformer for left-style data
         * @return the builder
         */
        Builder<L, R> setLeftTransformer( UnaryOperator<L> leftTransformer )
        {
            this.leftTransformer = leftTransformer;

            return this;
        }

        /**
         * @param rightTransformer the transformer for right-style data
         * @return the builder
         */
        Builder<L, R> setRightTransformer( UnaryOperator<Event<R>> rightTransformer )
        {
            this.rightTransformer = rightTransformer;

            return this;
        }

        /**
         * @param baselineTransformer the transformer for baseline-style data
         * @return the builder
         */
        Builder<L, R> setBaselineTransformer( UnaryOperator<Event<R>> baselineTransformer )
        {
            this.baselineTransformer = baselineTransformer;

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
     * Creates a paired dataset from the input, rescaling the left/right data as needed.
     * 
     * @param left the left data
     * @param rightOrBaseline the right or baseline data
     * @param desiredTimeScale the desired time scale
     * @param frequency the frequency with which to create pairs at the desired time scale
     * @param orientation the orientation of the non-left data, one of {@link LeftOrRightOrBaseline#RIGHT} or 
     *            {@link LeftOrRightOrBaseline#BASELINE}
     * @param timeWindow the time window to snip the pairs
     * @return the pairs
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     * @throws NullPointerException if the left, rightOrBaseline or timeWindow is null
     */

    private List<TimeSeries<Pair<L, R>>> createPairs( List<TimeSeries<L>> left,
                                                      List<TimeSeries<R>> rightOrBaseline,
                                                      UnaryOperator<Event<R>> rightOrBaselineTransformer,
                                                      TimeScaleOuter desiredTimeScale,
                                                      Duration frequency,
                                                      LeftOrRightOrBaseline orientation,
                                                      TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( left );

        Objects.requireNonNull( rightOrBaseline );

        List<TimeSeries<Pair<L, R>>> returnMe = new ArrayList<>();

        // Iterate through each combination of left/right series 
        for ( TimeSeries<L> nextLeft : left )
        {
            for ( TimeSeries<R> nextRight : rightOrBaseline )
            {
                TimeSeries<Pair<L, R>> pairs = this.createSeriesPairs( nextLeft,
                                                                       nextRight,
                                                                       rightOrBaselineTransformer,
                                                                       desiredTimeScale,
                                                                       frequency,
                                                                       timeWindow,
                                                                       orientation );

                if ( !pairs.getEvents().isEmpty() )
                {
                    returnMe.add( pairs );
                }
                else if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Found zero pairs while intersecting time-series {} with time-series {}.",
                                  left.hashCode(),
                                  nextRight.hashCode() );
                }
            }
        }

        // Log the number of time-series available for pairing and the number of paried time-series created
        if ( LOGGER.isDebugEnabled() )
        {
            PoolMetadata metaToReport = this.metadata;

            if ( orientation == LeftOrRightOrBaseline.BASELINE )
            {
                metaToReport = this.baselineMetadata;
            }

            LOGGER.debug( "While creating pool {}, discovered {} {} time-series and {} {} time-series from "
                          + "which to create pairs. Created {} paired time-series from these inputs.",
                          metaToReport,
                          left.size(),
                          LeftOrRightOrBaseline.LEFT,
                          rightOrBaseline.size(),
                          orientation,
                          returnMe.size() );
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
     * @return a paired time-series
     * @throws NullPointerException if the left, rightOrBaseline or timeWindow is null
     */

    private TimeSeries<Pair<L, R>> createSeriesPairs( TimeSeries<L> left,
                                                      TimeSeries<R> rightOrBaseline,
                                                      UnaryOperator<Event<R>> rightOrBaselineTransformer,
                                                      TimeScaleOuter desiredTimeScale,
                                                      Duration frequency,
                                                      TimeWindowOuter timeWindow,
                                                      LeftOrRightOrBaseline orientation )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( rightOrBaseline );
        Objects.requireNonNull( timeWindow );

        // Snip the left data to the right with a buffer on the lower bound, if required. This greatly improves
        // performance for a very long left series, which is quite typical
        Duration period = this.getPeriodFromTimeScale( desiredTimeScale );
        TimeSeries<L> scaledLeft = TimeSeriesSlicer.snip( left, rightOrBaseline, period, Duration.ZERO );
        TimeSeries<R> scaledRight = rightOrBaseline;
        boolean upscaleLeft = Objects.nonNull( desiredTimeScale ) && !desiredTimeScale.equals( left.getTimeScale() );
        boolean upscaleRight = Objects.nonNull( desiredTimeScale )
                               && !desiredTimeScale.equals( rightOrBaseline.getTimeScale() );

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

            // Acquire the times from the right series at which left upscaled values should end
            SortedSet<Instant> endsAt = this.getEventValidTimes( rightOrBaseline );

            TimeSeriesUpscaler<L> leftUp = this.getLeftUpscaler();
            // #92892
            RescaledTimeSeriesPlusValidation<L> upscaledLeft = leftUp.upscale( scaledLeft,
                                                                               desiredTimeScale,
                                                                               endsAt );

            scaledLeft = upscaledLeft.getTimeSeries();

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

            // Acquire the times from the left series at which right upscaled values should end
            SortedSet<Instant> endsAt = this.getEventValidTimes( scaledLeft );

            TimeSeriesUpscaler<R> rightUp = this.getRightUpscaler();
            RescaledTimeSeriesPlusValidation<R> upscaledRight = rightUp.upscale( rightOrBaseline,
                                                                                 desiredTimeScale,
                                                                                 endsAt );

            scaledRight = upscaledRight.getTimeSeries();

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

        // Transform the rescaled values, if required
        TimeSeries<L> scaledAndTransformedLeft = this.transform( scaledLeft, this.getLeftTransformer() );
        TimeSeries<R> scaledAndTransformedRight = this.transformByEvent( scaledRight, rightOrBaselineTransformer );

        // Create the pairs, if any
        TimeSeries<Pair<L, R>> pairs = this.getPairer()
                                           .pair( scaledAndTransformedLeft, scaledAndTransformedRight );

        // Snip the pairs to the pool boundary
        TimeSeries<Pair<L, R>> snippedPairs = this.snip( pairs, timeWindow );

        // When upscaling both sides, the superset of pairs may contain "unwanted" pairs.
        // Unwanted pairs are pairs that occur more frequently than the desired frequency.
        // By default, the desired frequency is the period associated with the desired time scale
        // aka, "back-to-back", but an explicit frequency otherwise
        // Filter any unwanted pairs from the superset
        if ( !snippedPairs.getEvents().isEmpty() && PoolSupplier.REGULAR_PAIRS && upscaleLeft && upscaleRight )
        {
            Instant start = this.getFirstReferenceTimeOrFirstValidTimeAdjustedForTimeScale( snippedPairs );
            snippedPairs = this.filterUpscaledPairsByFrequency( snippedPairs,
                                                                start,
                                                                desiredTimeScale.getPeriod(),
                                                                frequency );
        }

        // Log the pairing 
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While pairing {} time-series {}, "
                          + "which contained {} values, "
                          + "with {} time-series {},"
                          + " which contained {} values: "
                          + "created {} pairs at the desired time scale of {}.",
                          LeftOrRightOrBaseline.LEFT,
                          orientation,
                          scaledAndTransformedLeft.hashCode(),
                          scaledAndTransformedLeft.getEvents().size(),
                          scaledAndTransformedRight.hashCode(),
                          scaledAndTransformedRight.getEvents().size(),
                          snippedPairs.getEvents().size(),
                          desiredTimeScale );
        }

        return snippedPairs;
    }

    /**
     * Creates the climatological data as needed.
     * 
     * @return the climatological data or null if no climatology is defined
     */

    private VectorOfDoubles getClimatology()
    {
        if ( Objects.isNull( this.climatology ) )
        {
            return null;
        }

        List<TimeSeries<L>> climData = this.climatology.get()
                                                       .collect( Collectors.toList() );

        DoubleStream climatologyStream = DoubleStream.of();

        LOGGER.debug( "Adding climatolology to pool {}.", this.metadata );

        for ( TimeSeries<L> next : climData )
        {
            TimeSeries<L> nextSeries = next;

            TimeSeries<Double> transformed = TimeSeriesSlicer.transform( nextSeries,
                                                                         this.climatologyMapper::applyAsDouble );

            // Extract the doubles
            DoubleStream seriesDoubles = transformed.getEvents()
                                                    .stream()
                                                    .mapToDouble( Event::getValue );

            climatologyStream = DoubleStream.concat( climatologyStream, seriesDoubles );
        }

        return VectorOfDoubles.of( climatologyStream.toArray() );
    }

    /**
     * Creates a baseline dataset from a generator function using the right side of the input pairs as the source to
     * mimic.
     * 
     * @param generator the baseline generator function
     * @param pairs the pairs whose right side will be used to create the baseline
     * @return a list of generated baseline time-series
     */

    private List<TimeSeries<R>> createBaseline( UnaryOperator<TimeSeries<R>> generator,
                                                List<TimeSeries<Pair<L, R>>> pairs )
    {
        LOGGER.debug( "Creating baseline time-series with a generator function." );

        List<TimeSeries<R>> returnMe = new ArrayList<>();

        for ( TimeSeries<Pair<L, R>> next : pairs )
        {
            TimeSeries<R> rhs = TimeSeriesSlicer.transform( next, Pair::getRight );
            returnMe.add( generator.apply( rhs ) );
        }

        LOGGER.debug( "Finished creating baseline time-series with a generator function." );

        return Collections.unmodifiableList( returnMe );
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
     * Returns the desired time scale. In order of availability, this is:
     * 
     * <ol>
     * <li>The desired time scale provided on construction;</li>
     * <li>The Least Common Scale (LCS) computed from the input data; or</li>
     * <li>The LCS computed from the <code>existingTimeScale</code> provided in the input declaration.</li>
     * </ol>
     * 
     * The LCS is the smallest common multiple of the time scales associated with every ingested dataset for a given 
     * project, variable and feature. The LCS is computed from all sides of a pairing (left, right and baseline) 
     * collectively. 
     * 
     * @param leftData the left data
     * @param rightData the right data
     * @param baselineData the baseline data
     * @param inputDeclaration the input declaration
     * @return the desired time scale.
     */

    private TimeScaleOuter getDesiredTimeScale( List<TimeSeries<L>> leftData,
                                                List<TimeSeries<R>> rightData,
                                                List<TimeSeries<R>> baselineData,
                                                Inputs inputDeclaration )
    {
        if ( Objects.nonNull( this.desiredTimeScale ) )
        {
            LOGGER.trace( "While retrieving data for pool {}, discovered that the desired time scale of {} was "
                          + "supplied on construction of the pool.",
                          this.metadata,
                          this.desiredTimeScale );

            return this.desiredTimeScale;
        }

        // Find the Least Common Scale
        TimeScaleOuter leastCommonScale = null;
        Set<TimeScaleOuter> existingTimeScales = new HashSet<>();
        leftData.forEach( next -> existingTimeScales.add( next.getTimeScale() ) );
        rightData.forEach( next -> existingTimeScales.add( next.getTimeScale() ) );
        if ( Objects.nonNull( baselineData ) )
        {
            baselineData.forEach( next -> existingTimeScales.add( next.getTimeScale() ) );
        }

        // Remove any null element from the existing scales
        existingTimeScales.remove( null );

        // Look for the LCS among the ingested sources
        if ( !existingTimeScales.isEmpty() )
        {
            leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( existingTimeScales );

            LOGGER.debug( "While retrieving data for pool {}, discovered that the desired time scale was not supplied "
                          + "on construction of the pool. Instead, determined the desired time scale from the Least "
                          + "Common Scale of the ingested inputs, which was {}.",
                          this.metadata,
                          leastCommonScale );

            return leastCommonScale;
        }

        // Look for the LCS among the declared inputs
        if ( Objects.nonNull( this.inputs ) )
        {
            Set<TimeScaleOuter> declaredExistingTimeScales = new HashSet<>();
            TimeScaleConfig leftScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();
            TimeScaleConfig rightScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();

            if ( Objects.nonNull( leftScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( leftScaleConfig ) );
            }
            if ( Objects.nonNull( rightScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( rightScaleConfig ) );
            }
            if ( Objects.nonNull( inputDeclaration.getBaseline() )
                 && Objects.nonNull( inputDeclaration.getBaseline().getExistingTimeScale() ) )
            {
                declaredExistingTimeScales.add( TimeScaleOuter.of( inputDeclaration.getBaseline()
                                                                                   .getExistingTimeScale() ) );
            }

            if ( !declaredExistingTimeScales.isEmpty() )
            {
                leastCommonScale = TimeScaleOuter.getLeastCommonTimeScale( declaredExistingTimeScales );

                LOGGER.debug( "While retrieving data for pool {}, discovered that the desired time scale was not supplied "
                              + "on construction of the pool. Instead, determined the desired time scale from the Least "
                              + "Common Scale of the declared inputs, which  was {}.",
                              this.metadata,
                              leastCommonScale );
            }
        }

        return leastCommonScale;
    }

    /**
     * Applies a transformation to the input series.
     * 
     * @param <T> the event value type
     * @param toTransform the time-series to transform
     * @param transformer the transformer
     * @return the transformed series
     */

    private <T> TimeSeries<T> transform( TimeSeries<T> toTransform, UnaryOperator<T> transformer )
    {
        // No transformations?
        if ( Objects.isNull( transformer ) )
        {
            return toTransform;
        }

        TimeSeries<T> transformed = TimeSeriesSlicer.transform( toTransform, transformer );

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Tranformed the values associated with time-series {}, producing a new time-series {}.",
                          toTransform.hashCode(),
                          transformed.hashCode() );
        }

        return transformed;
    }

    /**
     * Applies a transformation to the input series that considers the encapsulating event.
     * 
     * @param <T> the event value type
     * @param toTransform the time-series to transform
     * @param transformer the transformer
     * @return the transformed series
     */

    private <T> TimeSeries<T> transformByEvent( TimeSeries<T> toTransform, UnaryOperator<Event<T>> transformer )
    {
        // No transformations?
        if ( Objects.isNull( transformer ) )
        {
            return toTransform;
        }

        TimeSeries<T> transformed = TimeSeriesSlicer.transformByEvent( toTransform, transformer );

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Tranformed the values associated with time-series {}, producing a new time-series {}.",
                          toTransform.hashCode(),
                          transformed.hashCode() );
        }

        return transformed;
    }

    /**
     * Returns the upscaler for left values, if any. Throws an exception if not available, because this is an internal
     * call and is only requested when necessary.
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
     * Returns the upscaler for right values, if any. Throws an exception if not available, because this is an internal
     * call and is only requested when necessary.
     * 
     * @return the upscaler for right values
     * @throws NullPointerException if the upscaler is not available
     */

    private TimeSeriesUpscaler<R> getRightUpscaler()
    {
        Objects.requireNonNull( this.rightUpscaler );

        return this.rightUpscaler;
    }

    /**
     * @return the transformer for left-ish data
     */

    private UnaryOperator<L> getLeftTransformer()
    {
        return this.leftTransformer;
    }

    /**
     * @return the transformer for right-ish data
     */

    private UnaryOperator<Event<R>> getRightTransformer()
    {
        return this.rightTransformer;
    }

    /**
     * @return the transformer for baseline-ish data
     */

    private UnaryOperator<Event<R>> getBaselineTransformer()
    {
        return this.baselineTransformer;
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
     * Helper that returns the times associated with values in the input series.
     * 
     * @param <T> the time-series event value type
     * @param timeSeries the time-series to use in determining when values should end
     * @return the times at which upscaled values should end
     */

    private <T> SortedSet<Instant> getEventValidTimes( TimeSeries<T> timeSeries )
    {
        // All possible times
        SortedSet<Instant> endsAt =
                timeSeries.getEvents()
                          .stream()
                          .map( Event::getTime )
                          .collect( Collectors.toCollection( TreeSet::new ) );

        return Collections.unmodifiableSortedSet( endsAt );
    }

    /**
     * Returns the frequency at which to create pairs. By default this is equal to the {@link TimeScaleOuter#getPeriod()} 
     * associated with the {@link #desiredTimeScale}, where defined, otherwise the value supplied on construction, 
     * which normally corresponds to the <code>frequency</code> associated with the {@link DesiredTimeScaleConfig}.
     * Otherwise <code>null</code>.
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
        else if ( Objects.nonNull( this.desiredTimeScale ) )
        {
            returnMe = this.desiredTimeScale.getPeriod();
        }

        return returnMe;
    }

    /**
     * Helper that returns the first available reference time from a time-series, otherwise the first valid time,
     * adjusted for the <code>period</code> associated with the time scale, otherwise <code>null</code>.
     * 
     * @return the first available reference time
     */

    private Instant getFirstReferenceTimeOrFirstValidTimeAdjustedForTimeScale( TimeSeries<?> timeSeries )
    {
        Collection<Instant> times = timeSeries.getReferenceTimes().values();

        Instant returnMe = null;

        if ( !times.isEmpty() )
        {
            returnMe = times.iterator().next();
        }

        // Valid time instead?
        if ( Objects.isNull( returnMe ) && !timeSeries.getEvents().isEmpty() )
        {
            returnMe = timeSeries.getEvents().first().getTime();

            if ( Objects.nonNull( timeSeries.getTimeScale() ) )
            {
                returnMe = returnMe.minus( timeSeries.getTimeScale().getPeriod() );
            }
        }

        return returnMe;
    }

    /**
     * <p>When conducting upscaling, all possible pairs are produced; that is, every pair whose left and right values
     * end at the same time and have the desired time scale. This may include pairs that are more frequent than the
     * desired frequency. The desired frequency is the <code>period</code> associated with the desired time scale, 
     * by default (aka "back-to-back" pairs), otherwise an explicit frequency.
     * 
     * <p>This method makes a best attempt to retain pairs from the input superset of pairs whose valid times follow a 
     * prescribed frequency. Consequently, this method effectively "thins out" the superset of all possible pairs in 
     * order to provide a subset of regularly spaced pairs. In general, there is no unique subset of pairs that follows 
     * a prescribed frequency unless a starting position is defined. Here, counting occurs with respect to a reference 
     * time (i.e., the reference time helps to select a subset). 
     * 
     * <p>See #47158-24.
     * 
     * <p>This is to re-assert my opinion, stated in #47158, that we should not attempt to find such a subset, but 
     * instead compute all pairs. For the same reason, the <code>frequency</code> associated with the 
     * <code>desiredTimeScale</code> should be eliminated. This method is an inevitable source of brittleness.
     * 
     * <p>TODO: this is an implementation detail in the present context; instead, consider relocating this to the 
     * {@link TimeSeriesSlicer}, increasing visibility to public and adding some unit tests.
     * 
     * @param toFilter the time-series to inspect
     * @param referenceTime the reference time from which to count
     * @param period the period associated with the desired time scale
     * @param frequency the regular frequency with which to count periods, defaults to the period
     * @return the filtered pairs
     * @throws NullPointerException if the toFilter, referenceTime or period is null
     */

    private TimeSeries<Pair<L, R>> filterUpscaledPairsByFrequency( TimeSeries<Pair<L, R>> toFilter,
                                                                   Instant referenceTime,
                                                                   Duration period,
                                                                   Duration frequency )
    {
        Objects.requireNonNull( toFilter );

        Objects.requireNonNull( referenceTime );

        Objects.requireNonNull( period );

        // More than one pair?
        TimeSeries<Pair<L, R>> filtered = toFilter;
        if ( toFilter.getEvents().size() > 1 )
        {
            // Duration by which to jump between periods
            // Default to the period, aka "back-to-back"
            Duration jump = frequency;
            if ( Objects.isNull( frequency ) )
            {
                jump = period;
            }

            TimeSeries.Builder<Pair<L, R>> filteredSeries = new TimeSeries.Builder<>();
            TimeSeriesMetadata localMetadata = toFilter.getMetadata();
            filteredSeries.setMetadata( localMetadata );

            List<Event<Pair<L, R>>> events = new ArrayList<>( toFilter.getEvents() );

            // Get the start time for the regular sequence
            Instant nextTime = this.getStartTimeForRegularSequenceOfPairs( referenceTime, period, jump, events );

            int totalEvents = events.size();
            Instant lastTime = events.get( totalEvents - 1 ).getTime();

            // Increment the sequence from the start time
            int start = 0;
            while ( nextTime.compareTo( lastTime ) <= 0 )
            {
                // Does a pair exist at the next regular time?
                for ( int i = start; i < totalEvents; i++ )
                {
                    Event<Pair<L, R>> nextEvent = events.get( i );
                    Instant nextEventTime = nextEvent.getTime();

                    // Yes it does. Add it.
                    if ( nextEventTime.equals( nextTime ) )
                    {
                        filteredSeries.addEvent( nextEvent );
                        start = i + 1;
                        break;
                    }
                }

                nextTime = nextTime.plus( jump );
            }

            filtered = filteredSeries.build();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Inspected {} pairs and eliminated {} pairs to be consistent with the production of "
                              + "regular pairs that have a period of {} and repeat every {}.",
                              toFilter.getEvents().size(),
                              filtered.getEvents().size() - toFilter.getEvents().size(),
                              period,
                              frequency );
            }
        }

        return filtered;
    }

    /**
     * Steps away from the reference time by the period, initially, then the frequency, until an event is discovered in
     * the list with the same valid time. If no event is discovered, returns the valid time of the first event in the 
     * list.
     * 
     * @param referenceTime the reference time
     * @param period the period
     * @param frequency the frequency
     * @param timeOrderedListOfEvents the events in order of valid time
     * @return the start time
     */

    private Instant getStartTimeForRegularSequenceOfPairs( Instant referenceTime,
                                                           Duration period,
                                                           Duration frequency,
                                                           List<Event<Pair<L, R>>> timeOrderedListOfEvents )
    {
        Instant firstTime = timeOrderedListOfEvents.get( 0 ).getTime();

        // First valid time is before the reference time: use the first valid time. The alternative would be to search
        // a regular sequence moving back in time and choosing the earliest instance on that sequence, similar to the 
        // forward search below. This would guarantee consistent behavior for time-series that span the reference time, 
        // ensuring the same choice of pairs, regardless of the earliest lead duration considered, but it would also add 
        // complexity and negative lead durations are an edge case.
        if ( firstTime.compareTo( referenceTime ) < 0 )
        {
            return firstTime;
        }

        // First valid time is after the reference time so step forwards
        return this.getStartTimeForRegularSequenceOfPairsSearchingForwards( referenceTime,
                                                                            period,
                                                                            frequency,
                                                                            timeOrderedListOfEvents );
    }

    /**
     * Steps forwards from the reference time by the period, initially, then the frequency, until an event is discovered 
     * in the list with the same valid time. If no event is discovered, returns the valid time of the first event in the 
     * list.
     * 
     * @param referenceTime the reference time
     * @param period the period
     * @param frequency the frequency
     * @param timeOrderedListOfEvents the events in order of valid time
     * @return the start time
     */

    private Instant getStartTimeForRegularSequenceOfPairsSearchingForwards( Instant referenceTime,
                                                                            Duration period,
                                                                            Duration frequency,
                                                                            List<Event<Pair<L, R>>> timeOrderedListOfEvents )
    {
        int totalEvents = timeOrderedListOfEvents.size();
        Instant firstTime = timeOrderedListOfEvents.get( 0 ).getTime();
        Instant lastTime = timeOrderedListOfEvents.get( totalEvents - 1 ).getTime();

        // Increment the sequence of valid times until the last time
        // It is debatable whether the first step away from the reference time should use the period or the frequency.
        // The period is chosen here.
        Instant nextSequenceTime = referenceTime.plus( period );

        // Iterate the regular sequence until the end
        while ( nextSequenceTime.compareTo( lastTime ) <= 0 )
        {
            // Does a pair exist in the list of pairs whose valid time matches the next time in the sequence?
            for ( int i = 0; i < totalEvents; i++ )
            {
                Event<Pair<L, R>> nextEvent = timeOrderedListOfEvents.get( i );
                Instant nextEventTime = nextEvent.getTime();

                int compare = nextEventTime.compareTo( nextSequenceTime );

                // Yes it does. Return it.
                if ( compare == 0 )
                {
                    return nextEventTime;
                }
                // Next event time is after the sequence time and the times are ordered, so break
                else if ( compare > 0 )
                {
                    break;
                }
            }

            nextSequenceTime = nextSequenceTime.plus( frequency );
        }

        // Resort to the first valid time in the list
        return firstTime;
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
                                                    List<ScaleValidationEvent> scaleValidationEvents )
    {
        Objects.requireNonNull( scaleValidationEvents );

        // Any warnings? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isWarnEnabled() )
        {
            Set<ScaleValidationEvent> warnEvents = scaleValidationEvents.stream()
                                                                        .filter( a -> a.getEventType() == EventType.WARN )
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
            Set<ScaleValidationEvent> debugWarnEvents = scaleValidationEvents.stream()
                                                                             .filter( a -> a.getEventType() == EventType.DEBUG )
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
     * Snips the first list of time-series to the bounds of the second list. This is an optimization that works when 
     * the time-series in the first list extend far beyond the bounds of the union of times in the second list. For 
     * example, the first list may contains observations that are also used to form a climatological dataset, which is 
     * shared across pools. This method does not affect accuracy.
     * 
     * <S> the type of time-series values to snip
     * <T> the type of time-series values to use when snipping
     * @param toSnip the list of time-series to snip
     * @param snipTo the list of time-series whose bounds will be used for snipping
     * @return the snipped and possibly consolidated data
     */

    private <S, T> List<TimeSeries<S>> snip( List<TimeSeries<S>> toSnip,
                                             List<TimeSeries<T>> snipTo )
    {
        TimeWindowOuter timeWindow = this.getTimeWindowFromSeries( snipTo );

        List<TimeSeries<S>> returnMe = new ArrayList<>();

        for ( TimeSeries<S> next : toSnip )
        {
            TimeSeries<S> filtered = TimeSeriesSlicer.filter( next, timeWindow );

            // Any events left?
            if ( !filtered.getEvents().isEmpty() )
            {
                returnMe.add( filtered );
            }
        }

        return Collections.unmodifiableList( returnMe );
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
            TimeWindowOuter partialSnip = TimeWindowOuter.of( snipTo.getEarliestReferenceTime(),
                                                              snipTo.getLatestReferenceTime(),
                                                              snipTo.getEarliestValidTime(),
                                                              snipTo.getLatestValidTime() );

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
     * Looks for time-series without reference times and consolidates them. Values within observation-like time-series
     * may be combined when rescaling. Thus, it is convenient to place them into one time-series. Values within 
     * forecast-like time-series cannot be combined across time-series.
     * 
     * @param <T> the type of event values
     * @param timeSeries the time-series to consolidate, if possible
     * @return any time-series that were consolidated plus any time-series that were not consolidated
     */

    private <T> List<TimeSeries<T>> consolidateTimeSeriesWithZeroReferenceTimes( List<TimeSeries<T>> timeSeries )
    {
        List<TimeSeries<T>> returnMe = new ArrayList<>();

        // Tolerate null input (e.g., for a baseline)
        if ( Objects.nonNull( timeSeries ) )
        {
            // Separate into time-series that have reference times and those that do not. Those with reference times
            // are not consolidated
            List<TimeSeries<T>> withoutReferenceTimes = new ArrayList<>();
            for ( TimeSeries<T> next : timeSeries )
            {
                Set<ReferenceTimeType> referenceTimes = next.getReferenceTimes()
                                                            .keySet();

                // One or more reference times that are not observation-like
                if ( !referenceTimes.isEmpty()
                     && !referenceTimes.equals( PoolSupplier.OBSERVATION_REFERENCE_TIME_TYPE ) )
                {
                    returnMe.add( next );
                }
                else
                {
                    // Remove any observation-like reference times
                    TimeSeriesMetadata meta =
                            new TimeSeriesMetadata.Builder( next.getMetadata() ).setReferenceTimes( Map.of() )
                                                                                .build();

                    TimeSeries<T> series = TimeSeries.of( meta, next.getEvents() );

                    withoutReferenceTimes.add( series );
                }
            }

            // Consolidate the time-series without reference times
            List<TimeSeries<T>> withoutReferenceTimesUnmodifiable =
                    Collections.unmodifiableList( withoutReferenceTimes );

            Collection<TimeSeries<T>> consolidated =
                    TimeSeriesSlicer.consolidateTimeSeriesWithZeroReferenceTimes( withoutReferenceTimesUnmodifiable );

            returnMe.addAll( consolidated );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns a time-window whose valid times span the input series. The lower bound is adjusted for the time-scale of
     * the input series and one instant/nanosecond is added to the lower bound to make it inclusive.
     * 
     * @param <T> the time-series event type
     * @param bounds the time-series whose bounds must be determined
     * @return the time window
     */

    private <T> TimeWindowOuter getTimeWindowFromSeries( List<TimeSeries<T>> bounds )
    {
        SortedSet<Instant> validTimes = new TreeSet<>();

        TimeScaleOuter timeScale = null;
        for ( TimeSeries<T> next : bounds )
        {
            // Add both reference times and valid times
            validTimes.addAll( next.getReferenceTimes().values() );

            // #73227-40
            if ( !next.getEvents().isEmpty() )
            {
                validTimes.add( next.getEvents().first().getTime() );
                validTimes.add( next.getEvents().last().getTime() );
            }
            timeScale = next.getTimeScale();
        }

        Instant lowerBound = Instant.MIN;
        Instant upperBound = Instant.MAX;

        if ( !validTimes.isEmpty() )
        {
            // Lower bound inclusive
            lowerBound = validTimes.first()
                                   .minus( Duration.ofNanos( 1 ) );
            upperBound = validTimes.last();
        }

        if ( Objects.nonNull( timeScale ) && !timeScale.isInstantaneous() )
        {
            lowerBound = lowerBound.minus( timeScale.getPeriod() );
        }

        return TimeWindowOuter.of( lowerBound, upperBound );
    }

    /**
     * Adds a prescribed offset to the valid time of each time-series in the list.
     * 
     * @param <T> the time-series event value type
     * @param toTransform the list of time-series to transform
     * @param offset the offset to add
     * @param lrb the side of data to help with logging
     * @return the adjusted time-series
     */

    private <T> List<TimeSeries<T>> applyValidTimeOffset( List<TimeSeries<T>> toTransform,
                                                          Duration offset,
                                                          LeftOrRightOrBaseline lrb )
    {

        List<TimeSeries<T>> transformed = toTransform;

        // Transform valid times?
        if ( Objects.nonNull( toTransform )
             && Objects.nonNull( offset )
             && !Duration.ZERO.equals( offset ) )
        {
            // Log the time shift
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Applying a valid time offset of {} to {} time-series.", offset, lrb );
            }

            transformed = new ArrayList<>();

            for ( TimeSeries<T> nextSeries : toTransform )
            {
                TimeSeries<T> nextTransformed = TimeSeriesSlicer.applyOffsetToValidTimes( nextSeries, offset );
                transformed.add( nextTransformed );
            }

            // Render immutable
            transformed = Collections.unmodifiableList( transformed );
        }

        return transformed;
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
            period = timeScale.getPeriod();
        }

        return period;
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
        this.desiredTimeScale = builder.desiredTimeScale;
        this.metadata = builder.metadata;
        this.baselineMetadata = builder.baselineMetadata;
        this.climatologyMapper = builder.climatologyMapper;
        this.baselineGenerator = builder.baselineGenerator;
        this.inputs = builder.inputs;
        this.leftTransformer = builder.leftTransformer;
        this.rightTransformer = builder.rightTransformer;
        this.baselineTransformer = builder.baselineTransformer;
        this.frequency = builder.frequency;
        this.crossPairer = builder.crossPairer;

        // Set any time offsets required
        Map<LeftOrRightOrBaseline, Duration> offsets = this.getValidTimeOffsets( this.inputs );
        this.leftOffset = offsets.get( LeftOrRightOrBaseline.LEFT );
        this.rightOffset = offsets.get( LeftOrRightOrBaseline.RIGHT );
        this.baselineOffset = offsets.get( LeftOrRightOrBaseline.BASELINE );

        if ( !offsets.isEmpty() )
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

        if ( Objects.isNull( this.desiredTimeScale ) )
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

        if ( Objects.isNull( this.inputs ) )
        {
            LOGGER.debug( WHILE_CONSTRUCTING_A_POOL_SUPPLIER_FOR
                          + "discovered that the inputs declaration was undefined.",
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
        if ( !returnMe.containsKey( LeftOrRightOrBaseline.LEFT ) )
        {
            returnMe.put( LeftOrRightOrBaseline.LEFT, Duration.ZERO );
        }

        if ( !returnMe.containsKey( LeftOrRightOrBaseline.RIGHT ) )
        {
            returnMe.put( LeftOrRightOrBaseline.RIGHT, Duration.ZERO );
        }

        if ( Objects.nonNull( inputsConfig )
             && Objects.nonNull( inputsConfig.getBaseline() )
             && !returnMe.containsKey( LeftOrRightOrBaseline.BASELINE ) )
        {
            returnMe.put( LeftOrRightOrBaseline.BASELINE, Duration.ZERO );
        }

        return returnMe;
    }


}
