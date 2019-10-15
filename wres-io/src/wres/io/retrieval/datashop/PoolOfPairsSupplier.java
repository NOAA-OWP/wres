package wres.io.retrieval.datashop;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.TimeScaleConfig;
import wres.config.generated.ProjectConfig.Inputs;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PairingException;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.RescalingException;
import wres.datamodel.scale.ScaleValidationEvent;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.ScaleValidationEvent.EventType;
import wres.datamodel.time.Event;
import wres.datamodel.time.RescaledTimeSeriesPlusValidation;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindow;

/**
 * <p>Supplies a {@link PoolOfPairs}, which is used to compute one or more verification statistics. The overall 
 * responsibility of the {@link PoolOfPairsSupplier} is to supply a {@link PoolOfPairs} on request. This is fulfilled 
 * by completing several smaller activities in sequence, namely:</p> 
 * 
 * <ol>
 * <li>Consuming the (possibly pool-shaped) left/right/baseline data for pairing, which is supplied by retrievers;</li>
 * <li>Rescaling the data, where needed, so that pairs can be formed at the desired time scale;</li>
 * <li>Creating pairs;</li>
 * <li>Trimming pairs to the pool boundaries; and</li>
 * <li>Supplying the pool-shaped pairs.</li>
 * </ol>
 * 
 * <p>Once retrieved, the {@link PoolOfPairs} is cached and supplied on demand. 
 * 
 * <p><b>Implementation notes:</b></p>
 * 
 * <p>This class is thread safe.
 * 
 * @author james.brown@hydrosolved.com
 * @param <L> the type of left value in each pair
 * @param <R> the type of right value in each pair and, where applicable, the type of baseline value
 */

public class PoolOfPairsSupplier<L, R> implements Supplier<PoolOfPairs<L, R>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( PoolOfPairsSupplier.class );

    /**
     * Climatological data source.
     */

    private final Supplier<Stream<TimeSeries<L>>> climatology;

    /**
     * Mapper from the left-type of climatological data to a double-type.
     */

    private final ToDoubleFunction<L> climatologyMapper;

    /**
     * Left data source. Optional, unless the climatological source is undefined.
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

    private final TimeScale desiredTimeScale;

    /**
     * Metadata for the mains pairs.
     */

    private final SampleMetadata metadata;

    /**
     * Metadata for the baseline pairs.
     */

    private final SampleMetadata baselineMetadata;

    /**
     * The pool to return.
     */

    private PoolOfPairs<L, R> pool;

    /**
     * Pool creation lock. Only create a pool once.
     */

    private final Object creationLock = new Object();

    /**
     * The inputs declaration, which is used to help compute the desired time scale, if required.
     */

    private final Inputs inputs;

    /**
     * Returns a {@link PoolOfPairs} for metric calculation.
     * 
     * @return a pool of pairs
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     */

    @Override
    public PoolOfPairs<L, R> get()
    {
        // Create pool if needed
        synchronized ( this.creationLock )
        {
            if ( Objects.nonNull( this.pool ) )
            {
                return this.pool;
            }

            this.createPool();
        }

        LOGGER.debug( "Returning existing pool for {}.", this.metadata );

        return this.pool;
    }

    /**
     * Creates a {@link PoolOfPairs}.
     * 
     * @throws DataAccessException if the pool data could not be retrieved
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     */

    public void createPool()
    {
        LOGGER.debug( "Creating pool {}.", this.metadata );

        PoolOfPairsBuilder<L, R> builder = new PoolOfPairsBuilder<>();

        // Set the metadata
        builder.setMetadata( this.metadata );
        builder.setMetadataForBaseline( this.baselineMetadata );

        // Left data provided or is climatology the left data?
        Stream<TimeSeries<L>> cStream;
        if ( Objects.nonNull( this.left ) )
        {
            cStream = this.left.get();
        }
        else
        {
            cStream = this.climatology.get();
        }

        // Retrieve the data
        List<TimeSeries<L>> leftData = cStream.collect( Collectors.toList() );
        List<TimeSeries<R>> rightData = this.right.get().collect( Collectors.toList() );
        List<TimeSeries<R>> baselineData = null;

        // Baseline that contains data?
        if ( this.hasBaseline() && Objects.isNull( this.baselineGenerator ) )
        {
            baselineData = this.baseline.get().collect( Collectors.toList() );
        }

        // Obtained the desired time scale. If this is unavailable, use the Least Common Scale.
        TimeScale desiredTimeScaleToUse = this.getDesiredTimeScale( leftData, rightData, baselineData, this.inputs );

        // Consolidate and snip the left data to the right bounds
        // This is a performance optimization for when the left dataset is large (e.g., climatology)
        TimeSeries<L> snippedLeft = this.snipAndConsolidate( leftData, rightData );

        List<TimeSeries<Pair<L, R>>> mainPairs = this.createPairs( snippedLeft, rightData, desiredTimeScaleToUse );

        for ( TimeSeries<Pair<L, R>> pairs : mainPairs )
        {

            // Filter the pairs against the pool boundaries, if required
            if ( this.metadata.hasTimeWindow() )
            {
                pairs = TimeSeriesSlicer.filter( pairs, this.metadata.getTimeWindow() );
            }

            builder.addTimeSeries( pairs );
        }

        // Create the baseline pairs
        if ( this.hasBaseline() )
        {
            LOGGER.debug( "Adding pairs for baseline to pool {}, which have metadata {}.",
                          this.metadata,
                          this.baselineMetadata );

            // Generator?
            if ( Objects.nonNull( this.baselineGenerator ) )
            {
                baselineData = this.createBaseline( this.baselineGenerator, mainPairs );
            }

            List<TimeSeries<Pair<L, R>>> basePairs = this.createPairs( snippedLeft,
                                                                       baselineData,
                                                                       desiredTimeScaleToUse );

            for ( TimeSeries<Pair<L, R>> pairs : basePairs )
            {
                // Filter the pairs against the pool boundaries, if required
                if ( this.metadata.hasTimeWindow() )
                {
                    pairs = TimeSeriesSlicer.filter( pairs, this.metadata.getTimeWindow() );
                }

                builder.addTimeSeriesForBaseline( pairs );
            }
        }

        // Set the climatology
        VectorOfDoubles clim = this.createClimatology( desiredTimeScaleToUse );
        builder.setClimatology( clim );

        // Create the pairs
        PoolOfPairs<L, R> returnMe = builder.build();

        LOGGER.debug( "Finished creating pool {}, which contains {} pairs.",
                      this.metadata,
                      returnMe.getRawData().size() );

        this.pool = returnMe;
    }

    /**
     * Builder for a {@link PoolOfPairsSupplier}.
     * 
     * @author james.brown@hydrosolved.com
     * @param <L> the left type of paired value
     * @param <R> the right type of paired value and, where required, the baseline type
     */

    static class PoolOfPairsSupplierBuilder<L, R>
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
         * Left data source. Optional, unless the climatological source is undefined.
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

        private TimeScale desiredTimeScale;

        /**
         * Metadata for the mains pairs.
         */

        private SampleMetadata metadata;

        /**
         * Metadata for the baseline pairs.
         */

        private SampleMetadata baselineMetadata;

        /**
         * Inputs declaration for the pool.
         */

        private Inputs inputs;

        /**
         * @param climatology the climatology to set
         * @param climatologyMapper the mapper from the climatological type to a double type
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setClimatology( Supplier<Stream<TimeSeries<L>>> climatology,
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
        public PoolOfPairsSupplierBuilder<L, R> setLeft( Supplier<Stream<TimeSeries<L>>> left )
        {
            this.left = left;

            return this;
        }

        /**
         * @param right the right to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setRight( Supplier<Stream<TimeSeries<R>>> right )
        {
            this.right = right;

            return this;
        }

        /**
         * @param baseline the baseline to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setBaseline( Supplier<Stream<TimeSeries<R>>> baseline )
        {
            this.baseline = baseline;

            return this;
        }

        /**
         * @param baselineGenerator a baseline generator to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setBaselineGenerator( UnaryOperator<TimeSeries<R>> baselineGenerator )
        {
            this.baselineGenerator = baselineGenerator;

            return this;
        }

        /**
         * @param pairer the pairer to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setPairer( TimeSeriesPairer<L, R> pairer )
        {
            this.pairer = pairer;

            return this;
        }

        /**
         * @param leftUpscaler the leftUpscaler to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setLeftUpscaler( TimeSeriesUpscaler<L> leftUpscaler )
        {
            this.leftUpscaler = leftUpscaler;

            return this;
        }

        /**
         * @param rightUpscaler the rightUpscaler to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setRightUpscaler( TimeSeriesUpscaler<R> rightUpscaler )
        {
            this.rightUpscaler = rightUpscaler;

            return this;
        }

        /**
         * @param desiredTimeScale the desiredTimeScale to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setDesiredTimeScale( TimeScale desiredTimeScale )
        {
            this.desiredTimeScale = desiredTimeScale;

            return this;
        }

        /**
         * @param metadata the metadata to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setMetadata( SampleMetadata metadata )
        {
            this.metadata = metadata;

            return this;
        }

        /**
         * @param baselineMetadata the baselineMetadata to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setBaselineMetadata( SampleMetadata baselineMetadata )
        {
            this.baselineMetadata = baselineMetadata;

            return this;
        }

        /**
         * @param inputs the inputs declaration to set
         * @return the builder
         */
        public PoolOfPairsSupplierBuilder<L, R> setInputsDeclaration( Inputs inputs )
        {
            this.inputs = inputs;

            return this;
        }

        /**
         * Builds a {@link PoolOfPairsSupplier}.
         * 
         * @return a pool supplier
         */

        public PoolOfPairsSupplier<L, R> build()
        {
            return new PoolOfPairsSupplier<>( this );
        }
    }

    /**
     * Creates a paired dataset from the input, rescaling the left/right data as needed.
     * 
     * @param left the left data
     * @param right the right data
     * @param desiredTimeScale the desired time scale
     * @throws RescalingException if the pool data could not be rescaled
     * @throws PairingException if the pool data could not be paired
     * @throws NoSuchUnitConversionException if the data units could not be converted
     * @return the pairs
     */

    private List<TimeSeries<Pair<L, R>>> createPairs( TimeSeries<L> left,
                                                      List<TimeSeries<R>> right,
                                                      TimeScale desiredTimeScale )
    {
        Objects.requireNonNull( left );

        Objects.requireNonNull( right );

        List<TimeSeries<Pair<L, R>>> returnMe = new ArrayList<>();
        for ( TimeSeries<R> nextRight : right )
        {
            TimeSeries<Pair<L, R>> pairs = this.createSeriesPairs( left, nextRight, desiredTimeScale );

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

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Consolidates and snips the input series to the prescribed time window.
     * 
     * @param <P> the time-series event type for the data to be snipped
     * @param <Q> the time-series event type for the data to use when snipping
     * @param toSnip the time-series to consolidate and snip
     * @param bounds the data whose bounds will be used for snipping
     * @return the snipped data
     */

    private <P, Q> TimeSeries<P> snipAndConsolidate( List<TimeSeries<P>> toSnip, List<TimeSeries<Q>> bounds )
    {
        TimeSeriesBuilder<P> builder = new TimeSeriesBuilder<>();

        TimeWindow timeWindow = this.getTimeWindowFromSeries( bounds );

        for ( TimeSeries<P> next : toSnip )
        {
            builder.addEvents( TimeSeriesSlicer.filter( next, timeWindow ).getEvents() );
            builder.setTimeScale( next.getTimeScale() ).addReferenceTimes( next.getReferenceTimes() );
        }

        return builder.build();
    }

    /**
     * Returns a time-window whose valid times span the input series. The lower bound is adjusted for the time-scale of
     * the input series.
     * 
     * @param <T> the time-series event type
     * @param bounds the time-series whose bounds must be determined
     * @return the time window
     */

    private <T> TimeWindow getTimeWindowFromSeries( List<TimeSeries<T>> bounds )
    {
        SortedSet<Instant> validTimes = new TreeSet<>();

        TimeScale timeScale = null;
        for ( TimeSeries<T> next : bounds )
        {
            // Add both reference times and valid times
            validTimes.addAll( next.getReferenceTimes().values() );
            validTimes.add( next.getEvents().first().getTime() );
            validTimes.add( next.getEvents().last().getTime() );
            timeScale = next.getTimeScale();
        }

        Instant lowerBound = Instant.MIN;
        Instant upperBound = Instant.MAX;
        
        if( !validTimes.isEmpty() )
        {
            lowerBound = validTimes.first();
            upperBound = validTimes.last();
        }

        if ( Objects.nonNull( timeScale ) && !timeScale.isInstantaneous() )
        {
            lowerBound = lowerBound.minus( timeScale.getPeriod() );
        }

        return TimeWindow.of( lowerBound, upperBound );
    }

    /**
     * Returns a time-series of pairs from a left and right series, rescaling as needed.
     * 
     * @param left the left time-series
     * @param right the right time-series
     * @param desiredTimeScale the desired time scale
     * @return a paired time-series
     */

    private TimeSeries<Pair<L, R>> createSeriesPairs( TimeSeries<L> left,
                                                      TimeSeries<R> right,
                                                      TimeScale desiredTimeScale )
    {
        Objects.requireNonNull( left );

        Objects.requireNonNull( right );

        TimeSeries<L> scaledLeft = left;
        TimeSeries<R> scaledRight = right;

        if ( Objects.nonNull( desiredTimeScale ) && !desiredTimeScale.equals( left.getTimeScale() ) )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Upscaling left time-series {} from {} to {}.",
                              left.hashCode(),
                              left.getTimeScale(),
                              desiredTimeScale );
            }

            // Acquire the times from the right series at which left upscaled values should end
            SortedSet<Instant> endsAt =
                    right.getEvents()
                         .stream()
                         .map( Event::getTime )
                         .collect( Collectors.toCollection( TreeSet::new ) );

            RescaledTimeSeriesPlusValidation<L> upscaledLeft = this.getLeftUpscaler()
                                                                   .upscale( left,
                                                                             desiredTimeScale,
                                                                             Collections.unmodifiableSortedSet( endsAt ) );

            scaledLeft = upscaledLeft.getTimeSeries();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Finished upscaling left time-series {} from {} to {}, which produced a "
                              + "new time-series, {}.",
                              left.hashCode(),
                              left.getTimeScale(),
                              desiredTimeScale,
                              scaledLeft.hashCode() );
            }

            // Log any warnings
            PoolOfPairsSupplier.logScaleValidationWarnings( left, upscaledLeft.getValidationEvents() );
        }

        if ( Objects.nonNull( desiredTimeScale ) && !desiredTimeScale.equals( right.getTimeScale() ) )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Upscaling right time-series {} from {} to {}.",
                              right.hashCode(),
                              right.getTimeScale(),
                              desiredTimeScale );
            }

            // Acquire the times from the left series at which right upscaled values should end
            SortedSet<Instant> endsAt =
                    left.getEvents()
                        .stream()
                        .map( Event::getTime )
                        .collect( Collectors.toCollection( TreeSet::new ) );

            RescaledTimeSeriesPlusValidation<R> upscaledRight = this.getRightUpscaler()
                                                                    .upscale( right,
                                                                              desiredTimeScale,
                                                                              Collections.unmodifiableSortedSet( endsAt ) );

            scaledRight = upscaledRight.getTimeSeries();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Finished upscaling right time-series {} from {} to {}, which produced a "
                              + "new time-series, {}.",
                              right.hashCode(),
                              right.getTimeScale(),
                              desiredTimeScale,
                              scaledRight.hashCode() );
            }

            // Log any warnings
            PoolOfPairsSupplier.logScaleValidationWarnings( right, upscaledRight.getValidationEvents() );
        }

        // Create the pairs, if any
        TimeSeries<Pair<L, R>> pairs = this.getPairer().pair( scaledLeft, scaledRight );

        // Log the pairing 
        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While pairing left time-series {}, "
                          + "which contained {} values, "
                          + "with right time-series {},"
                          + " which contained {} values: "
                          + "created {} pairs at the desired time scale of {}.",
                          scaledLeft.hashCode(),
                          scaledLeft.getEvents().size(),
                          scaledRight.hashCode(),
                          scaledRight.getEvents().size(),
                          pairs.getEvents().size(),
                          desiredTimeScale );
        }

        return pairs;
    }

    /**
     * Creates the climatological data as needed.
     * 
     * @param desiredTimeScale the desired time scale
     * @return the climatological data or null if no climatology is defined
     */

    private VectorOfDoubles createClimatology( TimeScale desiredTimeScale )
    {
        VectorOfDoubles returnMe = null;

        List<Double> listOfDoubles = new ArrayList<>();

        if ( Objects.nonNull( this.climatology ) )
        {
            LOGGER.debug( "Creating climatolology for pool {}.", this.metadata );

            // Map from TimeSeries<L> to VectorOfDoubles
            List<TimeSeries<L>> climate = this.climatology.get()
                                                          .collect( Collectors.toList() );

            for ( TimeSeries<L> next : climate )
            {
                TimeSeries<L> nextSeries = next;

                // Upscale?
                if ( Objects.nonNull( desiredTimeScale )
                     && !desiredTimeScale.equals( nextSeries.getTimeScale() ) )
                {
                    nextSeries = this.getLeftUpscaler().upscale( nextSeries, desiredTimeScale ).getTimeSeries();
                    LOGGER.debug( "Upscaled the climatological time-series from {} to {}.",
                                  nextSeries.getTimeScale(),
                                  desiredTimeScale );

                }

                TimeSeries<Double> transformed =
                        TimeSeriesSlicer.transform( nextSeries, this.climatologyMapper::applyAsDouble );
                transformed.getEvents().forEach( event -> listOfDoubles.add( event.getValue() ) );
            }

            returnMe = VectorOfDoubles.of( listOfDoubles.stream()
                                                        .mapToDouble( Double::doubleValue )
                                                        .toArray() );

            LOGGER.debug( "Finished creating climatology for pool {}. Discovered {} climatological values.",
                          this.metadata,
                          listOfDoubles.size() );
        }

        return returnMe;
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
     * @param input the input declaration
     * @return the desired time scale.
     */

    private TimeScale getDesiredTimeScale( List<TimeSeries<L>> leftData,
                                           List<TimeSeries<R>> rightData,
                                           List<TimeSeries<R>> baselineData,
                                           Inputs inputDeclaration )
    {
        if ( Objects.nonNull( this.desiredTimeScale ) )
        {
            LOGGER.trace( "While retrieving data for pool {}, discovered that the desired time scale of {} was "
                          + "supplied on construction of the pool.",
                          this.metadata );

            return this.desiredTimeScale;
        }

        // Find the Least Common Scale
        TimeScale leastCommonScale = null;
        Set<TimeScale> existingTimeScales = new HashSet<>();
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
            leastCommonScale = TimeScale.getLeastCommonTimeScale( existingTimeScales );

            LOGGER.debug( "While retrieving data for pool {}, discovered that the desired time scale was not supplied "
                          + "on construction of the pool. Instead, determined the desired time scale from the Least "
                          + "Common Scale of the ingested inputs, which was {}.",
                          this.metadata );

            return leastCommonScale;
        }

        // Look for the LCS among the declared inputs
        if ( Objects.nonNull( inputs ) )
        {
            Set<TimeScale> declaredExistingTimeScales = new HashSet<>();
            TimeScaleConfig leftScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();
            TimeScaleConfig rightScaleConfig = inputDeclaration.getLeft().getExistingTimeScale();

            if ( Objects.nonNull( leftScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScale.of( leftScaleConfig ) );
            }
            if ( Objects.nonNull( rightScaleConfig ) )
            {
                declaredExistingTimeScales.add( TimeScale.of( rightScaleConfig ) );
            }
            if ( Objects.nonNull( inputDeclaration.getBaseline() )
                 && Objects.nonNull( inputDeclaration.getBaseline().getExistingTimeScale() ) )
            {
                declaredExistingTimeScales.add( TimeScale.of( inputDeclaration.getBaseline().getExistingTimeScale() ) );
            }

            if ( !declaredExistingTimeScales.isEmpty() )
            {
                leastCommonScale = TimeScale.getLeastCommonTimeScale( declaredExistingTimeScales );

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
     * Returns the upscaler for left values, if any. Throws an exception if not available, because this is an internal
     * call and is only requested when necessary.
     * 
     * @return the upscaler for left values
     * @throws NullPointerException if the upscaler is not available
     */

    private TimeSeriesUpscaler<L> getLeftUpscaler()
    {
        Objects.requireNonNull( this.leftUpscaler,
                                "Left upscaler was required, but not available, when creating pool "
                                                   + this.metadata
                                                   + "." );

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
        Objects.requireNonNull( this.rightUpscaler,
                                "Right upscaler was required, but not available, when creating pool "
                                                    + this.metadata
                                                    + "." );

        return this.rightUpscaler;
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
     * Logs the validation events of type {@link ScaleValidationEvent#WARN} associated with rescaling.
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

        // Any warnings? Log those individually.              
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

                LOGGER.warn( "While rescaling a time-series with reference time {}, encountered {} validation "
                             + "warnings, as follows: {}{}",
                             context.getReferenceTimes().values().iterator().next(),
                             warnEvents.size(),
                             System.lineSeparator(),
                             message );
            }
        }
    }

    /**
     * Hidden constructor.  
     * 
     * @param builder the builder
     * @throws NullPointerException if a required input is null
     * @throws IllegalArgumentException if some input is inconsistent
     */

    private PoolOfPairsSupplier( PoolOfPairsSupplierBuilder<L, R> builder )
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

        // Validate
        String messageStart = "Cannot build the pool supplier: ";

        Objects.requireNonNull( this.right, messageStart + "add a right data source." );

        Objects.requireNonNull( this.pairer, messageStart + "add a pairer, in order to pair the data." );

        Objects.requireNonNull( this.metadata, messageStart + "add the metadata for the main pairs." );

        if ( Objects.isNull( this.desiredTimeScale ) )
        {
            LOGGER.debug( "While constructing a pool supplier for {}, "
                          + "discovered that the desired time scale was undefined.",
                          this.metadata );
        }

        if ( Objects.isNull( this.inputs ) )
        {
            LOGGER.debug( "While constructing a pool supplier for {}, "
                          + "discovered that the inputs declaration was undefined.",
                          this.metadata );
        }

        // Needs a climatology source OR a left source
        if ( Objects.isNull( this.climatology ) && Objects.isNull( this.left ) )
        {
            throw new NullPointerException( messageStart + "add either a left or a climatological data source." );
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

}
