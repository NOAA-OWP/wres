package wres.datamodel.pools.bootstrap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import java.util.TreeSet;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * <p>Resamples a pool of time-series supplied on construction. The time-series are resampled using the stationary block
 * bootstrap with a block size that is geometrically distributed with a probability of success, p, and a mean block
 * size of 1/p. For more information on the stationary block bootstrap, see:
 *
 * <p>Politis, D. N. and Romano, J. P. (1994). The Stationary Bootstrap. Journal of the American Statistical
 * Association, 89:428, 1303-1313.
 *
 * <p>When the pool contains a baseline, the baseline time-series must be the same in number and shape as the main
 * time-series. The baseline time-series are further assumed to be perfectly statistically dependent on the main
 * time-series. In other words, the same sample structure (block sizes and positions) is used to sample both sets of
 * time-series. Similarly, when the pool is composed of "mini-pools", such as different geographic features, the same
 * sample structure is applied across all mini-pools. Forecasts are treated differently than non-forecasts because
 * forecast time-series are unlikely to be stationary across different lead durations. Thus, forecast time-series are
 * sampled with respect to a fixed lead duration; in other words, the candidate events for resampling of a nominated
 * event all have the same lead duration as the nominated event.
 *
 * <p>This implementation requires regular time-series. Specifically, the timestep between each valid time in every
 * time series must be constant. Likewise, the duration between the first valid times in consecutive time-series must be
 * constant. The transition probability between time-series is calculated with respect to the mean block size, which is
 * supplied in timestep units, and the offset between time-series. Specifically, the number of mean blocks per offset is
 * calculated and adjusted so that it is 1 or larger and used to calculate the transition probability as 1 / adjusted
 * mean blocks per offset. In short, if the offset between time-series is larger than the timestep, then the probability
 * of no relationship (i.e., random sampling) between the first valid times in adjacent time-series is increased.
 *
 * @author James Brown
 */
public class StationaryBootstrapResampler<T>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( StationaryBootstrapResampler.class );

    /** The raw time-series from the main pool from which pseudo-time-series are resampled. One for each "mini-pool". */
    private final List<BootstrapPool<T>> main;

    /** The raw time-series from the baseline pool from which pseudo-time-series are resampled. One for each
     * "mini-pool". */
    private final List<BootstrapPool<T>> baseline;

    /** The original pool. */
    private final Pool<TimeSeries<T>> pool;

    /** The probability, p, with which to sample the next valid time randomly, where 1-p is the probability with which
     * to sample the value adjacent to the last sample. */
    private final BinomialDistribution p;

    /** The probability, q, with which to sample the next time-series randomly, where 1-q is the probability with which
     * to sample the time-series adjacent to the last sample. */
    private final BinomialDistribution q;

    /** A random number generator. */
    private final RandomGenerator randomGenerator;

    /**
     * Create an instance.
     * @param <T> the type of time-series event
     * @param pool the pool to resample, required
     * @param meanBlockSizeInTimesteps the mean block size in timestep units, which must be greater than zero
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size is less than or equal to zero or the pairs are invalid
     */

    public static <T> StationaryBootstrapResampler<T> of( Pool<TimeSeries<T>> pool,
                                                          long meanBlockSizeInTimesteps,
                                                          RandomGenerator randomGenerator )
    {
        return new StationaryBootstrapResampler<>( pool,
                                                   meanBlockSizeInTimesteps,
                                                   randomGenerator );
    }

    /**
     * Generates a realization of the pool.
     *
     * @return a realization
     */

    public Pool<TimeSeries<T>> resample()
    {
        Pool.Builder<TimeSeries<T>> poolBuilder = new Pool.Builder<>();
        poolBuilder.setMetadata( this.pool.getMetadata() )
                   .setClimatology( this.pool.getClimatology() );

        if ( this.pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( this.pool.getBaselineData()
                                                         .getMetadata() );
        }

        // Generate the common indexes to resample across mini pools and both main/baseline pairs. This assumes perfect
        // statistical dependence across mini pools and main/baseline pairs
        List<ResampleIndexes> indexes = this.generateIndexesForResampling( this.main.get( 0 ) );

        for ( int i = 0; i < this.main.size(); i++ )
        {
            BootstrapPool<T> nextPool = this.main.get( i );
            List<TimeSeries<T>> nextMain = this.resample( nextPool, indexes );
            Pool.Builder<TimeSeries<T>> innerPoolBuilder = new Pool.Builder<>();
            innerPoolBuilder.setMetadata( nextPool.getPool()
                                                  .getMetadata() )
                            .setClimatology( nextPool.getPool()
                                                     .getClimatology() )
                            .addData( nextMain );

            if ( this.pool.hasBaseline() )
            {
                BootstrapPool<T> nextBaselinePool = this.baseline.get( i );
                List<TimeSeries<T>> nextBaseline = this.resample( nextBaselinePool, indexes );
                innerPoolBuilder.setMetadataForBaseline( nextBaselinePool.getPool()
                                                                         .getMetadata() )
                                .setClimatology( nextBaselinePool.getPool()
                                                                 .getClimatology() )
                                .addDataForBaseline( nextBaseline );
            }

            poolBuilder.addPool( innerPoolBuilder.build() );
        }

        return poolBuilder.build();
    }

    /**
     * Generates the indexes for resampling the pool.
     *
     * @param pool the pool
     * @return the indexes to resample
     */

    private List<ResampleIndexes> generateIndexesForResampling( BootstrapPool<T> pool )
    {
        // Build a list of sample indexes
        List<TimeSeries<T>> poolSeries = pool.getPool()
                                             .get();

        List<ResampleIndexes> indexes = new ArrayList<>();
        for ( int i = 0; i < poolSeries.size(); i++ )
        {
            TimeSeries<T> nextSeries = poolSeries.get( i );
            ResampleIndexes nextIndexes;

            // Forecast time-series which are probably non-stationary across lead durations, unless they are based
            // on climatology
            if ( !nextSeries.getReferenceTimes()
                            .isEmpty() )
            {
                nextIndexes = this.generateIndexesForResamplingFromForecastSeries( nextSeries,
                                                                                   i,
                                                                                   pool,
                                                                                   indexes );
            }
            // Non-forecast time-series (of which there is only one, as established on construction)
            else
            {
                nextIndexes = this.generateIndexesForResamplingFromNonForecastSeries( nextSeries );
            }

            indexes.add( nextIndexes );
        }

        return Collections.unmodifiableList( indexes );
    }

    /**
     * Generates a set of indexes for resampling of a forecast time-series. A forecast time-series is assumed to be
     * stationary with respect to event index or "lead duration". In other words, the candidate events for sampling
     * must originate from the same event index in other time-series.
     *
     * @param series the time-series to resample
     * @param seriesIndex the series index
     * @param pool the pool
     * @param indexes the existing resampled indexes
     * @return the resample indexes for the current time-series
     */

    private ResampleIndexes generateIndexesForResamplingFromForecastSeries( TimeSeries<T> series,
                                                                            int seriesIndex,
                                                                            BootstrapPool<T> pool,
                                                                            List<ResampleIndexes> indexes )
    {
        int events = series.getEvents()
                           .size();

        List<int[]> nextIndexes = new ArrayList<>();
        List<List<Event<T>>> eventsToSample = pool.getTimeSeriesWithAtLeastThisManyEvents( events );
        int seriesCount = eventsToSample.size();

        // Iterate the events. Since these are forecasts, samples are only taken from the prescribed event index, so
        // the only random sampling is with respect to time-series, not event indexes within time-series
        for ( int eventIndex = 0; eventIndex < events; eventIndex++ )
        {
            int nextSeriesIndex;

            // Very first sample, select a series randomly
            if ( seriesIndex == 0 && eventIndex == 0 )
            {
                nextSeriesIndex = this.getFirstSampleFromFirstSeries( seriesCount );
            }
            // Remaining events in the first series
            else if ( seriesIndex == 0 )
            {
                nextSeriesIndex = this.getSampleThatIsNotFirstFromFirstSeries( seriesCount, eventIndex, nextIndexes );
            }
            // First event from a series that is not the first series
            else if ( seriesIndex > 0 && eventIndex == 0 )
            {
                nextSeriesIndex = this.getFirstSampleFromSeriesThatIsNotFirstSeries( seriesCount,
                                                                                     seriesIndex,
                                                                                     indexes );
            }
            // Event that is not the first event from a series that is not the first series
            else
            {
                nextSeriesIndex = this.getSampleThatIsNotFirstFromSeriesThatIsNotFirst( seriesCount,
                                                                                        eventIndex,
                                                                                        nextIndexes );
            }

            nextIndexes.add( new int[] { nextSeriesIndex, eventIndex } );
        }

        return new ResampleIndexes( nextIndexes );
    }

    /**
     * Generate a resample index for the first sample from the first series.
     *
     * @param seriesCount the number of time-series
     * @return the randomly chosen time-series index
     */
    private int getFirstSampleFromFirstSeries( int seriesCount )
    {
        return this.getRandomIndex( seriesCount );
    }

    /**
     * Generate a resample index for an event within the first series that is not the first event.
     *
     * @param seriesCount the number of time-series
     * @param eventIndex the event index
     * @param existingIndexes the indexes already sampled for the first series
     * @return the randomly chosen time-series index
     */
    private int getSampleThatIsNotFirstFromFirstSeries( int seriesCount,
                                                        int eventIndex,
                                                        List<int[]> existingIndexes )
    {
        int nextSeriesIndex;

        // Choose the next event from a randomly selected series with probability p
        if ( this.p.sample() == 1 )
        {
            nextSeriesIndex = this.getRandomIndex( seriesCount );
        }
        // Choose the next event from the series used for the last index with probability 1-p
        else
        {
            int[] lastIndex = existingIndexes.get( eventIndex - 1 );
            nextSeriesIndex = lastIndex[0];
        }

        return nextSeriesIndex;
    }

    /**
     * Generate a resample index for the first sample from the first series.
     *
     * @param seriesCount the number of time-series
     * @param seriesIndex the series index
     * @param indexes the indexes already sampled for other series
     * @return the randomly chosen time-series index
     */
    private int getFirstSampleFromSeriesThatIsNotFirstSeries( int seriesCount,
                                                              int seriesIndex,
                                                              List<ResampleIndexes> indexes )
    {
        int nextSeriesIndex;

        // Choose the next event from a randomly selected series with probability q
        if ( this.q.sample() == 1 )
        {
            nextSeriesIndex = this.getRandomIndex( seriesCount );
        }
        // Choose the next event from the series "next" to the one used for the first index in the last series
        // with probability 1-q
        else
        {
            ResampleIndexes lastSeriesIndexes = indexes.get( seriesIndex - 1 );
            int[] lastIndex = lastSeriesIndexes.indexes()
                                               .get( 0 );

            nextSeriesIndex = lastIndex[0] + 1;

            // Wrap around to beginning of pool
            if ( nextSeriesIndex >= seriesCount )
            {
                nextSeriesIndex = 0;
            }
        }

        return nextSeriesIndex;
    }

    /**
     * Generate a resample index for an event within the first series that is not the first event.
     *
     * @param seriesCount the series count
     * @param eventIndex the event index
     * @param existingIndexes the indexes already sampled for the first series
     * @return the randomly chosen time-series index
     */
    private int getSampleThatIsNotFirstFromSeriesThatIsNotFirst( int seriesCount,
                                                                 int eventIndex,
                                                                 List<int[]> existingIndexes )
    {
        // Same as acquiring a sample from the first series that is not the first sample
        return this.getSampleThatIsNotFirstFromFirstSeries( seriesCount, eventIndex, existingIndexes );
    }

    /**
     * Generates a set of indexes for resampling of a non-forecast time-series. The candidate events for resampling a
     * nominated event in a non-forecast series are all events across the supplied series. In other words, the sampling
     * is unconstrained, except for the constraints of statistical dependence imposed by the bootstrap resampling
     * itself.
     *
     * @param series the time-series to resample
     * @return the resample indexes for the current time-series
     */

    private ResampleIndexes generateIndexesForResamplingFromNonForecastSeries( TimeSeries<T> series )
    {
        int events = series.getEvents()
                           .size();

        List<int[]> nextIndexes = new ArrayList<>();
        for ( int eventIndex = 0; eventIndex < events; eventIndex++ )
        {
            int nextEventIndex;

            // Very first sample, select an event randomly
            if ( eventIndex == 0 )
            {
                nextEventIndex = this.getRandomIndex( events );
            }
            // Remaining events in the first series
            else
            {
                // Choose the next event at random with probability p
                if ( this.p.sample() == 1 )
                {
                    nextEventIndex = this.getRandomIndex( events );
                }
                // Choose the event next to the last index with probability 1-p
                else
                {
                    int[] lastIndex = nextIndexes.get( eventIndex - 1 );
                    nextEventIndex = lastIndex[1] + 1;

                    // Wrap the series
                    if ( nextEventIndex >= events )
                    {
                        nextEventIndex = 0;
                    }
                }
            }

            nextIndexes.add( new int[] { 0, nextEventIndex } );
        }

        return new ResampleIndexes( nextIndexes );
    }

    /**
     * Returns a random index between 0 and the specified value minus one.
     *
     * @param upperBoundExclusive the upper bound exclusive
     */

    private int getRandomIndex( int upperBoundExclusive )
    {
        return this.randomGenerator.nextInt( upperBoundExclusive );
    }

    /**
     * Generates a sample of a pool from the prescribed list of indexes, one set of indexes for each time-series in the
     * pool.
     *
     * @param pool the pool to resample
     * @param resampleIndexes the indexes to resample
     * @return the resampled pool
     */

    private List<TimeSeries<T>> resample( BootstrapPool<T> pool, List<ResampleIndexes> resampleIndexes )
    {
        List<TimeSeries<T>> resampledPool = new ArrayList<>();

        List<TimeSeries<T>> original = pool.getPool()
                                           .get();

        for ( int i = 0; i < original.size(); i++ )
        {
            TimeSeries<T> nextSeries = original.get( i );
            TimeSeries.Builder<T> builder = new TimeSeries.Builder<T>().setMetadata( nextSeries.getMetadata() );
            ResampleIndexes indexes = resampleIndexes.get( i );
            List<Event<T>> events = new ArrayList<>( nextSeries.getEvents() );
            int eventCount = events.size();
            List<List<Event<T>>> eventsToSample = pool.getTimeSeriesWithAtLeastThisManyEvents( eventCount );

            for ( int j = 0; j < eventCount; j++ )
            {
                Event<T> nextEvent = events.get( j );
                int[] index = indexes.indexes()
                                     .get( j );
                Event<T> resampledValue = eventsToSample.get( index[0] )
                                                        .get( index[1] );
                Event<T> resampled = Event.of( nextEvent.getTime(), resampledValue.getValue() );
                builder.addEvent( resampled );
            }
            resampledPool.add( builder.build() );
        }

        return Collections.unmodifiableList( resampledPool );
    }

    /**
     * Creates an instance.
     * @param pool the pool to resample, required
     * @param meanBlockSizeInTimesteps the block size, which must be greater than zero
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size is less than or equal to zero or the pairs are invalid
     */

    private StationaryBootstrapResampler( Pool<TimeSeries<T>> pool,
                                          long meanBlockSizeInTimesteps,
                                          RandomGenerator randomGenerator )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( randomGenerator );

        if ( meanBlockSizeInTimesteps <= 0 )
        {
            throw new IllegalArgumentException( "The mean block size for the stationary bootstrap must be greater than "
                                                + "zero but was: " + meanBlockSizeInTimesteps + "." );
        }

        // Cannot have more than one non-forecast time-series in a pool: consolidate them first
        long nonForecastCount = pool.get()
                                    .stream()
                                    .filter( n -> n.getReferenceTimes()
                                                   .isEmpty() )
                                    .count();
        if ( nonForecastCount > 1 )
        {
            throw new IllegalArgumentException( "Cannot resample a pool that contains more than one non-forecast "
                                                + "time-series. These time-series should be consolidated prior to "
                                                + "resampling." );
        }

        // Obtain the constant duration between consecutive times
        SortedSet<Duration> timesteps = pool.get()
                                            .stream()
                                            .flatMap( n -> TimeSeriesSlicer.getTimesteps( n )
                                                                           .stream() )
                                            .collect( Collectors.toCollection( TreeSet::new ) );

        if ( pool.hasBaseline() )
        {
            pool.getBaselineData()
                .get()
                .stream()
                .flatMap( n -> TimeSeriesSlicer.getTimesteps( n )
                                               .stream() )
                .forEach( timesteps::add );
        }

        if ( timesteps.size() > 1 )
        {
            throw new IllegalArgumentException( "Cannot resample time-series that contain more than one timestep. "
                                                + "Discovered the following timesteps among the supplied time-series: "
                                                + timesteps
                                                + "." );
        }

        SortedSet<Duration> timeOffsets = new TreeSet<>( TimeSeriesSlicer.getTimeOffsets( pool.get() ) );

        if ( pool.hasBaseline() )
        {
            SortedSet<Duration> baselineOffsets = TimeSeriesSlicer.getTimeOffsets( pool.getBaselineData()
                                                                                       .get() );
            timeOffsets.addAll( baselineOffsets );
        }

        if ( timeOffsets.size() > 1 )
        {
            throw new IllegalArgumentException( "Cannot resample time-series whose earliest valid times are offset by "
                                                + "a varying duration. Discovered the following time offsets among the "
                                                + "supplied time-series: "
                                                + timeOffsets
                                                + "." );
        }

        double pProb = 1.0 / meanBlockSizeInTimesteps;
        this.p = new BinomialDistribution( randomGenerator, 1, pProb );
        double qProb = this.getTransitionProbabilityBetweenSeries( pProb,
                                                                   timesteps,
                                                                   timeOffsets,
                                                                   meanBlockSizeInTimesteps );
        this.q = new BinomialDistribution( randomGenerator, 1, qProb );

        this.randomGenerator = randomGenerator;

        // Create the common structure to resample
        List<Pool<TimeSeries<T>>> miniPools = pool.getMiniPools();
        List<BootstrapPool<T>> innerMain = new ArrayList<>();
        List<BootstrapPool<T>> innerBaseline = new ArrayList<>();
        for ( Pool<TimeSeries<T>> nextMini : miniPools )
        {
            BootstrapPool<T> nextMain = BootstrapPool.of( nextMini );
            innerMain.add( nextMain );
            if ( nextMini.hasBaseline() )
            {
                BootstrapPool<T> nextBaseline = BootstrapPool.of( nextMini.getBaselineData() );
                innerBaseline.add( nextBaseline );
            }
        }

        this.main = Collections.unmodifiableList( innerMain );
        this.baseline = Collections.unmodifiableList( innerBaseline );

        this.pool = pool;

        if ( LOGGER.isDebugEnabled() )
        {
            int baselineCount = 0;
            if ( pool.hasBaseline() )
            {
                baselineCount = pool.getBaselineData()
                                    .get()
                                    .size();
            }
            LOGGER.debug( "Created a stationary bootstrap resampler with a mean block size of {} timesteps, a "
                          + "probability of {} for randomly sampling consecutive timesteps within the same "
                          + "time-series, a probability of {} for randomly sampling consecutive time-series, "
                          + "and a pool with {} main time-series and {} baseline time-series.",
                          meanBlockSizeInTimesteps,
                          pProb,
                          qProb,
                          pool.get()
                              .size(),
                          baselineCount );
        }
    }

    /**
     * Calculates a transition probability between series based on the mean block size in timestep units and the number
     * of timesteps per offset between series.
     *
     * @param pProb the transition probability between times within series
     * @param timesteps the timesteps
     * @param timeOffsets the time offsets
     * @param meanBlockSizeInTimesteps the mean block size in timesteps
     * @return the transition probability between series
     */

    private double getTransitionProbabilityBetweenSeries( double pProb,
                                                          SortedSet<Duration> timesteps,
                                                          SortedSet<Duration> timeOffsets,
                                                          long meanBlockSizeInTimesteps )
    {
        double qProb = pProb;

        // Find the number of timesteps per offset and use that to calculate a transition probability between series
        if ( !timeOffsets.isEmpty()
             && !timesteps.isEmpty() )
        {
            Duration offset = timeOffsets.first();
            Duration timestep = timesteps.first();

            BigDecimal timestepDecimal = BigDecimal.valueOf( timestep.getSeconds() )
                                                   .add( BigDecimal.valueOf( timestep.getNano(), 999_999 ) );

            BigDecimal offsetDecimal = BigDecimal.valueOf( offset.getSeconds() )
                                                 .add( BigDecimal.valueOf( offset.getNano(), 999_999 ) );

            BigDecimal timestepsPerOffset = timestepDecimal.divide( offsetDecimal, RoundingMode.HALF_UP );

            double meanBlocksPerOffset = timestepsPerOffset.doubleValue() * meanBlockSizeInTimesteps;

            // If the number of blocks per offset is less than 1, adjust to 1, i.e., random sampling, because the block
            // size does not capture even 1 offset between time-series
            if ( meanBlocksPerOffset < 1.0 )
            {
                meanBlocksPerOffset = 1.0;
            }

            qProb = 1.0 / meanBlocksPerOffset;
        }

        return qProb;
    }

    /**
     * A record that contains the indexes to resample for a prescribed time-series. Each index pair refers to a
     * position within a {@link BootstrapPool} for the time-series returned by
     * {@link BootstrapPool#getTimeSeriesWithAtLeastThisManyEvents(int)}.
     */

    private record ResampleIndexes( List<int[]> indexes )
    {
        @Override
        public String toString()
        {
            StringJoiner joiner = new StringJoiner( ",", "(", ")" );
            for ( int[] next : indexes )
            {
                joiner.add( Arrays.toString( next ) );
            }

            return joiner.toString();
        }
    }

}
