package wres.datamodel.bootstrap;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.TreeSet;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.CrossPair;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
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
 * <p>This implementation assumes regular time-series. Specifically, the timestep between each valid time in every
 * time series should be constant. Likewise, the duration between the first valid times in consecutive time-series
 * should be constant. The transition probability between time-series is calculated with respect to the mean block size,
 * which is supplied in timestep units, and the offset between time-series. Specifically, the number of mean blocks per
 * offset is calculated and adjusted so that it is 1 or larger and used to calculate the transition probability as
 * 1 / adjusted mean blocks per offset. In short, if the offset between time-series is larger than the timestep, then
 * the probability of no relationship (i.e., random sampling) between the first valid times in adjacent time-series is
 * increased.
 *
 * <p>TODO: Practically speaking, missing data will be encountered, whether formally (i.e., a missing value sentinel)
 * or via absence. When values are missing by absence, this implementation should be able to adjust the transition
 * probability between timesteps based on duration (i.e., reducing the probability of sampling the "next" time from the
 * previous sequence in proportion to the ratio of the actual timestep to the average timestep). This is already done
 * when the duration between consecutive time-series exceeds the timestep.
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

    /** A resample executor. */
    private final ExecutorService resampleExecutor;

    /**
     * Create an instance.
     * @param <T> the type of time-series event
     * @param pool the pool to resample, required
     * @param meanBlockSizeInTimesteps the mean block size in timestep units, which must be greater than zero
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @param resampleExecutor an executor service for resampling work, optional
     * @return an instance
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size is less than or equal to zero or the pairs are invalid
     */

    public static <T> StationaryBootstrapResampler<T> of( Pool<TimeSeries<T>> pool,
                                                          long meanBlockSizeInTimesteps,
                                                          RandomGenerator randomGenerator,
                                                          ExecutorService resampleExecutor )
    {
        return new StationaryBootstrapResampler<>( pool,
                                                   meanBlockSizeInTimesteps,
                                                   randomGenerator,
                                                   resampleExecutor );
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

        // Generate the common indexes to resample across mini pools and both the main/baseline pairs. This assumes
        // perfect statistical dependence across the mini pools and main/baseline pairs
        List<ResampleIndexes> indexes = this.generateIndexesForResampling( this.main.get( 0 ) );

        // Generate the samples using the common sample structure/indexes across the mini-pools and main/baseline series
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

        // One set of resample indexes for each time-series, indicating where to obtain the event values for that series
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

        // Execute the time-series resampling in parallel as this can be time-consuming for large time-series, mainly
        // adding the time-series events to a sorted set. This will only improve performance when the series count is
        // greater than one, so not for a single, long, time-series
        List<CompletableFuture<TimeSeries<T>>> futures = new ArrayList<>();
        for ( int i = 0; i < original.size(); i++ )
        {
            TimeSeries<T> nextSeries = original.get( i );
            UnaryOperator<TimeSeries<T>> resampler = this.getTimeSeriesResampler( pool, resampleIndexes, i );
            CompletableFuture<TimeSeries<T>> future =
                    CompletableFuture.supplyAsync( () -> resampler.apply( nextSeries ),
                                                   this.resampleExecutor );
            futures.add( future );
        }

        // Advance the futures
        try
        {
            for ( CompletableFuture<TimeSeries<T>> next : futures )
            {
                TimeSeries<T> resampled = next.get();
                resampledPool.add( resampled );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new ResamplingException( "Encountered an error while attempting to resample a time-series.", e );
        }
        catch ( ExecutionException e )
        {
            throw new ResamplingException( "Encountered an error while attempting to resample a time-series.", e );
        }

        return Collections.unmodifiableList( resampledPool );
    }

    /**
     * Creates a function that resamples a time-series.
     * @param pool the pool
     * @param resampleIndexes the resample indexes
     * @param seriesIndex the series index
     * @return the resampled time-series
     */

    private UnaryOperator<TimeSeries<T>> getTimeSeriesResampler( BootstrapPool<T> pool,
                                                                 List<ResampleIndexes> resampleIndexes,
                                                                 int seriesIndex )
    {
        return nextSeries ->
        {
            TimeSeries.Builder<T> builder = new TimeSeries.Builder<T>().setMetadata( nextSeries.getMetadata() );
            ResampleIndexes indexes = resampleIndexes.get( seriesIndex );
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

            return builder.build();
        };
    }

    /**
     * Creates an instance.
     * @param pool the pool to resample, required
     * @param meanBlockSizeInTimesteps the block size, which must be greater than zero
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @param resampleExecutor an executor service for resampling work
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size is less than or equal to zero or the pairs are invalid
     */

    private StationaryBootstrapResampler( Pool<TimeSeries<T>> pool,
                                          long meanBlockSizeInTimesteps,
                                          RandomGenerator randomGenerator,
                                          ExecutorService resampleExecutor )
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

        // Warn about missing data or irregular time-series. Perhaps more sophistication could be introduced in future
        // to account for missing data, but the current approach assumes regular time-series without a lot of missing
        // data. Warn, but allow because even a single missing event among many could trigger this check
        if ( timesteps.size() > 1 )
        {
            LOGGER.warn( "While resampling a pool to estimate the sampling uncertainties of the statistics, discovered "
                         + "more than one timestep among the time-series present, which may be caused by missing data "
                         + "or irregular time-series. The resampling technique assumes regular timeseries with a "
                         + "consistent transition probability between adjacent times. If the time-series contain a lot "
                         + "of missing data or irregular time-series, the sampling uncertainty estimates may be "
                         + "unreliable. The following timesteps were discovered: {}.", timesteps );
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

        // Cross-pair the main and baseline pairs with each other across all "mini pools". Resampling only works if
        // there is a consistent structure across the main and baseline pairs and across "mini pools" because perfect
        // statistical dependence is assumed, i.e., one structure for everything.
        pool = this.getCrossPairedTimeSeries( pool );

        double pProb = 1.0 / meanBlockSizeInTimesteps;
        this.p = new BinomialDistribution( randomGenerator, 1, pProb );
        double qProb = this.getTransitionProbabilityBetweenSeries( pProb,
                                                                   timesteps,
                                                                   timeOffsets,
                                                                   meanBlockSizeInTimesteps );
        this.q = new BinomialDistribution( randomGenerator, 1, qProb );

        this.randomGenerator = randomGenerator;

        if ( Objects.isNull( resampleExecutor ) )
        {
            this.resampleExecutor = ForkJoinPool.commonPool();
        }
        else
        {
            this.resampleExecutor = resampleExecutor;
        }

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
     * Cross-pairs all mini-pools and main/baseline time-series in the inputs.
     * @param pool the pool for which cross-pairing is needed
     * @return the cross-paired pool
     */

    private Pool<TimeSeries<T>> getCrossPairedTimeSeries( Pool<TimeSeries<T>> pool )
    {
        List<Pool<TimeSeries<T>>> miniPools = pool.getMiniPools();
        if ( miniPools.size() == 1 && !pool.hasBaseline() )
        {
            LOGGER.debug( "No cross-pairing required for the pool to resample." );
            return pool;
        }

        LOGGER.debug( "The pool to resample contains {} mini-pools.", miniPools.size() );
        TimeSeriesCrossPairer<T> crossPairer = TimeSeriesCrossPairer.of( CrossPair.FUZZY );

        // Take the main time-series from the first mini-pool and cross-pair against all other main and baseline pairs
        // from all other mini-pools. Then use this to cross-pair the main and baseline time-series across all other
        // mini-pools.  If the structure is changed following cross pairing, then warn about this.
        Pool<TimeSeries<T>> firstPool = miniPools.get( 0 );
        List<TimeSeries<T>> first = firstPool.get();
        for ( int i = 1; i < miniPools.size(); i++ )
        {
            Pool<TimeSeries<T>> next = miniPools.get( i );
            List<TimeSeries<T>> nextMain = next.get();
            first = crossPairer.apply( first, nextMain )
                               .getFirstPairs();
            // Baseline too?
            if ( next.hasBaseline() )
            {
                List<TimeSeries<T>> nextBaseline = next.getBaselineData()
                                                       .get();
                first = crossPairer.apply( first, nextBaseline )
                                   .getFirstPairs();
            }
        }

        Pool.Builder<TimeSeries<T>> poolBuilder =
                new Pool.Builder<TimeSeries<T>>().setMetadata( pool.getMetadata() )
                                                 .setClimatology( pool.getClimatology() );
        if ( pool.hasBaseline() )
        {
            poolBuilder.setMetadataForBaseline( pool.getBaselineData()
                                                    .getMetadata() );
        }

        // Finally, cross pair with the first baseline to provide the first set of main and baseline pairs
        Pool.Builder<TimeSeries<T>> firstPoolBuilder =
                new Pool.Builder<TimeSeries<T>>().setClimatology( firstPool.getClimatology() )
                                                 .setMetadata( firstPool.getMetadata() );
        if ( firstPool.hasBaseline() )
        {
            List<TimeSeries<T>> nextBaseline = firstPool.getBaselineData()
                                                        .get();
            CrossPairs<T> crossPaired = crossPairer.apply( first, nextBaseline );
            first = crossPaired.getFirstPairs();
            firstPoolBuilder.setMetadataForBaseline( firstPool.getBaselineData()
                                                              .getMetadata() );

            firstPoolBuilder.addDataForBaseline( crossPaired.getSecondPairs() );
        }

        // Add the main pairs to the first pool
        firstPoolBuilder.addData( first );

        // Add the first pool
        poolBuilder.addPool( firstPoolBuilder.build() );

        // Add all the other pools, cross-pairing against the first pairs, which have now been cross-paired against all
        // other time-series
        for ( int i = 1; i < miniPools.size(); i++ )
        {
            Pool<TimeSeries<T>> next = miniPools.get( i );
            Pool.Builder<TimeSeries<T>> nextPoolBuilder =
                    new Pool.Builder<TimeSeries<T>>().setClimatology( next.getClimatology() )
                                                     .setMetadata( next.getMetadata() );
            List<TimeSeries<T>> nextMain = next.get();
            List<TimeSeries<T>> crossPaired = crossPairer.apply( nextMain, first )
                                                         .getFirstPairs();
            nextPoolBuilder.addData( crossPaired );

            // Baseline too?
            if ( next.hasBaseline() )
            {
                Pool<TimeSeries<T>> nextBaselinePool = next.getBaselineData();
                List<TimeSeries<T>> nextBaseline = nextBaselinePool.get();
                List<TimeSeries<T>> crossPairedBaseline = crossPairer.apply( nextBaseline, first )
                                                                     .getFirstPairs();
                nextPoolBuilder.addDataForBaseline( crossPairedBaseline );
                nextPoolBuilder.setMetadataForBaseline( nextBaselinePool.getMetadata() );
            }

            // Add the next mini-pool
            poolBuilder.addPool( nextPoolBuilder.build() );
        }

        Pool<TimeSeries<T>> crossPairedPool = poolBuilder.build();

        // Check that the cross-pairing has not led to empty pools
        this.checkForEmptyPools( crossPairedPool, pool );

        // Report the effects of cross-pairing in terms of any time-series and events removed
        this.reportEffectsOfCrossPairing( crossPairedPool, pool );

        return crossPairedPool;
    }

    /**
     * Checks that there are no empty pools following cross-pairing that were not already present in the original pool.
     *
     * @param crossPairedPool the cross-paired pool
     * @param originalPool the original pool
     * @throws ResamplingException if cross pairing produced empty pools
     */

    private void checkForEmptyPools( Pool<TimeSeries<T>> crossPairedPool,
                                     Pool<TimeSeries<T>> originalPool )
    {
        List<Pool<TimeSeries<T>>> miniPoolsCross = crossPairedPool.getMiniPools();
        List<Pool<TimeSeries<T>>> miniPoolsOrig = originalPool.getMiniPools();

        String start = "When attempting to resample the pooled data, discovered that there were no common time-series "
                       + "and associated events across the ";
        String end = " pairs within the sub-pools that compose the overall pool. Resampling requires at least some "
                     + "common time-series events because the same events are resampled across the sub-pools, "
                     + "including the main and baseline datasets, where applicable. When estimating the sampling "
                     + "uncertainties, please ensure that there are some common time-series across the datasets that "
                     + "compose each pool or remove the sampling uncertainty assessment.";

        boolean mainError = false;
        boolean baselineError = false;

        for ( int i = 0; i < miniPoolsCross.size(); i++ )
        {
            if ( miniPoolsCross.get( i )
                               .get()
                               .isEmpty() != miniPoolsOrig.get( i )
                                                          .get()
                                                          .isEmpty() )
            {
                mainError = true;
            }

            if ( crossPairedPool.hasBaseline() && miniPoolsCross.get( i )
                                                                .getBaselineData()
                                                                .get()
                                                                .isEmpty() != miniPoolsOrig.get( i )
                                                                                           .getBaselineData()
                                                                                           .get()
                                                                                           .isEmpty() )
            {
                baselineError = true;
            }
        }

        if ( mainError && baselineError )
        {
            throw new ResamplingException( start + "main and baseline" + end );
        }
        else if ( mainError )
        {
            throw new ResamplingException( start + "main" + end );
        }
        else if ( baselineError )
        {
            throw new ResamplingException( start + "baseline" + end );
        }
    }

    /**
     * Reports the effects of cross-pairing on the data available for resampling.
     *
     * @param crossPairedPool the cross-paired pool
     * @param originalPool the original pool
     */

    private void reportEffectsOfCrossPairing( Pool<TimeSeries<T>> crossPairedPool,
                                              Pool<TimeSeries<T>> originalPool )
    {
        // Report at debug level
        if ( !LOGGER.isDebugEnabled() )
        {
            return;
        }

        int crossPairSeriesCountMain = 0;
        int crossPairEventCountMain = 0;

        for ( TimeSeries<T> next : crossPairedPool.get() )
        {
            crossPairSeriesCountMain += 1;
            crossPairEventCountMain += next.getEvents()
                                           .size();
        }

        int originalSeriesCountMain = 0;
        int originalEventCountMain = 0;

        for ( TimeSeries<T> next : originalPool.get() )
        {
            originalSeriesCountMain += 1;
            originalEventCountMain += next.getEvents()
                                          .size();
        }

        int originalSeriesCountBaseline = 0;
        int originalEventCountBaseline = 0;

        int crossPairSeriesCountBaseline = 0;
        int crossPairEventCountBaseline = 0;

        if ( crossPairedPool.hasBaseline() )
        {
            for ( TimeSeries<T> next : crossPairedPool.getBaselineData()
                                                      .get() )
            {
                crossPairSeriesCountBaseline += 1;
                crossPairEventCountBaseline += next.getEvents()
                                                   .size();
            }

            for ( TimeSeries<T> next : originalPool.getBaselineData()
                                                   .get() )
            {
                originalSeriesCountBaseline += 1;
                originalEventCountBaseline += next.getEvents()
                                                  .size();
            }
        }

        if ( crossPairSeriesCountMain != originalSeriesCountMain
             || crossPairEventCountMain != originalEventCountMain
             || crossPairSeriesCountBaseline != originalSeriesCountBaseline
             || crossPairEventCountBaseline != originalEventCountBaseline )
        {
            LOGGER.debug( "When cross-pairing the resampled time-series to ensure a common structure across all "
                          + "time-series, discovered that some time-series or events were removed from the "
                          + "original pool. This is necessary for sample uncertainty estimation with the "
                          + "stationary bootstrap, which assumes perfect statistical dependence across the sub-pools "
                          + "and main/baseline datasets within an overall pool. However, when the number or "
                          + "length of time-series differs greatly across the various datasets within a pool, the "
                          + "sampling uncertainty estimates may not be very reliable. The number of time-series in the "
                          + "original pool is {} and the number of time-series following cross-pairing is {}. The "
                          + "number of time-series events in the original pool is {} and the number of time-series "
                          + "events following cross-pairing is {}. The number of baseline time-series in the original "
                          + "pool is {} and the number of baseline time-series following cross-pairing is {}. The "
                          + "number of baseline time-series events in the original pool is {} and the number of "
                          + "baseline time-series events following cross-pairing is {}.",
                          originalSeriesCountMain,
                          crossPairSeriesCountMain,
                          originalEventCountMain,
                          crossPairEventCountMain,
                          originalSeriesCountBaseline,
                          crossPairSeriesCountBaseline,
                          originalEventCountBaseline,
                          crossPairEventCountBaseline );
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
     *
     * @param indexes the sample indexes
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
