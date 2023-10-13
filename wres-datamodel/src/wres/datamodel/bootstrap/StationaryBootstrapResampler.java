package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.CrossPairMethod;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.pairs.CrossPairs;
import wres.datamodel.space.FeatureGroup;
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
 * time series should be constant. However, the duration between the first valid times in consecutive time-series
 * can vary and is used to determine a transition probability, q, for resampling the "next" time-series within the same
 * pool. In this context, "next" means the time-series whose first valid time is larger the corresponding valid time in
 * the current time-series, but nearest in time among all of the time-series. The transition probability between
 * time-series, q, is calculated with respect to the mean block size, which is supplied in timestep units, and the
 * offset between time-series. Specifically, the number of mean blocks per offset is calculated and adjusted so that it
 * is 1 or larger and used to calculate the transition probability as 1 / adjusted mean blocks per offset. In short, if
 * the offset between time-series is larger than the timestep, then the probability of no relationship (i.e., random
 * sampling) between the first valid times in adjacent time-series is increased.
 *
 * <p><b>Implementation notes:</b>
 *
 * <p>The implementation described above is based partly on published material. However, as originally envisaged by
 * Politis and Romano (1994), the stationary bootstrap is used to resample a single, "observation-like", time-series,
 * not a collection of related time-series or forecasts. In short, this implementation is novel and based on unpublished
 * work. Put differently, it invokes assumptions about resampling pools that contain mixed data structures (e.g.,
 * several features, abstracted as "mini-pools") and about resampling forecast time-series, which have not been
 * presented elsewhere or rigorously tested (i.e., peer-reviewed).
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
     * to sample the time-series adjacent to the last sample. There is one distribution for each gap between the first
     * valid times of consecutive time-series, as well as between the last time-series and the first.*/
    private final Map<Duration, BinomialDistribution> q;

    /** A random number generator. */
    private final RandomGenerator randomGenerator;

    /** A resample executor. */
    private final ExecutorService resampleExecutor;

    /**
     * Create an instance.
     * @param <T> the type of time-series event
     * @param pool the pool to resample, required
     * @param meanBlockSizeInTimesteps the mean block size in timestep units, which must be greater than zero
     * @param timestep the timestep
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @param resampleExecutor an executor service for resampling work, optional
     * @return an instance
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size or timestep or pairs are invalid
     * @throws InsufficientDataForResamplingException if resampling cannot be performed due to lack of data
     */

    public static <T> StationaryBootstrapResampler<T> of( Pool<TimeSeries<T>> pool,
                                                          long meanBlockSizeInTimesteps,
                                                          Duration timestep,
                                                          RandomGenerator randomGenerator,
                                                          ExecutorService resampleExecutor )
    {
        return new StationaryBootstrapResampler<>( pool,
                                                   meanBlockSizeInTimesteps,
                                                   timestep,
                                                   randomGenerator,
                                                   resampleExecutor );
    }

    /**
     * Generates a realization of the pool.
     *
     * @return a realization
     * @throws ResamplingException if an exception was enountered on resampling
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
        List<ResampleIndexes> indexes = this.generateResampleIndexes( this.main.get( 0 ) );

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
     * @return the indexes to resample, which are based on the size-ordered time-series in the pool
     */

    private List<ResampleIndexes> generateResampleIndexes( BootstrapPool<T> pool )
    {
        // Group the time-series to resample based on the number of events within them and do the sampling separately
        // for each group. The resampled indexes are relative to series within each group, which are ordered by the
        // valid time of the first event within the group. For a given group, the source time-series for resampling
        // must contain at least as many events as the number of events for time-series within the group. IMPORTANT:
        // all indexing is performed relative to this size ordering, so resampling of the time-series must use the same
        // ordering
        List<List<TimeSeries<T>>> groupedBySize = pool.getOrderedTimeSeries();

        // One ResampledIndexes for each time-series, indicating where to obtain the event values for that series
        List<ResampleIndexes> outerIndexes = new ArrayList<>();
        for ( List<TimeSeries<T>> poolSeries : groupedBySize )
        {
            // Resampled indexes for the next group, which are relative to the grouped time-series
            List<ResampleIndexes> innerIndexes = new ArrayList<>();
            for ( int i = 0; i < poolSeries.size(); i++ )
            {
                TimeSeries<T> nextSeries = poolSeries.get( i );
                ResampleIndexes nextIndexes;

                // Forecast time-series which are probably non-stationary across lead durations, unless they are based
                // on climatology
                if ( !nextSeries.getReferenceTimes()
                                .isEmpty() )
                {
                    nextIndexes = this.generateResampleIndexesForForecastSeries( nextSeries,
                                                                                 i,
                                                                                 pool,
                                                                                 innerIndexes );
                }
                // Non-forecast time-series (of which there is only one, as established on construction)
                else
                {
                    nextIndexes = this.generateResampleIndexesForNonForecastSeries( nextSeries,
                                                                                    i,
                                                                                    pool,
                                                                                    innerIndexes );
                }

                innerIndexes.add( nextIndexes );
            }

            outerIndexes.addAll( innerIndexes );
        }

        return Collections.unmodifiableList( outerIndexes );
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

    private ResampleIndexes generateResampleIndexesForForecastSeries( TimeSeries<T> series,
                                                                      int seriesIndex,
                                                                      BootstrapPool<T> pool,
                                                                      List<ResampleIndexes> indexes )
    {
        int events = series.getEvents()
                           .size();

        List<int[]> nextIndexes = new ArrayList<>();
        // Can only sample from time-series with as many events as the template series
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
            else if ( seriesIndex == 0 )   // && eventIndex > 0 implied
            {
                nextSeriesIndex = this.getSampleThatIsNotFirstFromFirstSeries( seriesCount, eventIndex, nextIndexes );
            }
            // First event from a series that is not the first series
            else if ( seriesIndex > 0 && eventIndex == 0 )
            {
                nextSeriesIndex = this.getFirstSampleFromSeriesThatIsNotFirstSeries( seriesCount,
                                                                                     seriesIndex,
                                                                                     indexes,
                                                                                     eventsToSample );
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
     * Generate a resample index for the first sample from a series that is not the first series.
     *
     * @param seriesCount the number of time-series
     * @param seriesIndex the series index
     * @param indexes the indexes already sampled for other series
     * @param eventsToSample the events to sample
     * @return the randomly chosen time-series index
     */
    private int getFirstSampleFromSeriesThatIsNotFirstSeries( int seriesCount,
                                                              int seriesIndex,
                                                              List<ResampleIndexes> indexes,
                                                              List<List<Event<T>>> eventsToSample )
    {
        // There should be a last series, by definition of this method being called
        ResampleIndexes lastSeriesIndexes = indexes.get( seriesIndex - 1 );
        int[] lastIndex = lastSeriesIndexes.indexes()
                                           .get( 0 );

        int lastSeriesIndex = lastIndex[0];
        int nextSeriesIndex = lastSeriesIndex + 1;

        // Find the appropriate transition probability distribution, which depends on the gap. If the next index is
        // greater than the last available index, then circle back to the first index
        BinomialDistribution sampler;

        // Gap when restarting the pool. In this case, assume the default transition probability of p, rather than the
        // potentially much larger q that corresponds to the gap between the first and last value in the series. This
        // is debatable, but is the approach adopted in Politis and Romano (1994), albeit for a single,
        // observation-like time-series. To use the appropriate q probability instead, simply calculate the gap as
        // normal - the corresponding distribution should be available, as the gap between the first and last series is
        // among those created
        if ( nextSeriesIndex >= seriesCount )
        {
            nextSeriesIndex = 0;
            sampler = this.p;
        }
        // Gap when not restarting the pool
        else
        {
            // Find the duration between the last index and next index
            Instant first = eventsToSample.get( lastSeriesIndex )
                                          .get( 0 )
                                          .getTime();
            Instant second = eventsToSample.get( nextSeriesIndex )
                                           .get( 0 )
                                           .getTime();

            // Absolute gap
            Duration gap = Duration.between( first, second )
                                   .abs();
            sampler = this.q.get( gap );
        }

        // Choose the next event from a randomly selected series?
        if ( sampler.sample() == 1 )
        {
            nextSeriesIndex = this.getRandomIndex( seriesCount );
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
     * @param seriesIndex the series index
     * @param pool the pool
     * @param indexes the existing resampled indexes
     * @return the resample indexes for the current time-series
     */

    private ResampleIndexes generateResampleIndexesForNonForecastSeries( TimeSeries<T> series,
                                                                         int seriesIndex,
                                                                         BootstrapPool<T> pool,
                                                                         List<ResampleIndexes> indexes )
    {
        int eventCount = series.getEvents()
                               .size();
        // Non-forecast series are all exchangeable, no lead-time dependence
        List<List<Event<T>>> eventsToSample = pool.getTimeSeriesWithAllEvents();
        int seriesCount = eventsToSample.size();

        List<int[]> nextIndexes = new ArrayList<>();
        for ( int eventIndex = 0; eventIndex < eventCount; eventIndex++ )
        {
            int nextSeriesIndex;
            int nextEventIndex;

            // Very first sample, so select a series and event randomly
            if ( seriesIndex == 0
                 && eventIndex == 0 )
            {
                nextSeriesIndex = this.getRandomIndex( seriesCount );
                int seriesEventCount = eventsToSample.get( nextSeriesIndex )
                                                     .size();
                nextEventIndex = this.getRandomIndex( seriesEventCount );
            }
            // Start of a new series or continuation of an existing series
            else
            {
                // Find the index of the last sample taken
                int[] lastIndex;

                // Last sample comes from the local cache of indexes
                if ( eventIndex > 0 )
                {
                    lastIndex = nextIndexes.get( eventIndex - 1 );
                }
                // Last sample comes from the global cache of indexes
                else
                {
                    ResampleIndexes cachedIndex = indexes.get( seriesIndex - 1 );
                    List<int[]> lastIndexes = cachedIndex.indexes();
                    lastIndex = lastIndexes.get( lastIndexes.size() - 1 );
                }

                int[] nextIndex = this.generateResampleIndexForNonForecastSeries( lastIndex,
                                                                                  eventsToSample,
                                                                                  seriesCount );
                nextSeriesIndex = nextIndex[0];
                nextEventIndex = nextIndex[1];
            }

            nextIndexes.add( new int[] { nextSeriesIndex, nextEventIndex } );
        }

        return new ResampleIndexes( nextIndexes );
    }

    /**
     * Generates a resample index for a non-forecast time-series relative to the last sampled index.
     * @param lastIndex the last sampled index
     * @param eventsToSample the events to sample
     * @param seriesCount the number of time-series
     * @return the resample index
     */

    private int[] generateResampleIndexForNonForecastSeries( int[] lastIndex,
                                                             List<List<Event<T>>> eventsToSample,
                                                             int seriesCount )
    {
        int[] index;

        // Only one series
        if ( seriesCount == 1 )
        {
            index = this.generateResampleIndexWithinSeriesForNonForecastSeries( lastIndex,
                                                                                eventsToSample,
                                                                                seriesCount );
        }
        // Some values left to sample in the last series
        else if ( lastIndex[1] + 1 < eventsToSample.get( lastIndex[0] )
                                                   .size() )
        {
            index = this.generateResampleIndexWithinSeriesForNonForecastSeries( lastIndex,
                                                                                eventsToSample,
                                                                                seriesCount );
        }
        // No values left to sample in the last series, so need to move to a new series
        else
        {
            index = this.generateResampleIndexFromAnotherSeriesForNonForecastSeries( lastIndex,
                                                                                     eventsToSample,
                                                                                     seriesCount );
        }

        return index;
    }

    /**
     * Generates a resample index for a non-forecast time-series relative to the last sampled index where the sample
     * comes from the same series as the last sample.
     *
     * @param lastIndex the last sampled index
     * @param eventsToSample the events to sample
     * @param seriesCount the number of time-series
     * @return the resample index
     */

    private int[] generateResampleIndexWithinSeriesForNonForecastSeries( int[] lastIndex,
                                                                         List<List<Event<T>>> eventsToSample,
                                                                         int seriesCount )
    {
        int nextSeriesIndex;
        int nextEventIndex;

        // Sample the next value randomly with probability p
        if ( this.p.sample() == 1 )
        {
            nextSeriesIndex = this.getRandomIndex( seriesCount );
            int seriesEventCount = eventsToSample.get( nextSeriesIndex )
                                                 .size();
            nextEventIndex = this.getRandomIndex( seriesEventCount );
        }
        else
        {
            nextSeriesIndex = lastIndex[0];
            nextEventIndex = lastIndex[1] + 1;
            int seriesEventCount = eventsToSample.get( nextSeriesIndex )
                                                 .size();

            // Wrap the series
            if ( nextEventIndex >= seriesEventCount )
            {
                nextEventIndex = 0;
            }
        }

        return new int[] { nextSeriesIndex, nextEventIndex };
    }

    /**
     * Generates a resample index for a non-forecast time-series relative to the last sampled index where the sample
     * comes from different series than the last sample.
     * @param lastIndex the last sampled index
     * @param eventsToSample the events to sample
     * @param seriesCount the number of time-series
     * @return the resample index
     */

    private int[] generateResampleIndexFromAnotherSeriesForNonForecastSeries( int[] lastIndex,
                                                                              List<List<Event<T>>> eventsToSample,
                                                                              int seriesCount )
    {
        int nextSeriesIndex;
        int nextEventIndex;

        // Find the duration between the last index and next index
        Instant first = eventsToSample.get( lastIndex[0] )
                                      .get( lastIndex[1] )
                                      .getTime();
        int nextSeries = lastIndex[0] + 1;

        // Circular sampling, so return to first series in pool if needed. In this case, assume the default transition
        // probability of p, rather than the potentially much larger q that corresponds to the gap between the first
        // and last value in the series. This is debatable, but is the approach adopted in Politis and Romano (1994).
        // To use the appropriate q probability instead, simply calculate the gap as normal - the corresponding
        // distribution should be available, as the gap between the first and last series is among those created
        BinomialDistribution sampler;
        if ( nextSeries >= seriesCount )
        {
            nextSeries = 0;
            sampler = this.p;
        }
        else
        {
            Instant second = eventsToSample.get( nextSeries )
                                           .get( 0 ) // First event
                                           .getTime();

            // Absolute gap
            Duration gap = Duration.between( first, second )
                                   .abs();

            sampler = this.q.get( gap );
        }

        // Choose the next event from a randomly selected series with probability q
        if ( sampler.sample() == 1 )
        {
            nextSeriesIndex = this.getRandomIndex( seriesCount );
            int seriesEventCount = eventsToSample.get( nextSeriesIndex )
                                                 .size();
            nextEventIndex = this.getRandomIndex( seriesEventCount );
        }
        // Choose the first event from the next series
        else
        {
            nextSeriesIndex = nextSeries;
            nextEventIndex = 0;
        }

        return new int[] { nextSeriesIndex, nextEventIndex };
    }

    /**
     * Returns a random index between 0 and the specified value minus one.
     *
     * @param upperBoundExclusive the upper bound exclusive
     */

    private int getRandomIndex( int upperBoundExclusive )
    {
        // Zero-based indexing
        if ( upperBoundExclusive == 1 )
        {
            return 0;
        }

        return this.randomGenerator.nextInt( upperBoundExclusive );
    }

    /**
     * Generates a sample of a pool from the prescribed list of indexes, one set of indexes for each time-series in the
     * pool. The indexes use the ordering imposed by {@link #generateResampleIndexes(BootstrapPool)}.
     *
     * @param pool the pool to resample
     * @param resampleIndexes the indexes to resample
     * @return the resampled pool
     */

    private List<TimeSeries<T>> resample( BootstrapPool<T> pool, List<ResampleIndexes> resampleIndexes )
    {
        List<TimeSeries<T>> resampledPool = new ArrayList<>();

        // IMPORTANT: Use the same ordering that was used to generate the indexes
        List<List<TimeSeries<T>>> groupedBySize = pool.getOrderedTimeSeries();
        List<TimeSeries<T>> sizeOrder = groupedBySize.stream()
                                                     .flatMap( List::stream )
                                                     .toList();

        // Execute the time-series resampling in parallel as this can be time-consuming for large time-series, mainly
        // adding the time-series events to a sorted set. This will only improve performance when the series count is
        // greater than one, so not for a single, long, time-series
        List<CompletableFuture<TimeSeries<T>>> futures = new ArrayList<>();
        for ( int i = 0; i < sizeOrder.size(); i++ )
        {
            TimeSeries<T> nextSeries = sizeOrder.get( i );
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
            Thread.currentThread()
                  .interrupt();
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
            List<List<Event<T>>> eventsToSample;
            if ( pool.hasForecasts() )
            {
                eventsToSample = pool.getTimeSeriesWithAtLeastThisManyEvents( eventCount );
            }
            else
            {
                eventsToSample = pool.getTimeSeriesWithAllEvents();
            }

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
     * @param meanBlockSizeInTimesteps the block size in timestep units, which must be greater than zero
     * @param timestep the timestep
     * @param randomGenerator the random number generator to use when sampling block sizes, required
     * @param resampleExecutor an executor service for resampling work
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the block size or timestep or pairs are invalid
     */

    private StationaryBootstrapResampler( Pool<TimeSeries<T>> pool,
                                          long meanBlockSizeInTimesteps,
                                          Duration timestep,
                                          RandomGenerator randomGenerator,
                                          ExecutorService resampleExecutor )
    {
        Objects.requireNonNull( pool );
        Objects.requireNonNull( randomGenerator );
        Objects.requireNonNull( resampleExecutor );
        Objects.requireNonNull( timestep );

        if ( meanBlockSizeInTimesteps <= 0 )
        {
            throw new IllegalArgumentException( "The mean block size for the stationary bootstrap must be greater than "
                                                + "zero but was: " + meanBlockSizeInTimesteps + "." );
        }

        if ( timestep.isZero()
             || timestep.isNegative() )
        {
            throw new IllegalArgumentException( "The mean block size for the stationary bootstrap must be greater than "
                                                + "zero but was: " + meanBlockSizeInTimesteps + "." );
        }

        // Warn about missing data or irregular time-series. Perhaps more sophistication could be introduced in future
        // to account for missing data, but the current approach assumes regular time-series without a lot of missing
        // data. Warn, but allow because even a single missing event among many could trigger this check
        if ( LOGGER.isWarnEnabled() )
        {
            // Obtain the constant duration between consecutive times
            Set<Duration> timesteps = pool.get()
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
                LOGGER.warn( "While resampling a pool to estimate the sampling uncertainties of the statistics, "
                             + "discovered more than one timestep among the time-series present, which may be caused "
                             + "by missing data or irregular timesteps. The resampling technique assumes regular "
                             + "timeseries with a consistent transition probability between adjacent times. If the "
                             + "time-series contain a lot of missing data or irregular time-series, the sampling "
                             + "uncertainty estimates may be unreliable. The following timesteps were discovered: {}."
                             + " The pool is: {}.",
                             timesteps,
                             pool.getMetadata() );
            }
        }

        // Cross-pair the main and baseline pairs with each other across all "mini pools". Resampling only works if
        // there is a consistent structure across the main and baseline pairs and across "mini pools" because perfect
        // statistical dependence is assumed, i.e., one structure for everything.
        Pool<TimeSeries<T>> crossPool = this.getCrossPairedTimeSeries( pool );

        // Check that the cross-pairing has not led to empty pools
        this.validateCrossPairedData( crossPool, pool );

        // Report the effects of cross-pairing in terms of any time-series and events removed
        this.reportEffectsOfCrossPairing( crossPool, pool );

        // Set reference
        this.pool = crossPool;

        double pProb = 1.0 / meanBlockSizeInTimesteps;
        this.p = new BinomialDistribution( randomGenerator, 1, pProb );
        this.randomGenerator = randomGenerator;
        this.resampleExecutor = resampleExecutor;

        // Create the common structure to resample
        List<Pool<TimeSeries<T>>> miniPools = this.pool.getMiniPools();
        List<BootstrapPool<T>> innerMain = new ArrayList<>();
        List<BootstrapPool<T>> innerBaseline = new ArrayList<>();
        Set<Duration> timeOffsets = new HashSet<>(); // Time offsets per bootstrap pool
        for ( Pool<TimeSeries<T>> nextMini : miniPools )
        {
            BootstrapPool<T> nextMain = BootstrapPool.of( nextMini );
            timeOffsets.addAll( nextMain.getValidTimeOffsets() );
            innerMain.add( nextMain );
            if ( nextMini.hasBaseline() )
            {
                BootstrapPool<T> nextBaseline = BootstrapPool.of( nextMini.getBaselineData() );
                innerBaseline.add( nextBaseline );
                timeOffsets.addAll( nextBaseline.getValidTimeOffsets() );
            }
        }

        Set<Duration> uniqueTimeOffsets = new HashSet<>( timeOffsets );

        // Create the distributions to transition between time-series based on time offset of the first valid time
        this.q = this.getTransitionProbabilitiesBetweenSeries( timestep,
                                                               uniqueTimeOffsets,
                                                               meanBlockSizeInTimesteps,
                                                               this.randomGenerator );

        this.main = Collections.unmodifiableList( innerMain );
        this.baseline = Collections.unmodifiableList( innerBaseline );

        if ( LOGGER.isDebugEnabled() )
        {
            int baselineCount = 0;
            if ( this.pool.hasBaseline() )
            {
                baselineCount = this.pool.getBaselineData()
                                         .get()
                                         .size();
            }

            Map<Duration, Double> qProbs = this.q.entrySet()
                                                 .stream()
                                                 .collect( Collectors.toMap( Map.Entry::getKey,
                                                                             n -> n.getValue()
                                                                                   .getProbabilityOfSuccess() ) );

            LOGGER.debug( "Created a stationary bootstrap resampler with a mean block size of {} timesteps, a modal "
                          + "timestep of {}, a probability of {} for randomly sampling consecutive timesteps within "
                          + "the same time-series, probabilities of {} for randomly sampling consecutive time-series "
                          + "depending on the gap/duration between them, and a pool with {} main time-series and {} "
                          + "baseline time-series.",
                          meanBlockSizeInTimesteps,
                          timestep,
                          pProb,
                          qProbs,
                          this.pool.get()
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
        TimeSeriesCrossPairer<T> crossPairer = TimeSeriesCrossPairer.of( CrossPairMethod.FUZZY );

        // Take the main time-series from the first mini-pool and cross-pair against all other main and baseline pairs
        // from all other mini-pools. Then use this to cross-pair the main and baseline time-series across all other
        // mini-pools.
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

        return poolBuilder.build();
    }

    /**
     * Checks that there are no empty pools following cross-pairing that were not already present in the original pool
     * and that each sub-pool contains sufficient data for resampling.
     *
     * @param crossPairedPool the cross-paired pool
     * @param originalPool the original pool
     * @throws InsufficientDataForResamplingException if there was insufficient data for resampling
     */

    private void validateCrossPairedData( Pool<TimeSeries<T>> crossPairedPool,
                                          Pool<TimeSeries<T>> originalPool )
    {
        List<Pool<TimeSeries<T>>> miniPoolsCross = crossPairedPool.getMiniPools();
        List<Pool<TimeSeries<T>>> miniPoolsOrig = originalPool.getMiniPools();

        String start = "When attempting to resample the pooled data, discovered insufficient overlapping samples "
                       + "for the ";
        String end = " pairs across the sub-pools that compose the overall pool. Resampling requires at least two "
                     + "samples and, practically speaking, many more for reliable uncertainty estimation. Please "
                     + "ensure that there are some common time-series across the datasets that compose each pool or "
                     + "remove the sampling uncertainty assessment. The following sub-pools failed to produce two or "
                     + "more overlapping samples: ";

        boolean mainError = false;
        boolean baselineError = false;

        Set<FeatureGroup> failures = new TreeSet<>();
        for ( int i = 0; i < miniPoolsCross.size(); i++ )
        {
            // Check for consistent empty/full status of datasets following cross-pairing
            if ( this.isNotConsistentlyEmptyOrFull( miniPoolsCross.get( i )
                                                                  .get(), miniPoolsOrig.get( i )
                                                                                       .get() ) )
            {
                mainError = true;
                failures.add( miniPoolsOrig.get( i )
                                           .getMetadata()
                                           .getFeatureGroup() );
            }

            if ( crossPairedPool.hasBaseline()
                 && this.isNotConsistentlyEmptyOrFull( miniPoolsCross.get( i )
                                                                     .getBaselineData()
                                                                     .get(), miniPoolsOrig.get( i )
                                                                                          .getBaselineData()
                                                                                          .get() ) )
            {
                baselineError = true;
                failures.add( miniPoolsOrig.get( i )
                                           .getBaselineData()
                                           .getMetadata()
                                           .getFeatureGroup() );
            }

            // Check for insufficient data - can check the main pairs only due to cross-pairing
            if ( this.hasInsufficientData( miniPoolsCross.get( i )
                                                         .get() ) )
            {
                mainError = true;
                failures.add( miniPoolsOrig.get( i )
                                           .getMetadata()
                                           .getFeatureGroup() );
            }
        }

        if ( mainError && baselineError )
        {
            throw new InsufficientDataForResamplingException( start + "main and baseline" + end + failures );
        }
        else if ( mainError )
        {
            throw new InsufficientDataForResamplingException( start + "main" + end + failures );
        }
        else if ( baselineError )
        {
            throw new InsufficientDataForResamplingException( start + "baseline" + end + failures );
        }
    }

    /**
     * @return whether the empty status of the two time-series collections is consistent
     */

    private boolean isNotConsistentlyEmptyOrFull( List<TimeSeries<T>> first, List<TimeSeries<T>> second )
    {
        return first.isEmpty() != second.isEmpty();
    }

    /**
     * @return whether there is insufficient time-series data for sampling uncertainty estimation
     */

    private boolean hasInsufficientData( List<TimeSeries<T>> data )
    {
        // The time-series are forecasts and there is only one of them
        return data.size() == 1 && !data.get( 0 )
                                        .getMetadata()
                                        .getReferenceTimes()
                                        .isEmpty();
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
     * Creates a binomial distribution with probability of success that is proportional to the time offset between
     * time-series relative to the timestep of the data.
     *
     * @param timestep the timestep
     * @param timeOffsets the time offsets
     * @param meanBlockSizeInTimesteps the mean block size
     * @param random the random number generator
     * @return the distributions for calculating transition probabilities between time-series
     */

    private Map<Duration, BinomialDistribution> getTransitionProbabilitiesBetweenSeries( Duration timestep,
                                                                                         Set<Duration> timeOffsets,
                                                                                         long meanBlockSizeInTimesteps,
                                                                                         RandomGenerator random )
    {
        Map<Duration, BinomialDistribution> returnMe = new HashMap<>();

        if ( !timeOffsets.isEmpty() )
        {
            for ( Duration nextOffset : timeOffsets )
            {
                double qProb = this.getTransitionProbabilityBetweenSeries( timestep,
                                                                           nextOffset,
                                                                           meanBlockSizeInTimesteps );
                BinomialDistribution b = new BinomialDistribution( random, 1, qProb );
                returnMe.put( nextOffset, b );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Calculates a transition probability between series based on the mean block size in timestep units and the number
     * of timesteps per offset between series.
     *
     * @param timestep the timestep
     * @param timeOffset the time offset
     * @param meanBlockSizeInTimesteps the mean block size in timesteps
     * @return the transition probability between series
     */

    private double getTransitionProbabilityBetweenSeries( Duration timestep,
                                                          Duration timeOffset,
                                                          long meanBlockSizeInTimesteps )
    {
        // If the offset is less than the timestep, return the timestep probability
        if ( timeOffset.compareTo( timestep ) < 0 )
        {
            return 1.0 / meanBlockSizeInTimesteps;
        }

        // Find the number of timesteps per offset and use that to calculate a transition probability between series
        // Could perform this calculation with BigDecimal, creating in seconds and then adding the nanosecond part with
        // scale 999_999, but that is probably overkill relative to the big assumptions made with sampling uncertainty
        // estimation, and operations on BigDecimal dramatically increase runtime complexity. Use millisecond precision
        // instead
        double timestepDecimal = timestep.toMillis();
        double offsetDecimal = timeOffset.toMillis();
        double timestepsPerOffset = timestepDecimal / offsetDecimal;
        double meanBlocksPerOffset = timestepsPerOffset * meanBlockSizeInTimesteps;

        // If the number of blocks per offset is less than 1, adjust to 1, i.e., random sampling, because the block
        // size does not capture even 1 offset between time-series
        if ( meanBlocksPerOffset < 1.0 )
        {
            meanBlocksPerOffset = 1.0;
        }

        return 1.0 / meanBlocksPerOffset;
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
