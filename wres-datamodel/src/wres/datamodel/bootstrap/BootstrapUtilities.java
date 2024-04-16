package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;

/**
 * Utilities to assist with bootstrap resampling of data pools.
 *
 * @author James Brown
 */
public class BootstrapUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BootstrapUtilities.class );

    /**
     * Estimates the optimal block size for each left-ish time-series in each mini-pool and returns the average of the
     * optimal block sizes across all time-series.
     * @param <R> the type of right-ish time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap in timestep units, together with the timestep
     */
    public static <R> Pair<Long, Duration> getOptimalBlockSizeForStationaryBootstrap( Pool<TimeSeries<Pair<Double, R>>> pool )
    {
        List<Pool<TimeSeries<Pair<Double, R>>>> miniPools = pool.getMiniPools();

        List<Pair<Long, Duration>> blockSizes = new ArrayList<>();
        for ( Pool<TimeSeries<Pair<Double, R>>> next : miniPools )
        {
            // Main data with sufficient samples
            if ( BootstrapUtilities.hasSufficientDataForStationaryBootstrap( next.get() ) )
            {
                Pair<Long, Duration> nextMain =
                        getOptimalBlockSizeForStationaryBootstrap( next.get() );
                blockSizes.add( nextMain );
            }

            // Baseline data with sufficient samples
            if ( next.hasBaseline()
                 && BootstrapUtilities.hasSufficientDataForStationaryBootstrap( next.getBaselineData()
                                                                                    .get() ) )
            {
                List<TimeSeries<Pair<Double, R>>> baseline = next.getBaselineData()
                                                                 .get();
                Pair<Long, Duration> nextBaseline =
                        getOptimalBlockSizeForStationaryBootstrap( baseline );
                blockSizes.add( nextBaseline );
            }
        }

        if ( blockSizes.isEmpty() )
        {
            throw new InsufficientDataForResamplingException( "Insufficient data to "
                                                              + "calculate the optimal "
                                                              + "block size for the "
                                                              + "stationary bootstrap. "
                                                              + "The pool metadata was: "
                                                              + pool.getMetadata() );
        }

        double total = 0;
        Duration totalDuration = Duration.ZERO;
        for ( Pair<Long, Duration> next : blockSizes )
        {
            total += next.getLeft();
            totalDuration = totalDuration.plus( next.getValue() );
        }

        long optimalBlockSize = ( long ) Math.ceil( total / blockSizes.size() );
        Duration averageDuration = totalDuration.dividedBy( blockSizes.size() );

        LOGGER.debug( "Determined an optimal block size of {} timesteps of {} for applying the stationary bootstrap to "
                      + "the pool with metadata: {}. This is an average of the optimal block sizes across all observed "
                      + "time-series within the pool, which included the following block sizes: {}.",
                      optimalBlockSize,
                      averageDuration,
                      pool.getMetadata(),
                      blockSizes );

        return Pair.of( optimalBlockSize, averageDuration );
    }

    /**
     * Determines whether sufficient data is available for bootstrap resampling. There must be more than one event
     * across time time-series present.
     * @param <T> the type of time-series data
     * @param data the time-series data
     * @return whether there is sufficient data for the stationary bootstrap
     * @throws NullPointerException if the input is null
     */
    public static <T> boolean hasSufficientDataForStationaryBootstrap( List<TimeSeries<T>> data )
    {
        Objects.requireNonNull( data );

        long eventCount = data.stream()
                              .mapToLong( t -> t.getEvents()
                                                .size() )
                              .sum();

        if ( LOGGER.isDebugEnabled() )
        {
            Set<TimeSeriesMetadata> metadatas = data.stream()
                                                    .map( TimeSeries::getMetadata )
                                                    .collect( Collectors.toSet() );

            LOGGER.debug( "Discovered {} events on which to perform the stationary bootstrap. The time-series examined "
                          + "had the following metadata: {}. ", eventCount, metadatas );
        }

        return eventCount > 1;
    }

    /**
     * Estimates the optimal block size for the consolidated left-ish time-series in the input, together with the modal
     * timestep of the supplied time-series.
     * @param <T> the type of time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     */
    private static <T> Pair<Long, Duration> getOptimalBlockSizeForStationaryBootstrap( List<TimeSeries<Pair<Double, T>>> pool )
    {
        SortedMap<Instant, Double> consolidated = pool.stream()
                                                      .flatMap( n -> n.getEvents()
                                                                      .stream() )
                                                      .collect( Collectors.toMap( Event::getTime, e -> e.getValue()
                                                                                                        .getLeft(),
                                                                                  ( a, b ) -> a, TreeMap::new ) );

        double[] data = new double[consolidated.size()];
        List<Duration> durations = new ArrayList<>();
        Instant last = null;
        int index = 0;
        for ( Map.Entry<Instant, Double> next : consolidated.entrySet() )
        {
            data[index] = next.getValue();
            Instant time = next.getKey();

            if ( index > 0 )
            {
                Duration between = Duration.between( last, time );
                durations.add( between );
            }

            last = time;
            index++;
        }

        long optimalBlockSize = BlockSizeEstimator.getOptimalBlockSize( data );

        // Find the corresponding timestep, which is the modal timestep
        Set<TimeSeriesMetadata> metadatas = pool.stream()
                                                .map( TimeSeries::getMetadata )
                                                .collect( Collectors.toSet() );
        Duration modalTimestep =
                durations.stream()
                         .collect( Collectors.groupingBy( Function.identity(),
                                                          Collectors.counting() ) )
                         .entrySet()
                         .stream()
                         .max( Map.Entry.comparingByValue() )
                         .map( Map.Entry::getKey )
                         .orElseThrow( () -> new InsufficientDataForResamplingException( "Insufficient data to "
                                                                                         + "calculate the optimal "
                                                                                         + "block size for the "
                                                                                         + "stationary bootstrap. "
                                                                                         + "The time-series examined "
                                                                                         + "had the following metadata: "
                                                                                         + metadatas ) );

        return Pair.of( optimalBlockSize, modalTimestep );
    }

    /**
     * Do not construct.
     */
    private BootstrapUtilities()
    {
    }
}
