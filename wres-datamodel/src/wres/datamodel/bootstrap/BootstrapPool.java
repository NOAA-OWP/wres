package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * Stores the underlying time-series data within a {@link Pool} in an efficient format for bootstrap resampling.
 * Considers only the pooled data returned by {@link Pool#get()}. Thus, a separate {@link BootstrapPool} is required
 * for each "mini-pool" within an overall pool (e.g., each geographic feature) and for each main and baseline dataset.
 *
 * @author James Brown
 */
class BootstrapPool<T>
{
    /** The time-series mapped by number of events. Each list contains one or more time-series with at least as many
     * events as the corresponding map key. The time-series are time-ordered by the first valid time in the series. In
     * other words, consecutive series are the "nearest" to each other in time. Each inner list contains the events
     * for one time-series, also ordered by valid time.*/
    private final Map<Integer, List<List<Event<T>>>> timeSeriesEvents;

    /** The original pool. */
    private final Pool<TimeSeries<T>> pool;

    /**
     * Creates an instance.
     * @param <T> the type of pool event value
     * @param pool the pool
     * @throws NullPointerException if the pool is null
     * @throws IllegalArgumentException if the pool is unsuitable for bootstrap resampling
     */

    static <T> BootstrapPool<T> of( Pool<TimeSeries<T>> pool )
    {
        return new BootstrapPool<>( pool );
    }

    /**
     * Returns the time-series with at least the number of requested events.
     *
     * @param minimumEventCount the minimum number of events
     * @return the time-series with at least the number if requested events
     */

    List<List<Event<T>>> getTimeSeriesWithAtLeastThisManyEvents( int minimumEventCount )
    {
        // Any request should be made with respect to the structure from which this instance was created, i.e., there
        // must be at least one time-series with the minimumEventCount or more
        if ( !this.timeSeriesEvents.containsKey( minimumEventCount ) )
        {
            throw new IllegalArgumentException( "Could not find any time-series with "
                                                + minimumEventCount
                                                + " or more events." );
        }

        return this.timeSeriesEvents.get( minimumEventCount );
    }

    /**
     * Returns the absolute durations between the first valid times in consecutive time-series, as well as between the
     * first and last time-series.
     * @return the time offsets
     */

    Set<Duration> getValidTimeOffsets()
    {
        Set<Duration> offsets = new HashSet<>();
        for ( List<List<Event<T>>> next : this.timeSeriesEvents.values() )
        {
            if ( next.size() > 1 )
            {
                // Offsets between consecutive series
                for ( int i = 1; i < next.size(); i++ )
                {
                    Instant first = next.get( i - 1 )
                                        .get( 0 )
                                        .getTime();
                    Instant last = next.get( i )
                                       .get( 0 )
                                       .getTime();
                    Duration offset = Duration.between( first, last )
                                              .abs();
                    offsets.add( offset );
                }

                // Offset between the first and last series, which is required because the resampling is circular
                Instant first = next.get( 0 )
                                    .get( 0 )
                                    .getTime();
                Instant last = next.get( next.size() - 1 )
                                   .get( 0 )
                                   .getTime();
                Duration offset = Duration.between( first, last )
                                          .abs();
                offsets.add( offset );
            }
        }

        return Collections.unmodifiableSet( offsets );
    }

    /**
     * @return the original pool
     */
    Pool<TimeSeries<T>> getPool()
    {
        return this.pool;
    }

    /**
     * Creates a bootstrap pool from the input.
     * @param pool the pool
     * @throws NullPointerException if the pool is null
     * @throws IllegalArgumentException if the pool is unsuitable for bootstrap resampling
     */
    private BootstrapPool( Pool<TimeSeries<T>> pool )
    {
        Objects.requireNonNull( pool );

        this.validatePoolStructure( pool );

        // Sort the time-series by number of events
        SortedMap<Integer, List<TimeSeries<T>>> bySize = TimeSeriesSlicer.groupByEventCount( pool.get() );

        Map<Integer, List<List<Event<T>>>> innerTimeSeriesEvents = new HashMap<>();

        // Sorter that sorts consecutive series by their first valid time
        Comparator<TimeSeries<T>> sorter = Comparator.comparing( a -> a.getEvents()
                                                                       .first()
                                                                       .getTime() );

        for ( Map.Entry<Integer, List<TimeSeries<T>>> nextEntry : bySize.entrySet() )
        {
            Integer nextCount = nextEntry.getKey();
            SortedMap<Integer, List<TimeSeries<T>>> submap = bySize.subMap( nextCount, Integer.MAX_VALUE );
            List<List<Event<T>>> events = new ArrayList<>();
            for ( Map.Entry<Integer, List<TimeSeries<T>>> nextInnerEntry : submap.entrySet() )
            {
                List<TimeSeries<T>> nextInner = nextInnerEntry.getValue();

                // Sort by the first valid time in each series
                nextInner.sort( sorter );

                List<List<Event<T>>> unwrapped = nextInner.stream()
                                                          .map( n -> List.copyOf( n.getEvents() ) )
                                                          .toList();
                events.addAll( unwrapped );
            }

            innerTimeSeriesEvents.put( nextCount, Collections.unmodifiableList( events ) );
        }

        this.timeSeriesEvents = Collections.unmodifiableMap( innerTimeSeriesEvents );

        this.pool = pool;
    }

    /**
     * Validates the pool structure.
     * @param pool the pool to validate
     * @throws IllegalArgumentException if the pool structure is invalid for resampling
     */

    private void validatePoolStructure( Pool<TimeSeries<T>> pool )
    {
        if ( pool.get()
                 .isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot resample an empty pool." );
        }

        // Check for consistency of the main and baseline pairs, where applicable
        if ( pool.hasBaseline() )
        {
            List<TimeSeries<T>> main = pool.get();
            List<TimeSeries<T>> baseline = pool.getBaselineData()
                                               .get();

            // The main and baseline pools have the same number of time-series
            if ( main.size() != baseline.size() )
            {
                throw new IllegalArgumentException( "Cannot resample a pool with a different number of time-series "
                                                    + "in the main and baseline pools. The main pool contains "
                                                    + main.size()
                                                    + " time-series, whereas the baseline pool contains "
                                                    + baseline.size()
                                                    + " time-series. Consider using cross-pairing to render the main "
                                                    + "and baseline pools structurally identical, which will allow "
                                                    + "for an assessment of the sampling uncertainties through "
                                                    + "resampling. The pool "
                                                    + "metadata is: "
                                                    + pool.getMetadata() );
            }

            // Each main time-series has the same number of pairs as its corresponding baseline
            for ( int i = 0; i < main.size(); i++ )
            {
                if ( main.get( i )
                         .getEvents()
                         .size() != baseline.get( i )
                                            .getEvents()
                                            .size() )
                {
                    throw new IllegalArgumentException( "Cannot resample a pool whose corresponding main and baseline "
                                                        + "time-series have a different number of events. Consider "
                                                        + "using cross-pairing to render the main and baseline pools "
                                                        + "structurally identical, which will allow for an assessment "
                                                        + "of the sampling uncertainties through "
                                                        + "resampling. The pool "
                                                        + "metadata is: "
                                                        + pool.getMetadata() );
                }
            }
        }
    }
}
