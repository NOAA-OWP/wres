package wres.datamodel.bootstrap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

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
     * events as the corresponding map key. Each inner list contains the events for one time-series.*/
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
        SortedMap<Integer, List<TimeSeries<T>>> bySize = pool.get()
                                                             .stream()
                                                             .collect( Collectors.groupingBy( n -> n.getEvents()
                                                                                                    .size(),
                                                                                              TreeMap::new,
                                                                                              Collectors.toList() ) );

        Map<Integer, List<List<Event<T>>>> innerTimeSeriesEvents = new HashMap<>();

        for ( Map.Entry<Integer, List<TimeSeries<T>>> nextEntry : bySize.entrySet() )
        {
            Integer nextCount = nextEntry.getKey();
            SortedMap<Integer, List<TimeSeries<T>>> submap = bySize.subMap( nextCount, Integer.MAX_VALUE );
            List<List<Event<T>>> events = new ArrayList<>();
            for ( Map.Entry<Integer, List<TimeSeries<T>>> nextInnerEntry : submap.entrySet() )
            {
                List<TimeSeries<T>> nextInner = nextInnerEntry.getValue();
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
