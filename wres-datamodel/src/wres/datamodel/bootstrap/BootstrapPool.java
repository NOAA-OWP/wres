package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.statistics.generated.ReferenceTime;

/**
 * Stores the underlying time-series data within a {@link Pool} in an efficient format for bootstrap resampling.
 * Considers only the pooled data returned by {@link Pool#get()}. Thus, a separate {@link BootstrapPool} is required
 * for each "mini-pool" within an overall pool (e.g., each geographic feature) and for each main and baseline dataset.
 *
 * @author James Brown
 */
class BootstrapPool<T>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BootstrapPool.class );

    /** The time-series mapped by number of events. Each list contains one or more time-series with at least as many
     * events as the corresponding map key. The time-series are time-ordered by the first valid time in the series. In
     * other words, consecutive series are the "nearest" to each other in time. Each inner list contains the events
     * for one time-series, also ordered by valid time.*/
    private final SortedMap<Integer, List<List<Event<T>>>> timeSeriesEvents;

    /** The original pool. */
    private final Pool<TimeSeries<T>> pool;

    /** The time-series in the original pool, sorted for indexing and resampling. There is one list for each collection
     * of time-series with the same number of events, ordered from the smallest to the largest number of events. Within
     * each list, the time-series are ordered by the first valid time in the time-series, again in ascending order. */

    private final List<List<TimeSeries<T>>> ordered;

    /** Whether the pool contains forecasts. */
    private final boolean hasForecasts;

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
     * Returns the time-series with all events present
     *
     * @return the time-series with at least the number if requested events
     */

    List<List<Event<T>>> getTimeSeriesWithAllEvents()
    {
        return this.timeSeriesEvents.get( this.timeSeriesEvents.firstKey() );
    }

    /**
     * Returns the time-series from the original pool in a consistent order for index generation and resampling. There
     * is one inner list for each collection of time-series with a fixed number of events, arranged in ascending order.
     * Each inner list contains the time-series with the prescribed number of events in ascending order of the valid
     * time of the first event in the time-series.
     *
     * @return the ordered time-series
     */

    List<List<TimeSeries<T>>> getOrderedTimeSeries()
    {
        return this.ordered;
    }

    /**
     * Returns the absolute durations between the first valid times in consecutive time-series, as well as between the
     * first and last time-series.
     * @return the time offsets
     */

    Set<Duration> getValidTimeOffsets()
    {
        Set<Duration> offsets = new HashSet<>();

        // Are the time-series forecasts? If so, the gap is based on the first valid time in adjacent series, else the
        // first and last valid times
        for ( List<List<Event<T>>> next : this.timeSeriesEvents.values() )
        {
            if ( next.size() > 1 )
            {
                Set<Duration> nextOffsets = this.getOffsetsBetweenConsecutiveSeries( next );
                offsets.addAll( nextOffsets );
            }
        }

        return Collections.unmodifiableSet( offsets );
    }

    /**
     * Adds the time offset between adjacent time-series in the inputs.
     * @param series the series
     */
    private Set<Duration> getOffsetsBetweenConsecutiveSeries( List<List<Event<T>>> series )
    {
        Set<Duration> offsets = new HashSet<>();

        // Offsets between consecutive series
        for ( int i = 1; i < series.size(); i++ )
        {
            Instant first;
            List<Event<T>> firstSeries = series.get( i - 1 );

            if ( !firstSeries.isEmpty() )
            {
                if ( this.hasForecasts() )
                {
                    first = firstSeries.get( 0 )
                                       .getTime();
                }
                else
                {
                    first = firstSeries.get( firstSeries.size() - 1 )
                                       .getTime();
                }

                Instant last = series.get( i )
                                     .get( 0 )
                                     .getTime();
                Duration offset = Duration.between( first, last )
                                          .abs();
                offsets.add( offset );
            }
        }

        // Offset between the first and last series, which is required because the resampling is circular
        Duration offset = this.getOffsetBetweenFirstAndLastSeries( series );
        if ( Objects.nonNull( offset ) )
        {
            offsets.add( offset );
        }

        return Collections.unmodifiableSet( offsets );
    }

    /**
     * Gets the time offset between the first and last series.
     * @param series the series
     */
    private Duration getOffsetBetweenFirstAndLastSeries( List<List<Event<T>>> series )
    {
        // Offset between the first and last series, which is required because the resampling is circular
        List<Event<T>> firstSeries = series.get( 0 );
        List<Event<T>> lastSeries = series.get( series.size() - 1 );

        Duration offset = null;

        if ( !firstSeries.isEmpty()
             && !lastSeries.isEmpty() )
        {
            Instant first = firstSeries
                    .get( 0 )
                    .getTime();

            Instant last;

            if ( this.hasForecasts() )
            {
                last = lastSeries.get( 0 )
                                 .getTime();
            }
            else
            {
                last = lastSeries.get( lastSeries.size() - 1 )
                                 .getTime();
            }

            offset = Duration.between( first, last )
                             .abs();
        }

        return offset;
    }

    /**
     * @return the original pool
     */
    Pool<TimeSeries<T>> getPool()
    {
        return this.pool;
    }

    /**
     * @return whether the pool contains forecast time-series
     */

    boolean hasForecasts()
    {
        return this.hasForecasts;
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

        // Sort the time-series by number of events and then by valid time
        // Sorter that sorts consecutive series by their first valid time
        Comparator<TimeSeries<T>> sorter = Comparator.comparing( a -> a.getEvents()
                                                                       .first()
                                                                       .getTime() );
        SortedMap<Integer, List<TimeSeries<T>>> bySize = TimeSeriesSlicer.groupByEventCount( pool.get() );
        List<List<TimeSeries<T>>> groupedBySize = new ArrayList<>();
        for ( Map.Entry<Integer, List<TimeSeries<T>>> nextEntry : bySize.entrySet() )
        {
            int count = nextEntry.getKey();
            List<TimeSeries<T>> nextList = nextEntry.getValue();
            if ( count > 0 )
            {
                nextList.sort( sorter );
            }
            groupedBySize.add( nextList );
        }

        this.ordered = Collections.unmodifiableList( groupedBySize );

        SortedMap<Integer, List<List<Event<T>>>> innerTimeSeriesEvents = new TreeMap<>();

        for ( Map.Entry<Integer, List<TimeSeries<T>>> nextEntry : bySize.entrySet() )
        {
            Integer nextCount = nextEntry.getKey();
            SortedMap<Integer, List<TimeSeries<T>>> submap = bySize.subMap( nextCount, Integer.MAX_VALUE );
            List<List<Event<T>>> events = new ArrayList<>();
            for ( Map.Entry<Integer, List<TimeSeries<T>>> nextInnerEntry : submap.entrySet() )
            {
                List<TimeSeries<T>> nextInner = nextInnerEntry.getValue();

                // Sort by the first valid time in each series
                if ( nextCount > 0 )
                {
                    nextInner.sort( sorter );
                }

                List<List<Event<T>>> unwrapped = nextInner.stream()
                                                          .map( n -> List.copyOf( n.getEvents() ) )
                                                          .toList();
                events.addAll( unwrapped );
            }

            innerTimeSeriesEvents.put( nextCount, Collections.unmodifiableList( events ) );
        }

        // Log the structure
        if ( LOGGER.isDebugEnabled() )
        {
            SortedMap<Integer, Integer> sortedSeries =
                    innerTimeSeriesEvents.entrySet()
                                         .stream()
                                         .collect( Collectors.toMap( Map.Entry::getKey, n -> n.getValue()
                                                                                              .size(),
                                                                     ( a, b ) -> a,
                                                                     TreeMap::new ) );

            LOGGER.debug( "Created a bootstrap pool with the following time-series structure to sample (event count="
                          + "number of time-series with at least that event count): {}.", sortedSeries );
        }

        this.timeSeriesEvents = Collections.unmodifiableSortedMap( innerTimeSeriesEvents );
        this.pool = pool;
        this.hasForecasts = this.pool.get()
                                     .stream()
                                     .anyMatch( n -> n.getReferenceTimes()
                                                      .containsKey( ReferenceTime.ReferenceTimeType.T0 ) );
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
