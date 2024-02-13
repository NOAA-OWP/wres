package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import wres.datamodel.Climatology;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.pools.Pool;
import wres.datamodel.Slicer;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for slicing/dicing and transforming time-series.
 *
 * @author James Brown
 * @see     Slicer
 * @see     PoolSlicer
 */

public final class TimeSeriesSlicer
{
    /** The default unit for lead durations. */
    public static final ChronoUnit LEAD_RESOLUTION = ChronoUnit.MINUTES;

    /** Re-used log message string. */
    private static final String WHILE_ATTEMPTING_TO_FIND_THE_INTERSECTING_TIMES_BETWEEN_THE_LEFT_SERIES_AND_THE_RIGHT =
            "While attempting to find the intersecting times between the left series {} and the right ";

    /** Null input error message. */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesSlicer.class );

    /** UTC. */
    private static final ZoneId ZONE_ID = ZoneId.of( "UTC" );

    /**
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series.
     *
     * @param <L> the type of left paired value
     * @param <R> the type of right paired value
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <L, R> Predicate<TimeSeries<Pair<L, R>>> anyOfLeftInTimeSeries( Predicate<L> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of left." );

        return times -> {

            // Iterate the times
            for ( Event<Pair<L, R>> next : times.getEvents() )
            {
                // Condition is met for one time
                if ( predicate.test( next.getValue().getLeft() ) )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * <p>Composes the input predicate as applying to the right side of any paired value within a time-series.
     *
     * @param <L> the type of left paired value
     * @param <R> the type of right paired value
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <L, R> Predicate<TimeSeries<Pair<L, R>>> anyOfRightInTimeSeries( Predicate<R> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of right." );

        return times -> {

            // Iterate the times
            for ( Event<Pair<L, R>> next : times.getEvents() )
            {
                // Condition is met for one time
                if ( predicate.test( next.getValue().getRight() ) )
                {
                    return true;
                }
            }

            return false;
        };
    }


    /**
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series and,
     * separately, to the right side of any paired value within that time-series.
     *
     * @param <S> the type of left and right paired values
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <S> Predicate<TimeSeries<Pair<S, S>>> anyOfLeftAndAnyOfRightInTimeSeries( Predicate<S> predicate )
    {
        Objects.requireNonNull( predicate,
                                "Specify non-null input when slicing a time-series by any of left"
                                + "and any of right." );

        return times -> {

            // Iterate the times
            boolean left = false;
            boolean right = false;

            for ( Event<Pair<S, S>> next : times.getEvents() )
            {
                // Condition is met on either side at any point within the series
                if ( predicate.test( next.getValue().getLeft() ) )
                {
                    left = true;
                }

                if ( predicate.test( next.getValue().getRight() ) )
                {
                    right = true;
                }

                if ( left && right )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * Returns a filtered view of a time-series based on the input predicate.
     *
     * @see #filterByEvent(TimeSeries, Predicate)
     * @param <T> the type of time-series value
     * @param timeSeries the time-series
     * @param filter the filter
     * @return a filtered view of the input series
     * @throws NullPointerException if any input is null
     */

    public static <T> TimeSeries<T> filter( TimeSeries<T> timeSeries, Predicate<T> filter )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( filter );

        Predicate<Event<T>> byEvent = e -> filter.test( e.getValue() );

        return TimeSeriesSlicer.filterByEvent( timeSeries, byEvent );
    }

    /**
     * Returns a filtered view of a time-series based on the input predicate.
     *
     * @see #filter(TimeSeries, Predicate)
     * @param <T> the type of time-series value
     * @param timeSeries the time-series
     * @param filter the filter
     * @return a filtered view of the input series
     * @throws NullPointerException if any input is null
     */

    public static <T> TimeSeries<T> filterByEvent( TimeSeries<T> timeSeries, Predicate<Event<T>> filter )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( filter );

        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();

        builder.setMetadata( timeSeries.getMetadata() );

        timeSeries.getEvents()
                  .stream()
                  .filter( filter )
                  .forEach( builder::addEvent );

        return builder.build();
    }

    /**
     * Creates a filter that indicates whether any one reference time associated with the time-series falls within the
     * declared season.
     *
     * @see #getValidTimeSeasonTransformer(MonthDay, MonthDay)
     * @param <T> the time-series event value type
     * @param start the start monthday
     * @param end the end monthday
     * @return the filter
     * @throws NullPointerException if either input is null
     */

    public static <T> Predicate<TimeSeries<T>> getReferenceTimeSeasonFilter( MonthDay start, MonthDay end )
    {
        Objects.requireNonNull( start );
        Objects.requireNonNull( end );

        return timeSeries -> {

            // Seasons currently only apply to reference times
            if ( timeSeries.getReferenceTimes()
                           .isEmpty() )
            {
                return true;
            }

            // Get the interval to check
            Function<Instant, Pair<Instant, Instant>> intervalCalculator =
                    TimeSeriesSlicer.getIntervalFromMonthDays( start, end );

            for ( Instant next : timeSeries.getReferenceTimes()
                                           .values() )
            {
                Pair<Instant, Instant> nextInterval = intervalCalculator.apply( next );

                Instant earliest = nextInterval.getLeft();
                Instant latest = nextInterval.getRight();

                // Is the reference time within the interval?
                if ( next.isAfter( earliest )
                     && ( next.isBefore( latest )
                          || next.equals( latest ) ) )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * Creates a transformer that removes valid times that are outside the prescribed season.
     *
     * @see #getReferenceTimeSeasonFilter(MonthDay, MonthDay)
     * @param <T> the time-series event value type
     * @param start the start monthday
     * @param end the end monthday
     * @return the transformer
     * @throws NullPointerException if either input is null
     */

    public static <T> UnaryOperator<TimeSeries<T>> getValidTimeSeasonTransformer( MonthDay start, MonthDay end )
    {
        Objects.requireNonNull( start );
        Objects.requireNonNull( end );

        Function<Instant, Pair<Instant, Instant>> intervalCalculator =
                TimeSeriesSlicer.getIntervalFromMonthDays( start, end );

        Predicate<Event<T>> byEvent = e ->
        {
            Instant nextTime = e.getTime();
            Pair<Instant, Instant> nextInterval = intervalCalculator.apply( nextTime );

            Instant earliest = nextInterval.getLeft();
            Instant latest = nextInterval.getRight();

            // Is the valid time within the interval?
            return nextTime.isAfter( earliest )
                   && ( nextTime.isBefore( latest )
                        || nextTime.equals( latest ) );
        };

        return t -> TimeSeriesSlicer.filterByEvent( t, byEvent );
    }

    /**
     * Returns a filtered {@link TimeSeries} whose events are within the right-closed time intervals contained in the 
     * prescribed {@link TimeWindowOuter}.
     *
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param timeWindow the time window on which to slice
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if any input is null
     */

    public static <T> TimeSeries<T> filter( TimeSeries<T> input,
                                            TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( input );

        return TimeSeriesSlicer.filter( input, timeWindow, input.getReferenceTimes().keySet() );
    }

    /**
     * Returns a filtered {@link TimeSeries} whose events are within the right-closed time intervals contained in the 
     * prescribed {@link TimeWindowOuter}. When considering lead durations, the filter may focus on all 
     * {@link ReferenceTimeType} or a prescribed subset.
     *
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param timeWindow the time window on which to slice
     * @param referenceTimeTypes the reference time types to consider when filtering lead durations
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if any input is null
     */

    public static <T> TimeSeries<T> filter( TimeSeries<T> input,
                                            TimeWindowOuter timeWindow,
                                            Set<ReferenceTimeType> referenceTimeTypes )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( timeWindow );

        Objects.requireNonNull( referenceTimeTypes );

        // Find the subset of reference times to consider
        Map<ReferenceTimeType, Instant> subset = input.getReferenceTimes()
                                                      .entrySet()
                                                      .stream()
                                                      .filter( a -> referenceTimeTypes.contains( a.getKey() ) )
                                                      .collect( Collectors.toMap( Entry::getKey, Entry::getValue ) );

        // Filter the subset of reference times according to the pool boundaries
        Map<ReferenceTimeType, Instant> referenceTimes =
                TimeSeriesSlicer.filterReferenceTimes( subset, timeWindow );

        // Find the references times that were not in the subset, plus the ones that were and are within bounds
        Map<ReferenceTimeType, Instant> notConsideredOrWithinBounds = new EnumMap<>( ReferenceTimeType.class );
        notConsideredOrWithinBounds.putAll( input.getReferenceTimes() );
        notConsideredOrWithinBounds.keySet().removeAll( subset.keySet() );
        notConsideredOrWithinBounds.putAll( referenceTimes );

        TimeSeriesMetadata metadata =
                new TimeSeriesMetadata.Builder( input.getMetadata() ).setReferenceTimes( notConsideredOrWithinBounds )
                                                                     .build();

        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        // Some reference times existed and none were within the filter bounds?
        if ( !input.getReferenceTimes().isEmpty() && notConsideredOrWithinBounds.isEmpty() )
        {
            return builder.build();
        }

        // Iterate through the events and include events within the window
        for ( Event<T> nextEvent : input.getEvents() )
        {
            Instant nextValidTime = nextEvent.getTime();
            boolean isContained = TimeSeriesSlicer.isContained( nextValidTime,
                                                                timeWindow.getEarliestValidTime(),
                                                                timeWindow.getLatestValidTime() );
            // Within valid time bounds
            if ( isContained )
            {
                // Add an event if the lead duration with respect to all reference times
                // falls within the time window or there are no reference times
                for ( Instant nextReference : referenceTimes.values() )
                {
                    Duration leadDuration = Duration.between( nextReference, nextValidTime );

                    // Inside the right-closed period?
                    if ( !TimeSeriesSlicer.isContained( leadDuration,
                                                        timeWindow.getEarliestLeadDuration(),
                                                        timeWindow.getLatestLeadDuration() ) )
                    {
                        isContained = false;
                        break;
                    }
                }
            }

            // Contained?
            if ( isContained )
            {
                builder.addEvent( nextEvent );
            }
        }

        return builder.build();
    }

    /**
     * Groups the input events according to the event valid time. An event falls within a group if its valid time falls 
     * within an interval that ends at a prescribed time and begins a specified period before that time. The interval 
     * is right-closed. In other words, when an event time falls within <code>(endsAt-period,endsAt]</code>, then add
     * that event to the group associated with <code>endsAt</code>. Use this method to determine groups of events for
     * upscaling values that end at <code>endsAt</code>.
     *
     * @param <T> the type of event value
     * @param events the events to group
     * @param endsAt the end of each group, inclusive
     * @param period the period before each endsAt at which a group begins, exclusive
     * @return the grouped events
     * @throws NullPointerException if any input is null
     */


    public static <T> Map<Instant, SortedSet<Event<T>>> groupEventsByInterval( SortedSet<Event<T>> events,
                                                                               SortedSet<Instant> endsAt,
                                                                               Duration period )
    {
        Objects.requireNonNull( events, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( endsAt, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( period, NULL_INPUT_EXCEPTION );

        // Calculate the intervals
        SortedSet<Pair<Instant, Instant>> intervals = endsAt.stream()
                                                            .map( next -> Pair.of( next.minus( period ), next ) )
                                                            .collect( Collectors.toCollection( TreeSet::new ) );

        return TimeSeriesSlicer.groupEventsByInterval( events, Collections.unmodifiableSortedSet( intervals ) );
    }

    /**
     * <p>Groups the input events according to the event valid time. An event falls within a group if its valid time
     * falls within the corresponding group interval. Each interval is right-closed.
     *
     * <p>TODO: this method has a nested loop, which implies O(n^2) complexity, where <code>n</code> is the cardinality
     * of <code>endsAt</code>. While it does exploit time-ordering to avoid searching the entire set of
     * <code>endsAt</code>, it does not scale well for very large datasets.
     *
     * @param <T> the type of event value
     * @param events the events to group
     * @param intervals the intervals within which to group events
     * @return the grouped events
     * @throws NullPointerException if any input is null
     */


    public static <T> Map<Instant, SortedSet<Event<T>>> groupEventsByInterval( SortedSet<Event<T>> events,
                                                                               SortedSet<Pair<Instant, Instant>> intervals )
    {
        Objects.requireNonNull( events, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( intervals, NULL_INPUT_EXCEPTION );

        Map<Instant, SortedSet<Event<T>>> grouped = new HashMap<>();

        // Events in a sorted list
        List<Event<T>> listedEvents = new ArrayList<>( events );
        int eventCount = listedEvents.size();

        // Iterate the end times and group events whose times fall in (nextEnd-period,nextEnd]
        int startIndex = 0; // Position at which to start searching the listed events
        for ( Pair<Instant, Instant> nextInterval : intervals )
        {
            // Lower bound exclusive
            Instant nextStart = nextInterval.getLeft();
            Instant nextEnd = nextInterval.getRight();

            // Is event time within (start,nextEnd]?
            SortedSet<Event<T>> nextGroup = grouped.get( nextEnd );

            for ( int i = startIndex; i < eventCount; i++ )
            {
                Event<T> nextEvent = listedEvents.get( i );
                Instant eventTime = nextEvent.getTime();

                if ( eventTime.compareTo( nextEnd ) <= 0 && eventTime.compareTo( nextStart ) > 0 )
                {
                    // Create a new group
                    if ( Objects.isNull( nextGroup ) )
                    {
                        nextGroup = new TreeSet<>();
                        grouped.put( nextEnd, nextGroup );
                    }

                    nextGroup.add( nextEvent );
                }

                // Events are sorted, so stop looking and reset the start
                // position to test containment in the next interval
                // If the interval is overlapping, then count back to the first date that fits within 
                // the overlapping interval and start there.
                if ( eventTime.isAfter( nextEnd ) )
                {
                    startIndex = TimeSeriesSlicer.getIndexOfEarliestTimeThatIsGreaterThanInputTime( i,
                                                                                                    nextStart,
                                                                                                    listedEvents );

                    break;
                }
            }
        }

        return Collections.unmodifiableMap( grouped );
    }

    /**
     * Groups the time-series by number of events.
     *
     * @param <T> the type of time-series event value
     * @param series the time-series to group by event count
     * @return the time-series grouped by event count
     * @throws NullPointerException if the input is null
     */

    public static <T> SortedMap<Integer, List<TimeSeries<T>>> groupByEventCount( Collection<TimeSeries<T>> series )
    {
        Objects.requireNonNull( series );

        SortedMap<Integer, List<TimeSeries<T>>> grouped =
                series.stream()
                      .collect( Collectors.groupingBy( n -> n.getEvents()
                                                             .size(),
                                                       TreeMap::new,
                                                       Collectors.toList() ) );
        return Collections.unmodifiableSortedMap( grouped );
    }

    /**
     * Creates as many intervals as years within the supplied time series using the time scale, which must have one or
     * both month-day bookends defined. Each interval is right-closed.
     *
     * @param <T> the type of time series event value
     * @param desiredTimeScale the desired time scale
     * @param timeSeries the time series
     * @return the intervals
     * @throws NullPointerException if any input is null
     */

    public static <T> SortedSet<Pair<Instant, Instant>>
    getIntervalsFromTimeScaleWithMonthDays( TimeScaleOuter desiredTimeScale,
                                            TimeSeries<T> timeSeries )
    {
        Objects.requireNonNull( desiredTimeScale );
        Objects.requireNonNull( timeSeries );

        MonthDay startMonthDay = desiredTimeScale.getStartMonthDay();
        MonthDay endMonthDay = desiredTimeScale.getEndMonthDay();

        // At least one must be present. If only one is present, the period is guaranteed present
        if ( Objects.isNull( startMonthDay ) && Objects.isNull( endMonthDay ) )
        {
            throw new IllegalArgumentException( "Cannot extract intervals from a desired time scale that has no "
                                                + "bookends present." );
        }

        Function<Instant, Pair<Instant, Instant>> innerMapper =
                TimeSeriesSlicer.getIntervalFromMonthDays( startMonthDay, endMonthDay );

        // Create a mapper function that maps from an event to an interval
        Function<Event<T>, Pair<Instant, Instant>> mapper = event -> {

            Pair<Instant, Instant> interval = innerMapper.apply( event.getTime() );
            Instant start = interval.getLeft();
            Instant end = interval.getRight();

            // One bookend plus the period must be present, see above
            if ( Objects.isNull( start ) )
            {
                start = end.minus( desiredTimeScale.getPeriod() );
            }

            if ( Objects.isNull( end ) )
            {
                end = start.plus( desiredTimeScale.getPeriod() );
            }

            return Pair.of( start, end );
        };

        SortedSet<Pair<Instant, Instant>> intervals = timeSeries.getEvents()
                                                                .stream()
                                                                .map( mapper )
                                                                .collect( Collectors.toCollection( TreeSet::new ) );

        return Collections.unmodifiableSortedSet( intervals );
    }

    /**
     * Returns a function that calculates the interval that corresponds to the pair of month-day bookends when supplied
     * with a given datetime. This interval can be used to determine whether a given datetime is contained in the 
     * interval.
     *
     * @param startMonthDay the start month-day, possibly null
     * @param endMonthDay the end month-day, possibly null
     * @return the function
     */

    public static Function<Instant, Pair<Instant, Instant>> getIntervalFromMonthDays( MonthDay startMonthDay,
                                                                                      MonthDay endMonthDay )
    {
        return event -> {

            Objects.requireNonNull( event, "Cannot calculate an interval from a null event." );

            // Extract the year from the event
            ZonedDateTime zonedEvent = event.atZone( ZONE_ID );
            int year = zonedEvent.getYear();
            MonthDay eventMonthDay = MonthDay.of( zonedEvent.getMonth(), zonedEvent.getDayOfMonth() );

            // Start and end year default to current year
            int startYear = year;
            int endYear = year;

            // Does the interval span a year end?
            if ( Objects.nonNull( startMonthDay ) && Objects.nonNull( endMonthDay )
                 && startMonthDay.isAfter( endMonthDay ) )
            {
                // Is the event month-day after the lower bookend of the interval? If yes, use the current year as the
                // lower bound
                if ( eventMonthDay.compareTo( startMonthDay ) >= 0 )
                {
                    endYear = year + 1;
                }
                // No, then use the current year as the upper bound
                else
                {
                    startYear = year - 1;
                }
            }

            Instant start = null;
            Instant end = null;

            if ( Objects.nonNull( startMonthDay ) )
            {
                LocalDate startLocal = startMonthDay.atYear( startYear );

                // Start of day should be inclusive, so move back one instant
                start = startLocal.atStartOfDay( ZONE_ID )
                                  .minusNanos( 1 )
                                  .toInstant();
            }

            if ( Objects.nonNull( endMonthDay ) )
            {
                LocalDate endLocal = endMonthDay.atYear( endYear );

                // End of day, which occurs one instant before the start of the next day
                end = endLocal.atStartOfDay( ZONE_ID )
                              .plusDays( 1 )
                              .minusNanos( 1 )
                              .toInstant();
            }

            return Pair.of( start, end );
        };
    }

    /**
     * Helper that returns the times associated with values in the input series.
     *
     * @param <T> the type of time series event values
     * @param timeSeries the time-series whose valid times should be determined
     * @return the valid times
     * @throws NullPointerException if the input is null
     */

    public static <T> SortedSet<Instant> getValidTimes( TimeSeries<T> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        SortedSet<Instant> endsAt =
                timeSeries.getEvents()
                          .stream()
                          .map( Event::getTime )
                          .collect( Collectors.toCollection( TreeSet::new ) );

        return Collections.unmodifiableSortedSet( endsAt );
    }

    /**
     * Helper that returns the unique timesteps between valid times in the input.
     *
     * @param <T> the time-series event value type
     * @param timeSeries the time-series
     * @return the unique timesteps
     * @throws NullPointerException if the input is null
     */

    public static <T> SortedSet<Duration> getTimesteps( TimeSeries<T> timeSeries )
    {
        SortedSet<Instant> validTimes = TimeSeriesSlicer.getValidTimes( timeSeries );
        return TimeSeriesSlicer.getTimesteps( validTimes );
    }

    /**
     * <p>Returns a regular sequence of times that are present in both time series and are consistent with the desired 
     * timescale and begin within the designated time window. If the desired timescale contains month-day bookends,
     * returns the empty set.
     *
     * <p>When both time series require upscaling, this method makes a best attempt to retain those times from the 
     * superset of possible valid times that follow a prescribed frequency. Consequently, this method effectively 
     * "thins out" the superset of all possible times in order to provide a subset of regularly spaced times. In 
     * general, there is no unique subset of times that follows a prescribed frequency unless a starting position is 
     * defined. Here, counting occurs with respect to a reference time, when available (i.e., the reference time helps 
     * to select a subset). 
     *
     * <p>See #47158-24.
     *
     * <p>This is to re-assert my opinion, stated in #47158, that choosing a subset of possible times is somewhat 
     * arbitrary. This method is an inevitable source of brittleness or surprise.
     *
     * @param <L> the type of left value
     * @param <R> the type of right value
     * @param left the left-ish time-series
     * @param right the right-ish time series
     * @param timeWindow the time window
     * @param desiredTimeScale the optional desired timescale
     * @param frequency the optional frequency at which values should be sampled, defaults to the time scale period
     * @return a regular sequence of intersecting times
     * @throws NullPointerException if any input is null
     */

    public static <L, R> SortedSet<Instant> getRegularSequenceOfIntersectingTimes( TimeSeries<L> left,
                                                                                   TimeSeries<R> right,
                                                                                   TimeWindowOuter timeWindow,
                                                                                   TimeScaleOuter desiredTimeScale,
                                                                                   Duration frequency )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );
        Objects.requireNonNull( timeWindow );

        if ( Objects.nonNull( desiredTimeScale ) && desiredTimeScale.hasMonthDays() )
        {
            LOGGER.debug( WHILE_ATTEMPTING_TO_FIND_THE_INTERSECTING_TIMES_BETWEEN_THE_LEFT_SERIES_AND_THE_RIGHT
                          + "series {} at the desired time scale of {}, discovered month-day bookends in the desired "
                          + "time scale. Returning the empty set.",
                          left.getMetadata(),
                          right.getMetadata(),
                          desiredTimeScale );

            return Collections.emptySortedSet();
        }

        // Both contain reference times and those times are unequal, return the empty set
        if ( !left.getReferenceTimes().isEmpty() && !right.getReferenceTimes().isEmpty()
             && !left.getReferenceTimes().equals( right.getReferenceTimes() ) )
        {
            LOGGER.debug( WHILE_ATTEMPTING_TO_FIND_THE_INTERSECTING_TIMES_BETWEEN_THE_LEFT_SERIES_AND_THE_RIGHT
                          + "series {} at the desired time scale of {}, discovered unequal reference times. Returning "
                          + "the empty set.",
                          left.getMetadata(),
                          right.getMetadata(),
                          desiredTimeScale );

            return Collections.emptySortedSet();
        }

        // Find the intersecting valid times
        SortedSet<Instant> leftValidTimes = TimeSeriesSlicer.getValidTimes( left );
        SortedSet<Instant> rightValidTimes = TimeSeriesSlicer.getValidTimes( right );
        SortedSet<Instant> intersectingTimes = new TreeSet<>( leftValidTimes );
        intersectingTimes.retainAll( rightValidTimes );

        //Thinning required? If not, then return the intersecting times.
        boolean upscaleLeft = Objects.nonNull( desiredTimeScale ) && !desiredTimeScale.equals( left.getTimeScale() );
        boolean upscaleRight = Objects.nonNull( desiredTimeScale )
                               && !desiredTimeScale.equals( right.getTimeScale() );

        SortedSet<Instant> immutableIntersectingTimes = Collections.unmodifiableSortedSet( intersectingTimes );

        if ( intersectingTimes.isEmpty() || !upscaleLeft || !upscaleRight )
        {
            LOGGER.trace( WHILE_ATTEMPTING_TO_FIND_THE_INTERSECTING_TIMES_BETWEEN_THE_LEFT_SERIES_AND_THE_RIGHT
                          + "series {} at the desired time scale of {}, discovered {} intersecting times. No thinning "
                          + "will be performed. Returning these times: {}.",
                          left.getMetadata(),
                          right.getMetadata(),
                          desiredTimeScale,
                          immutableIntersectingTimes.size(),
                          immutableIntersectingTimes );

            return immutableIntersectingTimes;
        }

        Duration period = desiredTimeScale.getPeriod();
        Duration frequencyToUse = frequency;
        if ( Objects.isNull( frequencyToUse ) )
        {
            frequencyToUse = period;
        }

        // Use the first available reference time or the first valid time
        Instant referenceTime = TimeSeriesSlicer.getFirstReferenceTime( left, right );
        Instant start = referenceTime;

        if ( Objects.isNull( start ) )
        {
            start = TimeSeriesSlicer.getFirstValidTimeAdjustedForTimeScalePeriod( immutableIntersectingTimes, period );
        }

        SortedSet<Instant> snippedSequence = TimeSeriesSlicer.snip( immutableIntersectingTimes,
                                                                    timeWindow,
                                                                    referenceTime );

        LOGGER.debug( "Attempting to acquire a regular sequence of times with a reference time of {}, a frequency of "
                      + "{}, a start time of {} and a sequence of {} valid times snipped to a time windows of {}.",
                      referenceTime,
                      frequency,
                      start,
                      snippedSequence.size(),
                      timeWindow );

        return TimeSeriesSlicer.getRegularSequence( snippedSequence,
                                                    start,
                                                    period,
                                                    frequencyToUse );
    }

    /**
     * Extracts the events within a time-series and maps them by the duration between a prescribed 
     * {@link ReferenceTimeType} and the event valid time. If the reference time is not found, an empty map is returned.
     *
     * @param <T> the type of event value
     * @param timeSeries the time-series to map
     * @param referenceTimeType the reference time from which to compute durations
     * @return the mapped events
     * @throws NullPointerException if the event set is null
     */


    public static <T> Map<Duration, Event<T>> mapEventsByDuration( TimeSeries<T> timeSeries,
                                                                   ReferenceTimeType referenceTimeType )
    {
        Objects.requireNonNull( timeSeries, NULL_INPUT_EXCEPTION );

        Instant referenceTime = timeSeries.getReferenceTimes()
                                          .get( referenceTimeType );

        if ( Objects.isNull( referenceTime ) )
        {
            return Collections.emptyMap();
        }

        Map<Duration, Event<T>> returnMe = new TreeMap<>();
        timeSeries.getEvents()
                  .forEach( event -> returnMe.put( Duration.between( referenceTime, event.getTime() ), event ) );

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * <p>Returns a trace-view of an ensemble time-series. The input series is decomposed into one Set for each ensemble 
     * trace, with the label as a key.
     *
     * <p>Reference datetimes metadata are lost but ensemble labels are preserved.
     *
     * @param timeSeries The time-series to decompose
     * @return A map with the trace label as key, sorted set of Events as value.
     * @throws UnsupportedOperationException When the ensemble events contain a varying number of members and the 
     *            ensemble labels have not been provided to distinguish between them.
     */

    public static Map<Object, SortedSet<Event<Double>>> decomposeWithLabels( TimeSeries<Ensemble> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        if ( timeSeries.getEvents().isEmpty() )
        {
            return Collections.emptyMap();
        }

        Integer traceCount = null;

        // A map of ensemble members per valid time organized by label or index
        Map<Object, SortedSet<Event<Double>>> membersByTime = new TreeMap<>();

        // Check that all events have the same number of members
        for ( Event<Ensemble> next : timeSeries.getEvents() )
        {
            // No labels, so check for a constant number of ensemble members
            if ( Objects.nonNull( traceCount ) && next.getValue().size() != traceCount )
            {
                throw new UnsupportedOperationException( "Cannot determine the ensemble traces from the input "
                                                         + "time-series because the number of ensemble members "
                                                         + "varies by valid time and the trace labels were not "
                                                         + "available (hint for developers: add the ensemble labels "
                                                         + "to the ensemble information.)" );
            }

            Ensemble ensemble = next.getValue();
            traceCount = ensemble.size();

            double[] members = ensemble.getMembers();
            Labels nextLabels = ensemble.getLabels();

            String[] labels = nextLabels.getLabels();

            for ( int i = 0; i < traceCount; i++ )
            {
                Object label = i + 1;
                if ( ensemble.hasLabels() )
                {
                    label = labels[i];
                }

                SortedSet<Event<Double>> nextTime = membersByTime.get( label );
                if ( Objects.isNull( nextTime ) )
                {
                    nextTime = new TreeSet<>();
                    membersByTime.put( label, nextTime );
                }

                nextTime.add( DoubleEvent.of( next.getTime(), members[i] ) );
            }
        }

        return Collections.unmodifiableMap( membersByTime );
    }


    /**
     * <p>Returns a trace-view of an ensemble time-series. The input series is decomposed into one time-series for each
     * ensemble trace. The order of the traces in the returned list is the natural order of the trace labels. This
     * order becomes important when decomposing ensemble time-series, operating on them, and then recomposing them with
     * prescribed labels, because the labels are not preserved in the decomposed series. Also see
     * {@link #compose(List, SortedSet)}.
     *
     * @param timeSeries the time-series to decompose
     * @return a trace view with as many time-series as ensemble traces in natural order of trace label
     * @throws UnsupportedOperationException if the ensemble events contain a varying number of members and the
     *            ensemble labels have not been provided to distinguish between them
     */

    public static List<TimeSeries<Double>> decompose( TimeSeries<Ensemble> timeSeries )
    {
        Objects.requireNonNull( timeSeries );

        if ( timeSeries.getEvents().isEmpty() )
        {
            return List.of( new TimeSeries.Builder<Double>().setMetadata( timeSeries.getMetadata() )
                                                            .build() );
        }

        Map<Object, SortedSet<Event<Double>>> withLabels = TimeSeriesSlicer.decomposeWithLabels( timeSeries );
        List<TimeSeries<Double>> returnMe = new ArrayList<>();

        for ( Map.Entry<Object, SortedSet<Event<Double>>> next : withLabels.entrySet() )
        {
            TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
            TimeSeries<Double> series = builder.setMetadata( timeSeries.getMetadata() )
                                               .setEvents( next.getValue() )
                                               .build();
            returnMe.add( series );
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * <p>Composes an ensemble time-series from a collection of single-valued series. The opposite of 
     * {@link #decompose(TimeSeries)}. When supplied, the trace labels are ordered in their natural order and assigned
     * in order of the series provided. Also see {@link #decompose(TimeSeries)}.
     *
     * @param timeSeries the collection of time-series to compose in the natural order of the labels, when supplied
     * @param labels the ensemble labels, which contains zero labels or as many labels as timeSeries
     * @return a trace view with as many time-series as ensemble traces
     * @throws IllegalArgumentException if there is more than zero labels, but not the same number as time-series or
     *            the input series contain different reference times or time scales
     * @throws NullPointerException if any input is null
     */

    public static TimeSeries<Ensemble> compose( List<TimeSeries<Double>> timeSeries, SortedSet<String> labels )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( labels );

        if ( !labels.isEmpty() && labels.size() != timeSeries.size() )
        {
            throw new IllegalArgumentException( "Expected zero labels or as many labels as time-series ("
                                                + timeSeries.size()
                                                + "), but found: "
                                                + labels.size()
                                                + "." );
        }

        // Valid times and ensemble values for composition   
        Map<Instant, List<Double>> ensembles = new TreeMap<>();
        TimeSeriesMetadata metadata = null;

        for ( TimeSeries<Double> nextSeries : timeSeries )
        {
            for ( Event<Double> nextEvent : nextSeries.getEvents() )
            {
                List<Double> next = ensembles.get( nextEvent.getTime() );

                if ( Objects.isNull( next ) )
                {
                    next = new ArrayList<>();
                    ensembles.put( nextEvent.getTime(), next );
                }

                next.add( nextEvent.getValue() );
            }

            // Set the reference times and time scale
            if ( Objects.isNull( metadata ) )
            {
                metadata = nextSeries.getMetadata();
            }
            else if ( !nextSeries.getMetadata()
                                 .equals( metadata ) )
            {
                throw new IllegalArgumentException( "One or more of the input series have different metadata,"
                                                    + " which is not allowed when composing them into an "
                                                    + "ensemble." );
            }
        }

        return TimeSeriesSlicer.compose( ensembles, metadata, labels );
    }

    /**
     * Transforms a time-series from one type to another.
     *
     * @param <S> the existing type of time-series value
     * @param <T> the required type of time-series value
     * @param timeSeries the time-series
     * @param mapper the mapper
     * @param metaMapper an optional metadata mapper that updates the metadata
     * @return a filtered view of the input series
     * @throws NullPointerException if any input is null
     */

    public static <S, T> TimeSeries<T> transform( TimeSeries<S> timeSeries,
                                                  Function<S, T> mapper,
                                                  UnaryOperator<TimeSeriesMetadata> metaMapper )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( mapper );

        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();

        TimeSeriesMetadata metadata = timeSeries.getMetadata();

        if ( Objects.nonNull( metaMapper ) )
        {
            metadata = metaMapper.apply( metadata );
        }

        builder.setMetadata( metadata );

        for ( Event<S> event : timeSeries.getEvents() )
        {
            T transformed = mapper.apply( event.getValue() );

            if ( Objects.nonNull( transformed ) )
            {
                Event<T> transformedEvent = Event.of( event.getTime(), transformed );
                builder.addEvent( transformedEvent );
            }
        }

        return builder.build();
    }

    /**
     * Transforms a time-series from one type to another, conditionally upon the valid time of each event as well as 
     * the event value.
     *
     * @param <S> the existing type of time-series value
     * @param <T> the required type of time-series value
     * @param timeSeries the time-series
     * @param mapper the event mapper
     * @return a filtered view of the input series
     * @throws NullPointerException if any input is null
     */

    public static <S, T> TimeSeries<T> transformByEvent( TimeSeries<S> timeSeries, Function<Event<S>, Event<T>> mapper )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( mapper );

        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();

        builder.setMetadata( timeSeries.getMetadata() );

        for ( Event<S> event : timeSeries.getEvents() )
        {
            Event<T> transformed = mapper.apply( event );

            if ( Objects.nonNull( transformed ) )
            {
                builder.addEvent( transformed );
            }
        }

        return builder.build();
    }

    /**
     * Creates an ensemble transformer from a member transformer by applying the member transformer to each member of 
     * the ensemble.
     * @param memberTransformer the member transformer
     * @return an ensemble transformer
     * @throws NullPointerException if the memberTransformer is null
     */

    public static UnaryOperator<Ensemble> getEnsembleTransformer( UnaryOperator<Double> memberTransformer )
    {
        Objects.requireNonNull( memberTransformer );

        // Create an ensemble conversion function
        return ensemble -> {
            double[] members = ensemble.getMembers();
            double[] converted = Arrays.stream( members )
                                       .map( memberTransformer::apply )
                                       .toArray();

            return Ensemble.of( converted, ensemble.getLabels(), ensemble.areSortedMembersCached() );
        };
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     *
     * @param <L> the type of left value
     * @param <R> the type of right value
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static <L, R> Pool<TimeSeries<Pair<L, R>>> filter( Pool<TimeSeries<Pair<L, R>>> input,
                                                              Predicate<Pair<L, R>> condition,
                                                              DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( condition );

        Pool.Builder<TimeSeries<Pair<L, R>>> builder = new Pool.Builder<>();

        builder.setMetadata( input.getMetadata() );

        // Filter climatology as required
        if ( input.hasClimatology() )
        {
            Climatology climatology = input.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( input.getClimatology(), applyToClimatology );
            }

            builder.setClimatology( climatology );
        }

        // Filter the main data
        for ( TimeSeries<Pair<L, R>> next : input.get() )
        {
            builder.addData( TimeSeriesSlicer.filter( next, condition ) );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            Pool<TimeSeries<Pair<L, R>>> baseline = input.getBaselineData();

            for ( TimeSeries<Pair<L, R>> nextBase : baseline.get() )
            {
                builder.addDataForBaseline( TimeSeriesSlicer.filter( nextBase, condition ) );
            }

            builder.setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
    }

    /**
     * Consolidates the input collection of time-series into one time-series. Requires that none of the valid datetimes 
     * are duplicates.
     *
     * @param <T> the time-series event value type
     * @param collectedSeries the collected series
     * @return the consolidated series
     * @throws NullPointerException if the consolidatedSeries is null
     * @throws IllegalArgumentException if there are two or more events with the same valid datetime
     */

    public static <T> TimeSeries<T> consolidate( Collection<TimeSeries<T>> collectedSeries )
    {
        Objects.requireNonNull( collectedSeries );

        // Empty series
        if ( collectedSeries.isEmpty() )
        {
            // TODO: restore empty timeseries capability after weeding out spots
            // where some kind of metadata can be put in. Then return an empty 
            // time-series with metadata.
            throw new IllegalStateException( "Cannot consolidate an empty collection of timeseries." );
        }
        // Singleton series
        else if ( collectedSeries.size() == 1 )
        {
            return collectedSeries.iterator()
                                  .next();
        }

        TimeSeries.Builder<T> builder = new TimeSeries.Builder<>();

        for ( TimeSeries<T> next : collectedSeries )
        {
            builder.setMetadata( next.getMetadata() );
            builder.addEvents( next.getEvents() );
        }

        return builder.build();
    }

    /**
     * Returns the mid-point on the UTC timeline between the two inputs
     *
     * @param earliest the earliest time
     * @param latest the latest time
     * @return the mid-point on the UTC timeline
     * @throws NullPointerException if either input is null
     */

    public static Instant getMidPointBetweenTimes( Instant earliest, Instant latest )
    {
        Objects.requireNonNull( earliest );
        Objects.requireNonNull( latest );

        return earliest.plus( Duration.between( earliest, latest )
                                      .dividedBy( 2 ) );
    }

    /**
     * Adds a declared existing timescale to a time-series that has no timescale defined or updates the function
     * associated with a timescale that is defined.
     *
     * @param <T> the time-series event value type
     * @param timeSeries the time-series
     * @param timeScale the declared existing timescale
     * @return the augmented time-series
     * @throws IllegalArgumentException if the declared timescale is inconsistent with the existing timescale
     * @throws NullPointerException if either input is null
     */

    public static <T> TimeSeries<T> augmentTimeSeriesWithTimeScale( TimeSeries<T> timeSeries, TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( timeScale );

        TimeScaleOuter existingTimeScale = timeSeries.getTimeScale();

        TimeScaleOuter newTimeScale = TimeSeriesSlicer.augmentTimeScale( existingTimeScale, timeScale );

        // Update
        TimeSeriesMetadata newMetadata = timeSeries.getMetadata()
                                                   .toBuilder()
                                                   .setTimeScale( newTimeScale )
                                                   .build();

        return new TimeSeries.Builder<T>().setMetadata( newMetadata )
                                          .setEvents( timeSeries.getEvents() )
                                          .build();
    }

    /**
     * Augments an existing time-scale with a new one, where possible.
     *
     * @param existingTimeScale the existing time scale, optional
     * @param newTimeScale, the new time scale, required
     * @return the augmented time scale
     * @throws IllegalArgumentException if the declared time scale is inconsistent with the existing time-scale
     * @throws NullPointerException if the newTimeScale is null
     */

    public static TimeScaleOuter augmentTimeScale( TimeScaleOuter existingTimeScale, TimeScaleOuter newTimeScale )
    {
        Objects.requireNonNull( newTimeScale );

        // Create a time scale that matches the existing time scale, if not null, but has the function of the new time
        // scale. If these things are not equal or both instantaneous, the time scales are inconsistent and the declared
        // time scale cannot be augmented
        if ( Objects.nonNull( existingTimeScale ) )
        {
            TimeScale scale = existingTimeScale.getTimeScale()
                                               .toBuilder()
                                               .setFunction( newTimeScale.getFunction() )
                                               .build();
            TimeScaleOuter scaleToCheck = TimeScaleOuter.of( scale );
            if ( !scaleToCheck.equalsOrInstantaneous( newTimeScale ) )
            {
                throw new IllegalArgumentException( "Cannot mutate the existing time-scale because it is not null and "
                                                    + "it is inconsistent with the declared existing time scale. The "
                                                    + "existing time scale was: "
                                                    + existingTimeScale
                                                    + ". The declared existing time scale was: "
                                                    + newTimeScale
                                                    + "." );
            }
        }

        return newTimeScale;
    }

    /**
     * Inspects the sorted list of events by counting backwards from the input index. Returns the index of the earliest 
     * time that is larger than the prescribed start time. This is useful for backfilling when searching for groups of
     * events by time. See {@link #groupEventsByInterval(SortedSet, SortedSet, Duration)}.
     *
     * @param workBackFromHere the index at which to begin searching backwards
     * @param startTime the start time that must be exceeded
     * @param events the list of events in time order
     */

    private static <T> int getIndexOfEarliestTimeThatIsGreaterThanInputTime( int workBackFromHere,
                                                                             Instant startTime,
                                                                             List<Event<T>> events )
    {
        // Set the new start time
        int nextStartIndex = workBackFromHere;
        int onePriorToStart = nextStartIndex - 1;
        for ( int j = onePriorToStart; j > -1; j-- )
        {
            Event<T> backEvent = events.get( j );
            Instant backTime = backEvent.getTime();

            if ( backTime.isAfter( startTime ) )
            {
                nextStartIndex--;
            }
            else
            {
                break;
            }
        }

        return nextStartIndex;
    }

    /**
     * <p>Composes an ensemble time-series from a map of values.
     *
     * @param ensembles the ensemble members per time
     * @param metadata The time-series metadata
     * @param labels the member labels
     * @return an ensemble times-series
     * @throws NullPointerException if any input other than the time scale is null
     */

    private static TimeSeries<Ensemble> compose( Map<Instant, List<Double>> ensembles,
                                                 TimeSeriesMetadata metadata,
                                                 SortedSet<String> labels )
    {
        Objects.requireNonNull( ensembles );
        Objects.requireNonNull( metadata );
        Objects.requireNonNull( labels );

        TimeSeries.Builder<Ensemble> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        Labels labs;

        if ( !labels.isEmpty() )
        {
            String[] stringLabels = labels.toArray( new String[0] );
            // These are de-duplicated centrally
            labs = Labels.of( stringLabels );
        }
        else
        {
            labs = Labels.of();
        }

        for ( Map.Entry<Instant, List<Double>> next : ensembles.entrySet() )
        {
            Instant time = next.getKey();
            double[] doubles = next.getValue()
                                   .stream()
                                   .mapToDouble( Double::valueOf )
                                   .toArray();
            Ensemble value = Ensemble.of( doubles, labs );
            builder.addEvent( Event.of( time, value ) );
        }

        return builder.build();
    }

    /**
     * Snips the first series to the bounds of the second series.
     *
     * @param <S> the type of time-series event value in the series to snip
     * @param toSnip the series to snip
     * @param snipTo the series to snip to
     * @param lowerBuffer an optional buffer to subtract from the lower bound
     * @param upperBuffer an optional buffer to add to the upper bound
     * @return a snipped series
     * @throws NullPointerException if toSnip or snipTo is null
     */

    public static <S> TimeSeries<S> snip( TimeSeries<S> toSnip,
                                          TimeSeries<?> snipTo,
                                          Duration lowerBuffer,
                                          Duration upperBuffer )
    {
        Objects.requireNonNull( toSnip );
        Objects.requireNonNull( snipTo );

        if ( snipTo.getEvents().isEmpty() )
        {
            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace(
                        "While snipping series {} to series {} with lower buffer {} and upper buffer {}, no events "
                        + "were discovered within the series to snip to. Returning the unsnipped series.",
                        toSnip,
                        snipTo,
                        lowerBuffer,
                        upperBuffer );
            }

            return toSnip;
        }

        Instant lower = snipTo.getEvents()
                              .first()
                              .getTime();
        Instant upper = snipTo.getEvents()
                              .last()
                              .getTime();

        // Adjust the lower bound
        if ( Objects.nonNull( lowerBuffer ) )
        {
            lower = lower.minus( lowerBuffer );
        }

        // Adjust the upper bound
        if ( Objects.nonNull( upperBuffer ) )
        {
            upper = upper.plus( upperBuffer );
        }

        TimeSeries.Builder<S> snippedSeries = new TimeSeries.Builder<>();
        snippedSeries.setMetadata( toSnip.getMetadata() );

        // Iterate the tailset of events that starts with a valid time at the lower bound
        SortedSet<Event<S>> toSnipEvents = toSnip.getEvents();
        SortedSet<Event<S>> tailSet = toSnipEvents;

        if ( !toSnipEvents.isEmpty() )
        {
            Event<S> lowerBound = Event.of( lower,
                                            toSnipEvents.first()
                                                        .getValue() );

            tailSet = toSnipEvents.tailSet( lowerBound ); // #92522-109
        }

        for ( Event<S> next : tailSet )
        {
            Instant nextTime = next.getTime();

            if ( nextTime.compareTo( lower ) >= 0 )
            {
                if ( nextTime.compareTo( upper ) <= 0 )
                {
                    snippedSeries.addEvent( next );
                }
                // Events are sorted, exploit this: #92522-74
                else
                {
                    break;
                }
            }
        }

        TimeSeries<S> snipped = snippedSeries.build();

        if ( snipped.getEvents()
                    .isEmpty()
             && LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "While snipping series {} to series {} with lower buffer {} and upper buffer {}, no events "
                          + "were discovered within the series to snip that were within the bounds of the series to "
                          + "snip to.",
                          toSnip.getMetadata(),
                          snipTo.getMetadata(),
                          lowerBuffer,
                          upperBuffer );
        }

        return snipped;
    }

    /**
     * Adjusts the earliest lead duration of the time window to account for the period associated with the desired time 
     * scale in order to capture sufficient data for rescaling. If the time scale is instantaneous, no adjustment is
     * made.
     *
     * @param timeWindow the time window to adjust, required
     * @param desiredTimeScale the desired time scale, lenient if null (returns the input time window)
     * @return the adjusted time window
     */

    public static TimeWindowOuter adjustByTimeScalePeriod( TimeWindowOuter timeWindow, TimeScaleOuter desiredTimeScale )
    {
        Objects.requireNonNull( timeWindow );

        if ( Objects.nonNull( desiredTimeScale ) && desiredTimeScale.isInstantaneous() )
        {
            LOGGER.debug( "Not adjusting the time window of {} with the time scale because the time scale is "
                          + "instantaneous.", timeWindow );
            return timeWindow;
        }

        TimeWindowOuter adjustedWindow = timeWindow;
        if ( !timeWindow.getEarliestLeadDuration().equals( TimeWindowOuter.DURATION_MIN )
             && Objects.nonNull( desiredTimeScale ) )
        {
            Duration period = TimeScaleOuter.getOrInferPeriodFromTimeScale( desiredTimeScale );
            Duration lowerD = timeWindow.getEarliestLeadDuration()
                                        .minus( period );
            com.google.protobuf.Duration lower =
                    com.google.protobuf.Duration.newBuilder()
                                                .setSeconds( lowerD.getSeconds() )
                                                .setNanos( lowerD.getNano() )
                                                .build();

            TimeWindow inner = timeWindow.getTimeWindow()
                                         .toBuilder()
                                         .setEarliestLeadDuration( lower )
                                         .build();

            adjustedWindow = TimeWindowOuter.of( inner );

            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug(
                        "Adjusted the earliest lead duration of {} by {} to {}, in order to select sufficient data "
                        + "for rescaling.",
                        timeWindow,
                        desiredTimeScale.getPeriod(),
                        adjustedWindow );
            }
        }

        return adjustedWindow;
    }

    /**
     * Adds a prescribed offset to the valid time of each time-series in the list.
     *
     * @param <T> the time-series event value type
     * @param toTransform the list of time-series to transform
     * @param offset the offset to add
     * @return the adjusted time-series
     */

    public static <T> TimeSeries<T> applyOffsetToValidTimes( TimeSeries<T> toTransform, Duration offset )
    {
        Objects.requireNonNull( toTransform );
        Objects.requireNonNull( offset );

        TimeSeries<T> transformed = toTransform;

        // Transform valid times?
        if ( !Duration.ZERO.equals( offset ) )
        {
            SortedSet<Event<T>> events = toTransform.getEvents();
            TimeSeries.Builder<T> timeTransformed = new TimeSeries.Builder<>();
            timeTransformed.setMetadata( toTransform.getMetadata() );

            for ( Event<T> next : events )
            {
                Instant adjustedTime = next.getTime()
                                           .plus( offset );
                Event<T> event = Event.of( adjustedTime, next.getValue() );
                timeTransformed.addEvent( event );
            }

            transformed = timeTransformed.build();

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( "Added {} to the valid times associated with time-series {}.",
                              offset,
                              transformed.hashCode() );
            }
        }

        return transformed;
    }

    /**
     * Snips the input series to the prescribed time window. Only snips lead durations with respect to reference times 
     * with the type {@link ReferenceTimeType#T0}.
     *
     * @param <T> the time-series event value type
     * @param toSnip the time-series to snip
     * @param snipTo the time window to use when snipping
     * @return the snipped time-series
     */

    public static <T> TimeSeries<T> snip( TimeSeries<T> toSnip, TimeWindowOuter snipTo )
    {
        Objects.requireNonNull( toSnip );

        TimeSeries<T> returnMe = toSnip;

        if ( Objects.nonNull( snipTo ) )
        {

            // Snip datetimes first, because lead durations are only snipped with respect to 
            // the ReferenceTimeType.T0
            TimeWindow inner = MessageFactory.getTimeWindow( snipTo.getEarliestReferenceTime(),
                                                             snipTo.getLatestReferenceTime(),
                                                             snipTo.getEarliestValidTime(),
                                                             snipTo.getLatestValidTime() );
            TimeWindowOuter partialSnip = TimeWindowOuter.of( inner );

            LOGGER.debug( "Snipping paired time-series {} to the time window of {}.",
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
     * Returns a decimal representation of the duration in the prescribed units with conversion to millisecond
     * precision. For greater precision, use a {@link java.math.BigDecimal}, but they are expensive.
     * @param duration the duration
     * @param unit the unit
     * @return the duration with millisecond precision
     */
    public static double durationToDecimalMilliPrecision( Duration duration, ChronoUnit unit )
    {
        Objects.requireNonNull( unit );
        Objects.requireNonNull( duration );

        return ( duration.getSeconds() * 1000 + ( duration.getNano() / 1_000_000.0 ) ) / unit.getDuration()
                                                                                             .toMillis();
    }

    /**
     * Returns the reference times that fall within the right-closed time window.
     *
     * @param referenceTimes the reference times
     * @param timeWindow the time window
     * @return the reference times that fall within the window
     */

    private static Map<ReferenceTimeType, Instant> filterReferenceTimes( Map<ReferenceTimeType, Instant> referenceTimes,
                                                                         TimeWindowOuter timeWindow )
    {

        Map<ReferenceTimeType, Instant> returnMe = new EnumMap<>( ReferenceTimeType.class );

        // Unbounded, so everything qualifies
        if ( timeWindow.getEarliestReferenceTime().equals( Instant.MIN )
             && timeWindow.getLatestReferenceTime().equals( Instant.MAX ) )
        {
            returnMe = referenceTimes;
        }
        else
        {
            for ( Map.Entry<ReferenceTimeType, Instant> nextReference : referenceTimes.entrySet() )
            {
                Instant reference = nextReference.getValue();

                // Lower bound exclusive, upper inclusive
                if ( TimeSeriesSlicer.isContained( reference,
                                                   timeWindow.getEarliestReferenceTime(),
                                                   timeWindow.getLatestReferenceTime() ) )
                {
                    returnMe.put( nextReference.getKey(), nextReference.getValue() );
                }
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Returns true if the input time is contained within the right-closed bounds provided.
     *
     * @param time the time to test
     * @param lowerExclusive the lower exclusive limit
     * @param upperInclusive the upper inclusive limit
     * @return true if the time is within (lowerExclusive, upperInclusive], otherwise false
     */

    private static boolean isContained( Instant time, Instant lowerExclusive, Instant upperInclusive )
    {
        Objects.requireNonNull( time );

        Objects.requireNonNull( lowerExclusive );

        Objects.requireNonNull( upperInclusive );

        if ( lowerExclusive.equals( Instant.MIN ) && upperInclusive.equals( Instant.MAX ) )
        {
            return true;
        }

        return time.equals( upperInclusive ) || ( time.isAfter( lowerExclusive ) && time.isBefore( upperInclusive ) );
    }

    /**
     * Returns true if the input duration is contained within the right-closed bounds provided.
     *
     * @param duration the duration to test
     * @param lowerExclusive the lower exclusive limit
     * @param upperInclusive the upper inclusive limit
     * @return true if the duration is within (lowerExclusive, upperInclusive], otherwise false
     */

    private static boolean isContained( Duration duration, Duration lowerExclusive, Duration upperInclusive )
    {
        Objects.requireNonNull( duration );

        Objects.requireNonNull( lowerExclusive );

        Objects.requireNonNull( upperInclusive );

        return duration.equals( upperInclusive ) ||
               ( duration.compareTo( lowerExclusive ) > 0 && duration.compareTo( upperInclusive ) < 0 );
    }

    /**
     * Helper that returns the first available reference time from the time series, otherwise <code>null</code>.
     *
     * @param left the left time series
     * @param right the right time series
     * @return the first available reference time or null
     */

    private static Instant getFirstReferenceTime( TimeSeries<?> left,
                                                  TimeSeries<?> right )
    {
        Collection<Instant> times = new ArrayList<>( left.getReferenceTimes().values() );
        times.addAll( right.getReferenceTimes().values() );

        Instant returnMe = null;

        if ( !times.isEmpty() )
        {
            returnMe = times.iterator()
                            .next();
        }

        return returnMe;
    }

    /**
     * Helper that returns the first available valid time, adjusted for the <code>period</code> associated with the 
     * time scale, otherwise <code>null</code>.
     *
     * @param validTimes the valid times
     * @param period the time scale period
     * @return the first available valid time adjusted for the time scale period
     */

    private static Instant getFirstValidTimeAdjustedForTimeScalePeriod( SortedSet<Instant> validTimes,
                                                                        Duration period )
    {
        Instant returnMe = null;

        // Valid time instead?
        if ( !validTimes.isEmpty() )
        {
            returnMe = validTimes.first();

            if ( Objects.nonNull( period ) )
            {
                returnMe = returnMe.minus( period );
            }
        }

        return returnMe;
    }

    /**
     * Returns a regular sequence of times from the supplied set of times using the prescribed period and frequency.
     *
     * @param timesToThin the times from which a regular sequence should be created
     * @param origin the start of the sequence
     * @param period the period associated with the desired time scale
     * @param frequency the regular frequency with which to count periods, defaults to the period
     * @return a regular sequence of times
     * @throws NullPointerException if any input is null
     */

    private static SortedSet<Instant> getRegularSequence( SortedSet<Instant> timesToThin,
                                                          Instant origin,
                                                          Duration period,
                                                          Duration frequency )
    {
        Objects.requireNonNull( timesToThin );
        Objects.requireNonNull( origin );
        Objects.requireNonNull( period );

        LOGGER.debug( "Constructing a regular sequence of times with an origin of {}, a period of {}, and a frequency "
                      + "of {}.",
                      origin,
                      period,
                      frequency );

        // fewer than two times? Nothing to thin
        if ( timesToThin.size() < 2 )
        {
            LOGGER.debug( "Discovered {} times to thin in the input set of times, so returning the input set.",
                          timesToThin.size() );

            return timesToThin;
        }

        // More than one pair?
        SortedSet<Instant> thinnedTimes = new TreeSet<>();
        // Duration by which to jump between periods
        // Default to the period, aka "back-to-back"
        Duration jump = frequency;
        if ( Objects.isNull( frequency ) )
        {
            jump = period;
        }

        List<Instant> listedTimesToThin = List.copyOf( timesToThin );

        // Get the start time for the regular sequence
        Instant nextTime = TimeSeriesSlicer.getStartTimeForRegularSequence( origin,
                                                                            period,
                                                                            jump,
                                                                            listedTimesToThin );

        int totalEvents = timesToThin.size();
        Instant lastTime = timesToThin.last();

        // Increment the sequence from the start time
        int start = 0;
        while ( nextTime.compareTo( lastTime ) <= 0 )
        {
            // Does a time exist at the next regular time?
            for ( int i = start; i < totalEvents; i++ )
            {
                Instant nextListedTime = listedTimesToThin.get( i );

                // Yes it does. Add it.
                if ( nextListedTime.equals( nextTime ) )
                {
                    thinnedTimes.add( nextListedTime );
                    start = i + 1;
                    break;
                }
            }

            nextTime = nextTime.plus( jump );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Inspected {} times and eliminated {} times to produce a regular sequence that has a "
                          + "period of {} and repeats every {}.",
                          timesToThin.size(),
                          timesToThin.size() - thinnedTimes.size(),
                          period,
                          frequency );
        }

        return Collections.unmodifiableSortedSet( thinnedTimes );
    }

    /**
     * Steps away from the reference time by the period, initially, then the frequency, until an event is discovered in
     * the list with the same valid time. If no event is discovered, returns the valid time of the first event in the 
     * list.
     *
     * @param referenceTime the reference time
     * @param period the period
     * @param frequency the frequency
     * @param timeOrderedListOfTimes the events in order of valid time
     * @return the start time
     */

    private static Instant getStartTimeForRegularSequence( Instant referenceTime,
                                                           Duration period,
                                                           Duration frequency,
                                                           List<Instant> timeOrderedListOfTimes )
    {
        Instant firstTime = timeOrderedListOfTimes.get( 0 );

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
        return TimeSeriesSlicer.getStartTimeForRegularSequenceSearchingForwards( referenceTime,
                                                                                 period,
                                                                                 frequency,
                                                                                 timeOrderedListOfTimes );
    }

    /**
     * Steps forwards from the reference time by the period, initially, then the frequency, until a time is discovered 
     * in the list. If no time is discovered, returns the first time in the list.
     *
     * @param referenceTime the reference time
     * @param period the period
     * @param frequency the frequency
     * @param timeOrderedListOfTimes the events in order of valid time
     * @return the start time
     */

    private static Instant getStartTimeForRegularSequenceSearchingForwards( Instant referenceTime,
                                                                            Duration period,
                                                                            Duration frequency,
                                                                            List<Instant> timeOrderedListOfTimes )
    {
        int totalTimes = timeOrderedListOfTimes.size();
        Instant firstTime = timeOrderedListOfTimes.get( 0 );
        Instant lastTime = timeOrderedListOfTimes.get( totalTimes - 1 );

        // Increment the sequence of valid times until the last time
        // It is debatable whether the first step away from the reference time should use the period or the frequency.
        // The period is chosen here.
        Instant nextSequenceTime = referenceTime.plus( period );

        // Iterate the regular sequence until the end
        while ( nextSequenceTime.compareTo( lastTime ) <= 0 )
        {
            // Does a time exist in the list of times that matches the next time in the sequence?
            for ( Instant nextTime : timeOrderedListOfTimes )
            {
                int compare = nextTime.compareTo( nextSequenceTime );

                // Yes it does. Return it.
                if ( compare == 0 )
                {
                    return nextTime;
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
     * Returns the subset of valid times that fall within the time window.
     * @param times the times
     * @param timeWindow the time window
     * @param referenceTime an optional forecast reference time
     * @return the subset of times that fall within the window
     */

    private static SortedSet<Instant> snip( SortedSet<Instant> times,
                                            TimeWindowOuter timeWindow,
                                            Instant referenceTime )
    {
        Objects.requireNonNull( times );
        Objects.requireNonNull( timeWindow );

        SortedSet<Instant> contained = new TreeSet<>();

        for ( Instant nextValidTime : times )
        {
            boolean include;

            // Filter by valid times
            // Falls on upper bound or falls within bounds
            include = nextValidTime.equals( timeWindow.getLatestValidTime() )
                      || ( nextValidTime.isAfter( timeWindow.getEarliestValidTime() )
                           && nextValidTime.isBefore( timeWindow.getLatestValidTime() ) );

            // Filter by lead durations
            if ( include && Objects.nonNull( referenceTime ) && !timeWindow.bothLeadDurationsAreUnbounded() )
            {
                Duration leadDuration = Duration.between( referenceTime, nextValidTime );

                // Falls on upper bound or falls within bounds
                include = leadDuration.equals( timeWindow.getLatestLeadDuration() )
                          || ( leadDuration.compareTo( timeWindow.getEarliestLeadDuration() ) > 0
                               && leadDuration.compareTo( timeWindow.getLatestLeadDuration() ) <= 0 );
            }

            if ( include )
            {
                contained.add( nextValidTime );
            }
        }

        return Collections.unmodifiableSortedSet( contained );
    }

    /**
     * Helper that returns the unique timesteps between consecutive times in the input.
     *
     * @param times the times
     * @return the unique timesteps
     * @throws NullPointerException if the input is null
     */

    private static SortedSet<Duration> getTimesteps( SortedSet<Instant> times )
    {
        Objects.requireNonNull( times );

        Instant last = null;
        SortedSet<Duration> timesteps = new TreeSet<>();
        for ( Instant next : times )
        {
            if ( Objects.nonNull( last ) )
            {
                Duration timestep = Duration.between( last, next );
                timesteps.add( timestep );
            }
            last = next;
        }

        return Collections.unmodifiableSortedSet( timesteps );
    }

    /**
     * Hidden constructor.
     */

    private TimeSeriesSlicer()
    {
    }

}
