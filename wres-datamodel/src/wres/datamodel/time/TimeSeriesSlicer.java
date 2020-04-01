package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.Ensemble;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility class for slicing/dicing and transforming time-series datasets. 
 * 
 * @author james.brown@hydrosolved.com
 * @see     Slicer
 */

public final class TimeSeriesSlicer
{

    /**
     * Null input error message.
     */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesSlicer.class );

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

    public static <S> Predicate<TimeSeries<Pair<S, S>>>
            anyOfLeftAndAnyOfRightInTimeSeries( Predicate<S> predicate )
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

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();

        builder.setMetadata( timeSeries.getMetadata() );

        for ( Event<T> event : timeSeries.getEvents() )
        {
            if ( filter.test( event.getValue() ) )
            {
                builder.addEvent( event );
            }
        }

        return builder.build();
    }   
    
    /**
     * Returns a filtered {@link TimeSeries} whose events are within the right-closed time intervals contained in the 
     * prescribed {@link TimeWindow}.
     * 
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param timeWindow the time window on which to slice
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if any input is null
     */

    public static <T> TimeSeries<T> filter( TimeSeries<T> input, 
                                            TimeWindow timeWindow )
    {
        Objects.requireNonNull( input );
        
        return TimeSeriesSlicer.filter( input, timeWindow, input.getReferenceTimes().keySet() );
    }

    /**
     * Returns a filtered {@link TimeSeries} whose events are within the right-closed time intervals contained in the 
     * prescribed {@link TimeWindow}. When considering lead durations, the filter may focus on all 
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
                                            TimeWindow timeWindow, 
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
        Map<ReferenceTimeType, Instant> notConsideredOrWithinBounds = new TreeMap<>();
        notConsideredOrWithinBounds.putAll( input.getReferenceTimes() );
        notConsideredOrWithinBounds.keySet().removeAll( subset.keySet() );
        notConsideredOrWithinBounds.putAll( referenceTimes );

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();
        builder.setMetadata( input.getMetadata() );

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
     * Returns a filtered view of a time-series based on the input predicate.
     * 
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

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();

        builder.setMetadata( timeSeries.getMetadata() );

        timeSeries.getEvents()
                  .stream()
                  .filter( filter )
                  .forEach( builder::addEvent );

        return builder.build();
    }     

    /**
     * Groups the input events according to the event valid time. An event falls within a group if its valid time falls 
     * within an interval that ends at a prescribed time and begins a specified period before that time. The interval 
     * is right-closed. In other words, when an event time falls within <code>(endsAt-period,endsAt]</code>, then add
     * that event to the group associated with <code>endsAt</code>. Use this method to determine groups of events for
     * upscaling values that end at <code>endsAt</code>.
     * 
     * TODO: this method has a nested loop, which implies O(n^2) complexity, where <code>n</code> is the cardinality of
     * <code>endsAt</code>. While it does exploit time-ordering to avoid searching the entire set of 
     * <code>endsAt</code>, it does not scale well for very large datasets.
     * 
     * @param <T> the type of event value
     * @param events the events to group
     * @param endsAt the end of each group, inclusive
     * @param period the period before each endsAt at which a group begins, exclusive
     * @return the grouped events
     * @throws NullPointerException if any input is null
     */


    public static <T> Map<Instant, SortedSet<Event<T>>> groupEventsByInterval( SortedSet<Event<T>> events,
                                                                               Set<Instant> endsAt,
                                                                               Duration period )
    {
        Objects.requireNonNull( events, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( endsAt, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( period, NULL_INPUT_EXCEPTION );

        Map<Instant, SortedSet<Event<T>>> grouped = new HashMap<>();

        // Events in a sorted list
        List<Event<T>> listedEvents = new ArrayList<>( events );
        int eventCount = listedEvents.size();

        SortedSet<Instant> ends = new TreeSet<>( endsAt );

        // Iterate the end times and group events whose times fall in (nextEnd-period,nextEnd]
        int startIndex = 0; // Position at which to start searching the listed events
        for ( Instant nextEnd : ends )
        {
            // Lower bound exclusive
            Instant nextStart = nextEnd.minus( period );

            for ( int i = startIndex; i < eventCount; i++ )
            {
                Event<T> nextEvent = listedEvents.get( i );
                Instant eventTime = nextEvent.getTime();

                // Is event time within (start,nextEnd]?
                SortedSet<Event<T>> nextGroup = grouped.get( nextEnd );
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
            return Map.of();
        }

        Map<Duration, Event<T>> returnMe = new TreeMap<>();
        timeSeries.getEvents()
                  .forEach( event -> returnMe.put( Duration.between( referenceTime, event.getTime() ), event ) );

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Inspects the sorted list of events by counting backwards from the input index. Returns the index of the earliest 
     * time that is larger than the prescribed start time. This is useful for backfilling when searching for groups of
     * events by time. See {@link #groupEventsByInterval(SortedSet, Set, Duration)}.
     * 
     * @param workBackFromhere the index at which to begin searching backwards
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
     * <p>Returns a trace-view of an ensemble time-series. The input series is
     * decomposed into one Set for each ensemble trace, with the label as a key.
     *
     * Reference datetimes metadata are lost but ensemble labels are preserved.
     *
     * @param timeSeries The time-series to decompose
     * @return A map with the trace label as key, sorted set of Events as value.
     * @throws UnsupportedOperationException When the ensemble events contain a
     * varying number of members and the ensemble labels have not been provided
     * to distinguish between them.
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

            traceCount = next.getValue().size();

            double[] members = next.getValue().getMembers();
            Optional<String[]> nextLabels = next.getValue().getLabels();

            for ( int i = 0; i < traceCount; i++ )
            {
                Object label = i;
                if ( nextLabels.isPresent() )
                {
                    label = nextLabels.get()[i];
                }

                SortedSet<Event<Double>> nextTime = membersByTime.get( label );
                if ( Objects.isNull( nextTime ) )
                {
                    nextTime = new TreeSet<>();
                    membersByTime.put( label, nextTime );
                }

                nextTime.add( Event.of( next.getTime(), members[i] ) );
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
            return List.of( new TimeSeriesBuilder<Double>().setMetadata( timeSeries.getMetadata() )
                                                           .build() );
        }

        Map<Object, SortedSet<Event<Double>>> withLabels = TimeSeriesSlicer.decomposeWithLabels( timeSeries );
        List<TimeSeries<Double>> returnMe = new ArrayList<>();

        for ( Map.Entry<Object, SortedSet<Event<Double>>> next : withLabels.entrySet() )
        {
            TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
            TimeSeries<Double> series = builder.setMetadata( timeSeries.getMetadata() )
                                               .addEvents( next.getValue() )
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
     * @return a filtered view of the input series
     * @throws NullPointerException if any input is null
     */

    public static <S, T> TimeSeries<T> transform( TimeSeries<S> timeSeries, Function<S, T> mapper )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( mapper );

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();

        builder.setMetadata( timeSeries.getMetadata() );

        for ( Event<S> event : timeSeries.getEvents() )
        {
            T transformed = mapper.apply( event.getValue() );

            if ( Objects.nonNull( transformed ) )
            {
                builder.addEvent( Event.of( event.getTime(), transformed ) );
            }
        }

        return builder.build();
    }

    /**
     * Transforms the input type to another type.
     * 
     * @param <L> the left type
     * @param <R> the right type
     * @param <P> the transformed left type
     * @param <Q> the transformed right type
     * @param input the input
     * @param transformer the transformer
     * @return the transformed type
     * @throws NullPointerException if either input is null
     */

    public static <L, R, P, Q> PoolOfPairs<P, Q> transform( PoolOfPairs<L, R> input,
                                                            Function<Pair<L, R>, Pair<P, Q>> transformer )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( transformer );

        PoolOfPairsBuilder<P, Q> builder = new PoolOfPairsBuilder<>();

        builder.setClimatology( input.getClimatology() )
               .setMetadata( input.getMetadata() );

        // Add the main series
        for ( TimeSeries<Pair<L, R>> next : input.get() )
        {
            builder.addTimeSeries( transform( next, transformer ) );
        }

        // Add the baseline series if available
        if ( input.hasBaseline() )
        {
            PoolOfPairs<L, R> baseline = input.getBaselineData();

            for ( TimeSeries<Pair<L, R>> nextBase : baseline.get() )
            {
                builder.addTimeSeriesForBaseline( transform( nextBase, transformer ) );
            }

            builder.setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
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

    public static <L, R> PoolOfPairs<L, R> filter( PoolOfPairs<L, R> input,
                                                   Predicate<Pair<L, R>> condition,
                                                   DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( condition );

        PoolOfPairsBuilder<L, R> builder = new PoolOfPairsBuilder<>();

        builder.setMetadata( input.getMetadata() );

        //Filter climatology as required
        if ( input.hasClimatology() )
        {
            VectorOfDoubles climatology = input.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( input.getClimatology(), applyToClimatology );
            }

            builder.setClimatology( climatology );
        }

        // Filter the main data
        for ( TimeSeries<Pair<L, R>> next : input.get() )
        {
            builder.addTimeSeries( filter( next, condition ) );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            PoolOfPairs<L, R> baseline = input.getBaselineData();

            for ( TimeSeries<Pair<L, R>> nextBase : baseline.get() )
            {
                builder.addTimeSeriesForBaseline( filter( nextBase, condition ) );
            }

            builder.setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
    }


    /**
     * Returns the subset of time-series where the condition is met. Applies to both the main pairs and any baseline 
     * pairs. Does not modify the metadata associated with the input.
     * 
     * @param <L> the type of left value
     * @param <R> the type of right value
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static <L, R> PoolOfPairs<L, R> filterPerSeries( PoolOfPairs<L, R> input,
                                                            Predicate<TimeSeries<Pair<L, R>>> condition,
                                                            DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input );

        Objects.requireNonNull( condition );

        PoolOfPairsBuilder<L, R> builder = new PoolOfPairsBuilder<>();

        builder.setMetadata( input.getMetadata() );

        //Filter climatology as required
        if ( input.hasClimatology() )
        {
            VectorOfDoubles climatology = input.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( input.getClimatology(), applyToClimatology );
            }

            builder.setClimatology( climatology );
        }

        // Filter the main data
        for ( TimeSeries<Pair<L, R>> next : input.get() )
        {
            if ( condition.test( next ) )
            {
                builder.addTimeSeries( next );
            }
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            PoolOfPairs<L, R> baseline = input.getBaselineData();

            for ( TimeSeries<Pair<L, R>> nextBase : baseline.get() )
            {
                if ( condition.test( nextBase ) )
                {
                    builder.addTimeSeriesForBaseline( nextBase );
                }
            }

            builder.setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
    }

    /**
     * Consolidates the input collection into one series.
     * 
     * @param <T> the time-series event value type
     * @param collectedSeries the collected series
     * @return the consolidated series
     * @throws NullPointerException if the consolidatedSeries is null
     */

    public static <T> TimeSeries<T> consolidate( Collection<TimeSeries<T>> collectedSeries )
    {
        Objects.requireNonNull( collectedSeries );

        // Empty series
        if ( collectedSeries.isEmpty() )
        {
            // TODO: restore empty timeseries capability after weeding out spots
            // where some kind of metadata can be put in.
            throw new IllegalStateException( "Cannot consolidate an empty timeseries" );
            //return TimeSeries.of();
        }
        // Singleton series
        else if ( collectedSeries.size() == 1 )
        {
            return collectedSeries.iterator().next();
        }

        TimeSeriesBuilder<T> builder = new TimeSeriesBuilder<>();

        for ( TimeSeries<T> next : collectedSeries )
        {
            builder.addEvents( next.getEvents() );
            builder.setMetadata( next.getMetadata() );
        }

        return builder.build();
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

        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        builder.setMetadata( metadata );

        String[] labs = null;

        if ( !labels.isEmpty() )
        {
            labs = labels.toArray( new String[labels.size()] );
        }

        for ( Map.Entry<Instant, List<Double>> next : ensembles.entrySet() )
        {
            Instant time = next.getKey();
            Ensemble value = Ensemble.of( next.getValue().stream().mapToDouble( Double::valueOf ).toArray(), labs );
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
            LOGGER.trace( "While snipping series {} to series {} with lower buffer {} and upper buffer {}, no events "
                          + "were discovered within the series to snip to. Returning the unsnipped series.",
                          toSnip,
                          snipTo,
                          lowerBuffer,
                          upperBuffer );

            return toSnip;
        }
        
        Instant lower = snipTo.getEvents().first().getTime();
        Instant upper = snipTo.getEvents().last().getTime();

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

        TimeSeriesBuilder<S> snippedSeries = new TimeSeriesBuilder<>();
        snippedSeries.setMetadata( toSnip.getMetadata() );
        for ( Event<S> next : toSnip.getEvents() )
        {
            Instant nextTime = next.getTime();

            if ( nextTime.compareTo( lower ) >= 0 && nextTime.compareTo( upper ) <= 0 )
            {
                snippedSeries.addEvent( next );
            }
        }

        TimeSeries<S> snipped = snippedSeries.build();

        if ( snipped.getEvents().isEmpty() )
        {
            LOGGER.trace( "While snipping series {} to series {} with lower buffer {} and upper buffer {}, no events "
                          + "were discovered within the series to snip that were within the bounds of the series to "
                          + "snip to.",
                          toSnip,
                          snipTo,
                          lowerBuffer,
                          upperBuffer );
        }

        return snipped;
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
            TimeSeriesBuilder<T> timeTransformed = new TimeSeriesBuilder<>();
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
     * Returns the reference times that fall within the right-closed time window.
     * 
     * @param referenceTimes the reference times
     * @param timeWindow the time window
     * @return the reference times that fall within the window
     */

    private static Map<ReferenceTimeType, Instant> filterReferenceTimes( Map<ReferenceTimeType, Instant> referenceTimes,
                                                                         TimeWindow timeWindow )
    {

        Map<ReferenceTimeType, Instant> returnMe = new TreeMap<>();

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
     * @returns true if the time is within (lowerExclusive, upperInclusive], otherwise false
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
     * @returns true if the duration is within (lowerExclusive, upperInclusive], otherwise false
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
     * Hidden constructor.
     */

    private TimeSeriesSlicer()
    {
    }

}
