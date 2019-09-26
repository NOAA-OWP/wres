package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.Ensemble;
import wres.datamodel.Slicer;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

import org.apache.commons.lang3.tuple.Pair;

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
     * Failure to supply a non-null predicate.
     */

    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";

    /**
     * Failure to supply a non-null reference time type.
     */

    private static final String NULL_REFERENCE_TIME_TYPE = "Specify a non-null reference time type.";

    /**
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series.
     * 
     * @param <L> the type of left paired value
     * @param <R> the type of right paired value
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <L, R> Predicate<TimeSeries<Pair<L,R>>> anyOfLeftInTimeSeries( Predicate<L> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of left." );

        return times -> {

            // Iterate the times
            for ( Event<Pair<L,R>> next : times.getEvents() )
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

    public static <L, R> Predicate<TimeSeries<Pair<L,R>>> anyOfRightInTimeSeries( Predicate<R> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of right." );

        return times -> {

            // Iterate the times
            for ( Event<Pair<L,R>> next : times.getEvents() )
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

    public static <S> Predicate<TimeSeries<Pair<S,S>>>
            anyOfLeftAndAnyOfRightInTimeSeries( Predicate<S> predicate )
    {
        Objects.requireNonNull( predicate,
                                "Specify non-null input when slicing a time-series by any of left"
                                           + "and any of right." );

        return times -> {

            // Iterate the times
            boolean left = false;
            boolean right = false;

            for ( Event<Pair<S,S>> next : times.getEvents() )
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
     * Returns the unique {@link Duration} associated with the input time-series, where a {@link Duration} is the
     * difference between the {@link Event#getTime()} and the {@link TimeSeries#getReferenceTimes()} that corresponds
     * to the specified type.
     * 
     * @param <T> the type of event
     * @param timeSeries the time-series to search
     * @param type the reference time type
     * @return the durations
     * @throws NullPointerException if the input is null
     */

    public static <T> SortedSet<Duration> getDurations( List<TimeSeries<T>> timeSeries, ReferenceTimeType type )
    {
        Objects.requireNonNull( timeSeries );

        SortedSet<Duration> durations = new TreeSet<>();

        for ( TimeSeries<T> nextSeries : timeSeries )
        {
            if ( nextSeries.getReferenceTimes().containsKey( type ) )
            {
                Instant referenceTime = nextSeries.getReferenceTimes().get( type );

                for ( Event<T> next : nextSeries.getEvents() )
                {
                    durations.add( Duration.between( referenceTime, next.getTime() ) );
                }
            }
        }

        return Collections.unmodifiableSortedSet( durations );
    }

    /**
     * Returns the unique reference datetime {@link Instant} associated with the input time-series for a given type
     * 
     * @param <T> the type of event
     * @param timeSeries the time-series to search
     * @param type the reference time type
     * @return the reference datetimes
     * @throws NullPointerException if any input is null
     */

    public static <T> SortedSet<Instant> getReferenceTimes( List<TimeSeries<T>> timeSeries, ReferenceTimeType type )
    {
        Objects.requireNonNull( timeSeries );

        Objects.requireNonNull( type, NULL_REFERENCE_TIME_TYPE );

        SortedSet<Instant> referenceTimes = new TreeSet<>();

        for ( TimeSeries<T> nextSeries : timeSeries )
        {
            Instant next = nextSeries.getReferenceTimes().get( type );

            if ( Objects.nonNull( next ) )
            {
                referenceTimes.add( next );
            }
        }

        return Collections.unmodifiableSortedSet( referenceTimes );
    }

    /**
     * Filters the input time-series by the {@link Duration} associated with each value. Does not modify the metadata 
     * associated with the input.
     * 
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param duration the duration condition on which to slice
     * @param type the reference time type
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if any input is null
     */

    public static <T> List<Event<T>>
            filterByDuration( List<TimeSeries<T>> input, Predicate<Duration> duration, ReferenceTimeType type )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( duration, NULL_PREDICATE_EXCEPTION );

        Objects.requireNonNull( type, NULL_REFERENCE_TIME_TYPE );

        List<Event<T>> returnMe = new ArrayList<>();

        for ( TimeSeries<T> nextSeries : input )
        {
            if ( nextSeries.getReferenceTimes().containsKey( type ) )
            {
                Instant referenceTime = nextSeries.getReferenceTimes().get( type );

                for ( Event<T> nextEvent : nextSeries.getEvents() )
                {
                    Duration candidateDuration = Duration.between( referenceTime, nextEvent.getTime() );

                    if ( duration.test( candidateDuration ) )
                    {
                        returnMe.add( nextEvent );
                    }
                }
            }
        }

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Filters the input time-series by basis time. Applies to both the main pairs and any baseline pairs. Does not 
     * modify the metadata associated with the input.
     * 
     * @param <T> the type of time-series value
     * @param input the pairs to slice
     * @param referenceTime the reference time condition on which to slice
     * @param type the reference time type
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if any input is null
     */

    public static <T> List<TimeSeries<T>> filterByReferenceTime( List<TimeSeries<T>> input,
                                                                 Predicate<Instant> referenceTime,
                                                                 ReferenceTimeType type )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( referenceTime, NULL_PREDICATE_EXCEPTION );

        Objects.requireNonNull( type, NULL_REFERENCE_TIME_TYPE );

        //Add the filtered data
        return input.stream()
                    .filter( next -> next.getReferenceTimes().containsKey( type )
                                     && referenceTime.test( next.getReferenceTimes().get( type ) ) )
                    .collect( Collectors.collectingAndThen( Collectors.toList(),
                                                            Collections::unmodifiableList ) );
    }

    /**
     * Groups the input events according to the event valid time. An event falls within a group if its valid time falls 
     * within an interval that ends at a prescribed time and begins the specified duration before the end. The interval 
     * is right-closed. In other words, <code>(endsAt-duration,endsAt]</code> for each instant in <code>endsAt</code>.
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

        // Iterate the end times and group events whose times fall in (nextEnd-period,nextEnd]
        for ( Instant nextEnd : endsAt )
        {
            // Lower bound exclusive
            Instant nextStart = nextEnd.minus( period );

            for ( Event<T> nextEvent : events )
            {
                Instant eventTime = nextEvent.getTime();

                // Is event time within (start,nextEnd]?
                if ( eventTime.compareTo( nextEnd ) <= 0 && eventTime.compareTo( nextStart ) > 0 )
                {
                    SortedSet<Event<T>> nextGroup = grouped.get( nextEnd );

                    // Create a new group
                    if ( Objects.isNull( nextGroup ) )
                    {
                        nextGroup = new TreeSet<>();
                        grouped.put( nextEnd, nextGroup );
                    }

                    nextGroup.add( nextEvent );
                }
            }

        }

        return Collections.unmodifiableMap( grouped );
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
            return List.of( new TimeSeriesBuilder<Double>().addReferenceTimes( timeSeries.getReferenceTimes() )
                                                           .setTimeScale( timeSeries.getTimeScale() )
                                                           .build() );
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

        List<TimeSeries<Double>> returnMe = new ArrayList<>();

        for ( SortedSet<Event<Double>> next : membersByTime.values() )
        {
            TimeSeriesBuilder<Double> builder = new TimeSeriesBuilder<>();
            TimeSeries<Double> series = builder.addReferenceTimes( timeSeries.getReferenceTimes() )
                                               .setTimeScale( timeSeries.getTimeScale() )
                                               .addEvents( next )
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
        Map<ReferenceTimeType, Instant> referenceTimes = null;
        TimeScale timeScale = null;

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
            if ( Objects.isNull( referenceTimes ) )
            {
                referenceTimes = nextSeries.getReferenceTimes();
                timeScale = nextSeries.getTimeScale();
            }
            else if ( !nextSeries.getReferenceTimes().equals( referenceTimes ) )
            {
                throw new IllegalArgumentException( "One or more of the input series have different reference "
                                                    + "times, which is not allowed when composing them into an "
                                                    + "ensemble." );
            }
            // Unequal time scales
            else if ( !nextSeries.getTimeScale().equals( timeScale ) )
            {
                throw new IllegalArgumentException( "One or more of the input series have different time scales, "
                                                    + "which is not allowed when composing them into an "
                                                    + "ensemble." );
            }

        }

        return TimeSeriesSlicer.compose( ensembles, referenceTimes, timeScale, labels );
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

        builder.setTimeScale( timeSeries.getTimeScale() )
               .addReferenceTimes( timeSeries.getReferenceTimes() );

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

        builder.setTimeScale( timeSeries.getTimeScale() )
               .addReferenceTimes( timeSeries.getReferenceTimes() );

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
     * <p>Composes an ensemble time-series from a map of values.
     *
     * @param ensembles the ensemble members per time
     * @param referenceTimes the reference times
     * @param timeScale the time scale
     * @param labels the member labels
     * @return an ensemble times-series
     * @throws NullPointerException if any input is null
     */

    private static TimeSeries<Ensemble> compose( Map<Instant, List<Double>> ensembles,
                                                 Map<ReferenceTimeType, Instant> referenceTimes,
                                                 TimeScale timeScale,
                                                 SortedSet<String> labels )
    {
        Objects.requireNonNull( ensembles );

        Objects.requireNonNull( referenceTimes );

        Objects.requireNonNull( timeScale );

        Objects.requireNonNull( labels );

        TimeSeriesBuilder<Ensemble> builder = new TimeSeriesBuilder<>();
        builder.addReferenceTimes( referenceTimes ).setTimeScale( timeScale );
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
     * Hidden constructor.
     */

    private TimeSeriesSlicer()
    {
    }


}
