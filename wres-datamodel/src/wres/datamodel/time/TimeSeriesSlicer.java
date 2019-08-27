package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import wres.datamodel.Slicer;
import wres.datamodel.sampledata.pairs.Pair;

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
     * <p>Composes the input predicate as applying to the left side of any paired value within a time-series.
     * 
     * @param <S> the type of left paired value
     * @param <T> the type of pair
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <S, T extends Pair<S, ?>> Predicate<TimeSeries<T>> anyOfLeftInTimeSeries( Predicate<S> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of left." );

        return times -> {

            // Iterate the times
            for ( Event<T> next : times.getEvents() )
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
     * @param <S> the type of right paired value
     * @param <T> the type of pair
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <S, T extends Pair<?, S>> Predicate<TimeSeries<T>> anyOfRightInTimeSeries( Predicate<S> predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing a time-series by any of right." );

        return times -> {

            // Iterate the times
            for ( Event<T> next : times.getEvents() )
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
     * @param <T> the type of pair
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static <S, T extends Pair<S, S>> Predicate<TimeSeries<T>>
            anyOfLeftAndAnyOfRightInTimeSeries( Predicate<S> predicate )
    {
        Objects.requireNonNull( predicate,
                                "Specify non-null input when slicing a time-series by any of left"
                                           + "and any of right." );

        return times -> {

            // Iterate the times
            boolean left = false;
            boolean right = false;
            
            for ( Event<T> next : times.getEvents() )
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
                
                if( left && right )
                {
                    return true;
                }
            }

            return false;
        };
    }

    /**
     * Returns the unique {@link Duration} associated with the input time-series, where a {@link Duration} is the
     * difference between the {@link Event#getTime()} and the {@link TimeSeries#getReferenceTime()}.
     * 
     * @param <T> the type of event
     * @param timeSeries the time-series to search
     * @return the durations
     * @throws NullPointerException if the input is null
     */
    
    public static <T> SortedSet<Duration> getDurations( List<TimeSeries<T>> timeSeries )
    {
        Objects.requireNonNull( timeSeries );
        
        SortedSet<Duration> durations = new TreeSet<>();
        
        for( TimeSeries<T> nextSeries : timeSeries )
        {
            for( Event<T> next : nextSeries.getEvents() )
            {
                durations.add( Duration.between( nextSeries.getReferenceTime(), next.getTime() ) );
            }
        }
        
        return Collections.unmodifiableSortedSet( durations );
    }

    /**
     * Returns the unique reference datetime {@link Instant} associated with the input time-series.
     * 
     * @param <T> the type of event
     * @param timeSeries the time-series to search
     * @return the reference datetimes
     * @throws NullPointerException if the input is null
     */
    
    public static <T> SortedSet<Instant> getReferenceTimes( List<TimeSeries<T>> timeSeries )
    {
        Objects.requireNonNull( timeSeries );
        
        SortedSet<Instant> referenceTimes = timeSeries.stream()
                                                      .map( TimeSeries::getReferenceTime )
                                                      .collect( Collectors.toCollection( TreeSet::new ) );
    
        return Collections.unmodifiableSortedSet( referenceTimes );
    }
    
    /**
     * Filters the input time-series by the {@link Duration} associated with each value. Does not modify the metadata 
     * associated with the input.
     * 
     * @param <T> the type of time-series data
     * @param input the input to slice
     * @param duration the duration condition on which to slice
     * @return the subset of the input that meets the condition
     * @throws NullPointerException if either the input or condition is null
     */
    
    public static <T> List<Event<T>> filterByDuration( List<TimeSeries<T>> input, Predicate<Duration> duration )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
    
        Objects.requireNonNull( duration, NULL_PREDICATE_EXCEPTION );
    
        List<Event<T>> returnMe = new ArrayList<>();
    
        for ( TimeSeries<T> nextSeries : input )
        {
            for ( Event<T> nextEvent : nextSeries.getEvents() )
            {
                Duration candidateDuration = Duration.between( nextSeries.getReferenceTime(), nextEvent.getTime() );

                if ( duration.test( candidateDuration ) )
                {
                    returnMe.add( nextEvent );
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
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */
    
    public static <T> List<TimeSeries<T>> filterByReferenceTime( List<TimeSeries<T>> input,
                                                                 Predicate<Instant> referenceTime )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );
    
        Objects.requireNonNull( referenceTime, NULL_PREDICATE_EXCEPTION );
    
        //Add the filtered data
        return input.stream()
                    .filter( next -> referenceTime.test( next.getReferenceTime() ) )
                    .collect( Collectors.collectingAndThen( Collectors.toList(),
                                                            Collections::unmodifiableList ) );
    }
    
    /**
     * Hidden constructor.
     */

    private TimeSeriesSlicer()
    {
    }


}
