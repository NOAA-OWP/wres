package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeSet;

import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * A helper class for time-series.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
class TimeSeriesHelper
{

    /**
     * Error message denoting attempt to modify an immutable time-series via an iterator.
     */

    static final String UNSUPPORTED_MODIFICATION = "While attempting to modify an immutable time-series.";

    /**
     * Helper method that adjusts the earliest and latest basis times of the {@link TimeWindow} associated with the 
     * input {@link Metadata} when iterating over the atomic time-series by basis time.
     * 
     * @param input the input metadata
     * @param earliestTime the earliest basis time for the new metadata
     * @param latestTime the latest basis time for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static Metadata getBasisTimeAdjustedMetadata( Metadata input, Instant earliestTime, Instant latestTime )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        Metadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = DefaultMetadataFactory.getInstance()
                                             .getMetadata( returnMe,
                                                           TimeWindow.of( earliestTime,
                                                                          latestTime,
                                                                          current.getReferenceTime(),
                                                                          current.getEarliestLeadTime(),
                                                                          current.getLatestLeadTime() ) );
        }
        return returnMe;
    }

    /**
     * Helper method that adjusts the earliest and latest durations of the {@link TimeWindow} associated with the input
     * {@link Metadata} when iterating over the atomic time-series by duration.
     * 
     * @param input the input metadata
     * @param earliestDuration the earliest duration for the new metadata
     * @param latestDuration the latest duration for the new metadata
     * @return the adjusted metadata
     * @throws NullPointerException if any of the inputs are null
     */

    static Metadata getDurationAdjustedMetadata( Metadata input, Duration earliestDuration, Duration latestDuration )
    {
        //Test the input only, as the others are tested on construction
        Objects.requireNonNull( "Specify non-null input for the current metadata." );
        Metadata returnMe = input;
        if ( input.hasTimeWindow() )
        {
            TimeWindow current = input.getTimeWindow();
            returnMe = DefaultMetadataFactory.getInstance()
                                             .getMetadata( returnMe,
                                                           TimeWindow.of( current.getEarliestTime(),
                                                                          current.getLatestTime(),
                                                                          current.getReferenceTime(),
                                                                          earliestDuration,
                                                                          latestDuration ) );
        }
        
        return returnMe;
    }

    /**
     * Returns the earliest basis time.
     * 
     * @param basisTimes the basis times
     * @return the earliest basis time
     */

    static Instant getEarliestBasisTime( List<Instant> basisTimes )
    {
        if( basisTimes.isEmpty() )
        {
            return Instant.MIN;
        }
        else if ( basisTimes.size() == 1 )
        {
            return ( basisTimes ).iterator().next();
        }
        
        return new TreeSet<>( basisTimes ).first();
    }

    /**
     * Returns the latest basis time.
     * 
     * @param basisTimes the basis times
     * @return the latest basis time
     */

    static Instant getLatestBasisTime( List<Instant> basisTimes )
    {
        if( basisTimes.isEmpty() )
        {
            return Instant.MAX;
        }
        else if ( basisTimes.size() == 1 )
        {
            return ( basisTimes ).iterator().next();
        }
        
        return new TreeSet<>( basisTimes ).last();
    }    
    
    /**
     * Returns a string representation of the {@link TimeSeries}.
     * @param <T> the type of time-series
     * @param timeSeries
     * @return a string representation
     */

    static <T> String toString( TimeSeries<T> timeSeries )
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        if ( timeSeries.hasMultipleTimeSeries() )
        {
            for ( TimeSeries<T> next : timeSeries.basisTimeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        else
        {
            for ( Event<T> next : timeSeries.timeIterator() )
            {
                joiner.add( next.toString() );
            }
        }
        return joiner.toString();
    }

    /**
     * Unwraps a list of lists into the atomic contents.
     * 
     * @param the listed type
     * @param input the input
     * @return the atomic content
     */

    static <T> List<T> unwrap( List<Event<List<Event<T>>>> input )
    {
        List<T> returnMe = new ArrayList<>();
        for ( Event<List<Event<T>>> nextSeries : input )
        {
            for ( Event<T> nextItem : nextSeries.getValue() )
            {
                if ( Objects.nonNull( nextItem ) )
                {
                    returnMe.add( nextItem.getValue() );
                }
            }
        }
        return returnMe;
    }

    /**
     * Renders all lists in the input to immutable types.
     * 
     * @param input the input with possibly mutable lists
     * @return the input with immutable lists
     * @throws MetricInputException if the input is null or any items in the list are null
     */

    static <T> List<Event<List<Event<T>>>> getImmutableTimeSeries( List<Event<List<Event<T>>>> input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify a non-null list of pairs to render immutable." );
        }
        List<Event<List<Event<T>>>> returnMe = new ArrayList<>();
        for ( Event<List<Event<T>>> nextSeries : input )
        {
            if ( Objects.isNull( nextSeries ) )
            {
                throw new MetricInputException( "Cannot build a time-series with one or more null time-series." );
            }
            List<Event<T>> nextList = new ArrayList<>();
            nextList.addAll( nextSeries.getValue() );
            if ( nextList.stream().anyMatch( Objects::isNull ) )
            {
                throw new MetricInputException( "Cannot build a time-series with one or more null events." );
            }
            returnMe.add( Event.of( nextSeries.getTime(), Collections.unmodifiableList( nextList ) ) );
        }
        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Sorts the input in time order.
     * 
     * @param input the unsorted input
     * @return the sorted input in time order
     * @throws MetricInputException if the input is null or any items in the list are null
     */

    static <T> List<Event<List<Event<T>>>> sort( List<Event<List<Event<T>>>> input )
    {
        if ( Objects.isNull( input ) )
        {
            throw new MetricInputException( "Specify a non-null list of pairs to sort." );
        }
        List<Event<List<Event<T>>>> returnMe = new ArrayList<>();
        for ( Event<List<Event<T>>> nextSeries : input )
        {
            if ( Objects.isNull( nextSeries ) )
            {
                throw new MetricInputException( "Cannot sort a time-series with one or more null time-series." );
            }
            List<Event<T>> nextList = new ArrayList<>();
            nextList.addAll( nextSeries.getValue() );
            if ( nextList.stream().anyMatch( Objects::isNull ) )
            {
                throw new MetricInputException( "Cannot sort a time-series with one or more null events." );
            }
            // Sort by inner time
            nextList.sort( TimeSeriesHelper::compareByTime );
            
            returnMe.add( Event.of( nextSeries.getTime(), nextList ) );
        }
        
        // Sort by outer time
        returnMe.sort( TimeSeriesHelper::compareByTime );
        
        return returnMe;
    }    
    
    /**
     * Compares two events by time.
     * 
     * @param left the left event
     * @param right the right event
     * @return a negative integer, zero, or a positive integer as the left event is less than, equal to, or greater 
     *            than the right event.
     */
    
    private static <T> int compareByTime( Event<T> left, Event<T> right )
    {
        return left.getTime().compareTo( right.getTime() );
    }    
    
    /**
     * Prevent construction.
     */

    private TimeSeriesHelper()
    {
    }

}
