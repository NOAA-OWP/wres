package wres.datamodel.time;

import java.time.Instant;
import java.util.Objects;

/**
 * An outcome or value at a specific {@link Instant} on the timeline.
 * 
 * @param <T> the the type of event
 * @author james.brown@hydrosolved.com
 */

public class Event<T> implements Comparable<Event<T>>
{

    /**
     * The event time.
     */

    private final Instant eventTime;

    /**
     * The event.
     */

    private final T value;

    /**
     * Returns an {@link Event}.
     * 
     * @param <T> the event type
     * @param time the event time
     * @param value the event value
     * @return an event
     * @throws NullPointerException if the time or value are null
     */

    public static <T> Event<T> of( Instant time, T value )
    {
        return new Event<>( time, value );
    }

    /**
     * Return the event time as an {@link Instant}.
     * 
     * @return the time
     */

    public Instant getTime()
    {
        return this.eventTime;
    }

    /**
     * Returns the event value.
     * 
     * @return the event value
     */

    public T getValue()
    {
        return this.value;
    }
    
    @Override
    public String toString()
    {
        return "(" + eventTime + "," + value + ")";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof Event ) )
        {
            return false;
        }

        Event<?> inEvent = (Event<?>) o;

        return inEvent.value.equals( this.value ) && inEvent.eventTime.equals( this.eventTime );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.value, this.eventTime );
    }

    /**
     * Build an event with a time, reference time and value.
     * 
     * @param eventTime the required time
     * @param event the required event
     * @throws NullPointerException if the eventTime is null or the event is null
     */

    private Event( Instant eventTime, T event )
    {
        Objects.requireNonNull( eventTime, "Specify a non-null time for the event." );

        Objects.requireNonNull( event, "Specify a non-null value for the event." );

        this.value = event;       
        this.eventTime = eventTime;
    }

    /**
     * Compares this {@link Event} against the input {@link Event}, returning a negative integer, zero or positive 
     * integer as this {@link Event} is less than, equal to, or greater than the input {@link Event}.
     * 
     * @return a negative integer, zero or positive integer as this object is less than, equal to or greater than 
     *            the input
     */
    
    @Override
    public int compareTo( Event<T> o )
    {
        Objects.requireNonNull( o, "Specify a non-null input for comparison." );
        
        int returnMe = this.getTime().compareTo( o.getTime() );        
        
        if( returnMe != 0 )
        {
            return returnMe;
        }
        
        // Consistent with equals
        if( o.getValue().equals( this.getValue() ) )
        {
            return 0;
        }
        
        return -1;
    }

}

