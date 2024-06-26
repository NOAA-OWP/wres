package wres.datamodel.time;

import java.time.Instant;
import java.util.Objects;

/**
 * Default implementation of an {@link Event}.
 * 
 * @param <T> the type of event
 * @author James Brown
 */

public class DefaultEvent<T> implements Event<T>
{
    /** The event time. */
    private final Instant eventTime;

    /** The event value. */
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
        return new DefaultEvent<>( time, value );
    }

    /**
     * Return the event time as an {@link Instant}.
     * 
     * @return the time
     */

    @Override
    public Instant getTime()
    {
        return this.eventTime;
    }

    /**
     * Returns the event value.
     * 
     * @return the event value
     */

    @Override
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
        if ( ! ( o instanceof DefaultEvent<?> inEvent ) )
        {
            return false;
        }

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

    private DefaultEvent( Instant eventTime, T event )
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