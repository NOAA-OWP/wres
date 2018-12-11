package wres.datamodel.time;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * An event at a specific {@link Instant} on the timeline. Additionally, an event may have a reference time, which is 
 * also represented by an {@link Instant}. The default reference time is equal to the event time.
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
     * The reference time.
     */

    private final Instant referenceTime;

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
        return new Event<>( time, time, value );
    }
    
    /**
     * Returns an {@link Event}.
     * 
     * @param <T> the event type
     * @param time the event time
     * @param referenceTime the optional reference time
     * @param value the event value
     * @return an event
     * @throws NullPointerException if the time or value are null
     */

    public static <T> Event<T> of( Instant referenceTime, Instant time, T value )
    {
        if( Objects.isNull( referenceTime ) )
        {
            return new Event<>( time, time, value );
        }
        
        return new Event<>( referenceTime, time, value );
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
     * Return the reference time as an {@link Instant}.
     * 
     * @return the reference time
     */

    public Instant getReferenceTime()
    {
        return this.referenceTime;
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

    /**
     * Returns the {@link Duration} between the {@link #referenceTime} and the {@link #eventTime} as 
     * <code>Duration.between( this.getReferenceTime(), this.getTime() )</code>.
     * 
     * @return the duration between the reference time and the event time
     */
    
    public Duration getDuration()
    {
        return Duration.between( this.getReferenceTime(), this.getTime() );
    }
    
    @Override
    public String toString()
    {
        return "(" + referenceTime + "," + eventTime + "," + value + ")";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof Event ) )
        {
            return false;
        }

        Event<?> inEvent = (Event<?>) o;

        return inEvent.value.equals( this.value ) && inEvent.eventTime.equals( this.eventTime )
               && inEvent.referenceTime.equals( this.referenceTime );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( this.value, this.eventTime, this.referenceTime );
    }

    /**
     * Build an event with a time, reference time and value.
     * 
     * @param referenceTime the reference time
     * @param eventTime the required time
     * @param event the required event
     * @throws NullPointerException if the eventTime is null or the event is null
     */

    private Event( Instant referenceTime, Instant eventTime, T event )
    {
        Objects.requireNonNull( eventTime, "Specify a non-null time for the event." );

        Objects.requireNonNull( event, "Specify a non-null value for the event." );
        
        Objects.requireNonNull( referenceTime, "Specify a non-null reference time." );

        this.value = event;       
        this.eventTime = eventTime;
        this.referenceTime = referenceTime;
    }

    /**
     * Compares this {@link Event} against the input {@link Event}, returning a negative integer, zero or positive 
     * integer as this {@link Event} is less than, equal to, or greater than the input {@link Event}. The comparison 
     * is made firstly on {@link Event#getReferenceTime()} and secondly on {@link Event#getTime()}.
     * 
     * @return a negative integer, zero or positive integer as this object is less than, equal to or greater than 
     *            the input
     */
    
    @Override
    public int compareTo( Event<T> o )
    {
        Objects.requireNonNull( o, "Specify a non-null input for comparison." );
        
        int returnMe = this.referenceTime.compareTo( o.referenceTime );
        
        if( returnMe != 0 )
        {
            return returnMe;
        }
            
        return this.eventTime.compareTo( o.eventTime );
    }

}

