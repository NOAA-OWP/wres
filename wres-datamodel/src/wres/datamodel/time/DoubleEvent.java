package wres.datamodel.time;

import java.time.Instant;
import java.util.Objects;

import org.apache.commons.math3.util.Precision;

/**
 * <p>A real-valued outcome at a specific {@link Instant} on the timeline. A specialization of an {@link Event} to
 * reduce memory overhead.
 * 
 * <p>TODO: remove this class when the JDK supports generics over primitive types.
 * 
 * @author James Brown
 */

public class DoubleEvent implements Event<Double>
{
    /** The event time. */
    private final Instant eventTime;

    /** The event value. */
    private final double value;

    /**
     * Returns an {@link DoubleEvent}.
     * 
     * @param time the event time
     * @param value the event value
     * @return an event
     * @throws NullPointerException if the time or value are null
     */

    public static DoubleEvent of( Instant time, double value )
    {
        return new DoubleEvent( time, value );
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

    public Double getValue()
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
        if ( ! ( o instanceof DoubleEvent inEvent ) )
        {
            return false;
        }

        return Precision.equalsIncludingNaN( inEvent.value, this.value, Precision.EPSILON )
               && inEvent.eventTime.equals( this.eventTime );
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

    private DoubleEvent( Instant eventTime, double event )
    {
        Objects.requireNonNull( eventTime, "Specify a non-null time for the event." );

        this.value = event;
        this.eventTime = eventTime;
    }

    /**
     * Compares this {@link DoubleEvent} against the input {@link DoubleEvent}, returning a negative integer, zero or 
     * positive integer as this {@link DoubleEvent} is less than, equal to, or greater than the input 
     * {@link DoubleEvent}.
     * 
     * @return a negative integer, zero or positive integer as this object is less than, equal to or greater than the 
     *            input
     */

    @Override
    public int compareTo( Event<Double> o )
    {
        int returnMe = this.getTime().compareTo( o.getTime() );

        if ( returnMe != 0 )
        {
            return returnMe;
        }

        // Consistent with equals
        if ( Precision.equalsIncludingNaN( o.getValue(), this.value, Precision.EPSILON ) )
        {
            return 0;
        }

        return -1;
    }

}

