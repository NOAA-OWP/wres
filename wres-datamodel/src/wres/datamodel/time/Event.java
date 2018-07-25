package wres.datamodel.time;

import java.time.Instant;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

/**
 * An event at a specific {@link Instant} on the timeline. Includes a default implementation that acts as a facade for
 * a {@link Pair} of {@link Instant} and the event value.
 * 
 * @param <T> the the type of event
 * @author james.brown@hydrosolved.com
 */

public class Event<T>
{

    /**
     * The pair.
     */

    private final Pair<Instant, T> pair;

    /**
     * Returns a default implementation of an {@link Event} that acts as a facade for a {@link Pair}.
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
     * Return the {@link Instant} associated with the occurrence.
     * 
     * @return the time
     */

    public Instant getTime()
    {
        return pair.getLeft();
    }

    /**
     * Returns the event value.
     * 
     * @return the event value
     */

    public T getValue()
    {
        return pair.getRight();
    }

    @Override
    public String toString()
    {
        return pair.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof Event ) )
        {
            return false;
        }
        return ( (Event) o ).pair.equals( pair );
    }

    @Override
    public int hashCode()
    {
        return pair.hashCode();
    }

    /**
     * Build an event with a time and value.
     * 
     * @param time the time
     * @param value the value
     * @throws NullPointerException if either input is null
     */

    private Event( Instant time, T value )
    {
        Objects.requireNonNull( time, "Specify a non-null time for the event." );
        Objects.requireNonNull( value, "Specify a non-null value for the event." );
        pair = Pair.of( time, value );
    }

}

