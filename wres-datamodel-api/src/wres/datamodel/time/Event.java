package wres.datamodel.time;

import java.time.Instant;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

/**
 * An event at a specific {@link Instant} on the timeline. Includes a default implementation that acts as a facade for
 * a {@link Pair} of {@link Instant} and the event value.
 * 
 * @param <T> the the type of event
 * @version 0.1
 * @since 0.4
 * @author james.brown@hydrosolved.com
 */

public interface Event<T>
{

    /**
     * Return the {@link Instant} associated with the occurrence.
     * 
     * @return the time
     */

    Instant getTime();

    /**
     * Returns the event value.
     * 
     * @return the event value
     */

    T getValue();

    /**
     * Returns a default implementation of an {@link Event} that acts as a facade for a {@link Pair}.
     * 
     * @param <T> the event type
     * @param time the event time
     * @param value the event value
     * @return an event
     * @throws NullPointerException if the time or value are null
     */

    static <T> Event<T> of( Instant time, T value )
    {
        final class DefaultEvent implements Event<T>
        {
            /**
             * The pair.
             */

            Pair<Instant, T> pair;

            /**
             * Build an event with a time and value.
             * 
             * @param time the time
             * @param value the value
             * @throws NullPointerException if either input is null
             */

            public DefaultEvent( Instant time, T value )
            {
                Objects.requireNonNull( time, "Specify a non-null time for the event." );
                Objects.requireNonNull( value, "Specify a non-null value for the event." );
                pair = Pair.of( time, value );
            }

            @Override
            public Instant getTime()
            {
                return pair.getLeft();
            }

            @Override
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
                if ( ! ( o instanceof DefaultEvent ) )
                {
                    return false;
                }
                return ( (DefaultEvent) o ).pair.equals( pair );
            }

            @Override
            public int hashCode()
            {
                return pair.hashCode();
            }
        }
        return new DefaultEvent( time, value );
    }

}

