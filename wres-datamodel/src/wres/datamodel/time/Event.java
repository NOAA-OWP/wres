package wres.datamodel.time;

import java.time.Instant;

/**
 * <p>An outcome or value at a specific {@link Instant} on the timeline.
 * 
 * <p>TODO: collapse this interface and the default implementation {@link DefaultEvent} when generic specialization is
 * supported by the JDK so that the {@link DoubleEvent} is no longer required for performance.
 * 
 * @param <T> the type of event
 * @author James Brown
 */

public interface Event<T> extends Comparable<Event<T>>
{
    /**
     * Return the event time as an {@link Instant}.
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
     * Convenience method to provide a default instance.
     * 
     * @param <T> the event value type
     * @param time the time
     * @param value the value
     * @return the event
     */
    
    static <T> Event<T> of( Instant time, T value )
    {
        return DefaultEvent.of( time, value );
    }
}

