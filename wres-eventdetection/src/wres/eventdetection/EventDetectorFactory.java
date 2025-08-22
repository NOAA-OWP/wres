package wres.eventdetection;

import java.util.Objects;

import wres.config.components.EventDetectionMethod;
import wres.config.components.EventDetectionParameters;

/**
 * A factory class that generates {@link EventDetector} for detecting events in time-series data.
 *
 * @author James Brown
 */

public class EventDetectorFactory
{

    /**
     * Returns an {@link EventDetector} for the named {@link EventDetectionMethod}.
     *
     * @param method the method, required
     * @param parameters the event detection parameters
     * @return the detector
     * @throws NullPointerException if the method is null
     * @throws IllegalArgumentException if the method is unsupported
     */
    public static EventDetector getEventDetector( EventDetectionMethod method,
                                                  EventDetectionParameters parameters )
    {
        Objects.requireNonNull( method );
        Objects.requireNonNull( parameters );

        if ( method == EventDetectionMethod.REGINA_OGDEN
             || method == EventDetectionMethod.DEFAULT )
        {
            return ReginaOgdenEventDetector.of( parameters );
        }

        throw new IllegalArgumentException( "Unsupported event detection method: " + method + "." );
    }

    /**
     * Do not construct.
     */
    private EventDetectorFactory()
    {
    }
}
