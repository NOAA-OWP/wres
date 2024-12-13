package wres.eventdetection;

import java.util.Objects;
import wres.config.yaml.components.EventDetectionMethod;

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
     * @return the detector
     * @throws NullPointerException if the method is null
     * @throws IllegalArgumentException if the method is unsupported
     */
    public static EventDetector getEventDetector( EventDetectionMethod method )
    {
        Objects.requireNonNull( method );

        if ( method == EventDetectionMethod.REGINA_OGDEN
             || method == EventDetectionMethod.DEFAULT )
        {
            return ReginaOgdenEventDetector.of();
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
