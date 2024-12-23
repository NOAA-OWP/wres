package wres.eventdetection;

/**
 * An exception encountered while performing event detection.
 *
 * @author James Brown
 */
public class EventDetectionException extends RuntimeException
{
    /**
     * Constructs a {@link EventDetectionException} with the specified message.
     *
     * @param message the message.
     */

    public EventDetectionException( String message )
    {
        super( message );
    }
}
