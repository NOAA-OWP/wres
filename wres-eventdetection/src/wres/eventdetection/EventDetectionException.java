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
     * @param cause the cause of the exception
     */

    public EventDetectionException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
