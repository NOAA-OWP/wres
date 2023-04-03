package wres.events.subscribe;

import java.io.Serial;

/**
 * An unchecked exception that indicates an unrecoverable failure in a subscriber that must be propagated.
 * 
 * @author James Brown
 */

public class UnrecoverableSubscriberException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -2029793213220550948L;

    /**
     * Builds a {@link UnrecoverableSubscriberException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public UnrecoverableSubscriberException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
