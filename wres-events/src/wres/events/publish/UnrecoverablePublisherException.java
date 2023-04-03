package wres.events.publish;

import java.io.Serial;

/**
 * An unchecked exception that indicates an unrecoverable failure in a publisher that must be propagated.
 * 
 * @author James Brown
 */

class UnrecoverablePublisherException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 5655493413619097256L;

    /**
     * Builds a {@link UnrecoverablePublisherException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public UnrecoverablePublisherException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
