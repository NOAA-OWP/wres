package wres.events.subscribe;

/**
 * An unchecked exception that indicates an unrecoverable failure in a subscriber that must be responded to promptly by 
 * closing the subscriber gracefully, wherever possible.
 * 
 * @author james.brown@hydrosolved.com
 */

public class UnrecoverableSubscriberException extends RuntimeException
{

    private static final long serialVersionUID = -2029793213220550948L;

    /**
     * Constructs a {@link UnrecoverableSubscriberException} with the specified message.
     * 
     * @param message the message.
     */

    public UnrecoverableSubscriberException( final String message )
    {
        super( message );
    }
    
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
