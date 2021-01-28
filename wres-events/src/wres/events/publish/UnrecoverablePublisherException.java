package wres.events.publish;

/**
 * An unchecked exception that indicates an unrecoverable failure in a publisher that must be propagated.
 * 
 * @author james.brown@hydrosolved.com
 */

class UnrecoverablePublisherException extends RuntimeException
{

    private static final long serialVersionUID = 5655493413619097256L;

    /**
     * Constructs a {@link UnrecoverablePublisherException} with the specified message.
     * 
     * @param message the message.
     */

    public UnrecoverablePublisherException( final String message )
    {
        super( message );
    }
    
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
