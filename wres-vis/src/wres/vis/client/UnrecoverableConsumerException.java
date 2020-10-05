package wres.vis.client;

/**
 * A checked exception that indicates an unrecoverable failure in a consumer that must be responded to promptly.
 * 
 * @author james.brown@hydrosolved.com
 */

public class UnrecoverableConsumerException extends Exception
{

    private static final long serialVersionUID = -2029793213220550948L;

    /**
     * Constructs a {@link UnrecoverableConsumerException} with the specified message.
     * 
     * @param message the message.
     */

    public UnrecoverableConsumerException( final String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link UnrecoverableConsumerException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public UnrecoverableConsumerException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
