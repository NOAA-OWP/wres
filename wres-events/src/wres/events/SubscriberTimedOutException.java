package wres.events;

/**
 * Exception to throw when consumption has timed out. This corresponds to HTTP status code 408.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SubscriberTimedOutException extends RuntimeException
{

    /**
     * Serialization id.
     */
    
    private static final long serialVersionUID = 2265382485793031701L;

    /**
     * Constructs a {@link SubscriberTimedOutException} with the specified message.
     * 
     * @param message the message.
     */

    public SubscriberTimedOutException( final String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link SubscriberTimedOutException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public SubscriberTimedOutException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
