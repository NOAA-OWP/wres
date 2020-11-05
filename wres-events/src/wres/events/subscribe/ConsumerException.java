package wres.events.subscribe;

/**
 * Exception to throw when an evaluation message could not be consumed for any reason.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ConsumerException extends RuntimeException
{

    /**
     * Serialization id.
     */
    
    private static final long serialVersionUID = -1604366016990242665L;

    /**
     * Constructs a {@link ConsumerException} with the specified message.
     * 
     * @param message the message.
     */

    public ConsumerException( String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link ConsumerException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public ConsumerException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
