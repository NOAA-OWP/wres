package wres.events;

/**
 * Exception to throw when a statistics message could not be posted.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EvaluationEventException extends RuntimeException
{

    /**
     * Serialization id.
     */
    
    private static final long serialVersionUID = 2265382485793031701L;

    /**
     * Builds an exception with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public EvaluationEventException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
