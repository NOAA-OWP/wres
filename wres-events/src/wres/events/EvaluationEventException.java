package wres.events;

/**
 * Exception to throw when an evaluation event occurs. These events may be recoverable.
 * 
 * @author James Brown
 */

public class EvaluationEventException extends RuntimeException
{

    /**
     * Serialization id.
     */
    
    private static final long serialVersionUID = 2265382485793031701L;

    /**
     * Constructs a {@link EvaluationEventException} with the specified message.
     * 
     * @param message the message.
     */

    public EvaluationEventException( final String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link EvaluationEventException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public EvaluationEventException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
