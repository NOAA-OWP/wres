package wres.events;

import java.io.Serial;

/**
 * Exception to throw when the evaluation messaging fails in an unrecoverable way.
 * 
 * @author James Brown
 */

public class EvaluationFailedToCompleteException extends RuntimeException
{
    /**
     * Serialization id.
     */
    
    @Serial
    private static final long serialVersionUID = 2265382485793031701L;

    /**
     * Constructs a {@link EvaluationFailedToCompleteException} with the specified message.
     * 
     * @param message the message.
     */

    public EvaluationFailedToCompleteException( final String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link EvaluationFailedToCompleteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public EvaluationFailedToCompleteException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
