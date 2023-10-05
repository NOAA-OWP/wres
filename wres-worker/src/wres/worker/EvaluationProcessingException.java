package wres.worker;

import java.io.Serial;

/**
 * <p>Indicates an issue occurred during processing of an evaluation
 *
 */

public class EvaluationProcessingException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5909142292756194892L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public EvaluationProcessingException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * Creates an instance.
     * @param message the message
     */
    public EvaluationProcessingException( String message )
    {
        super( message );
    }
}
