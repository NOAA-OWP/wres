package wres.events.subscribe;

/**
 * An unchecked exception that indicates an unrecoverable failure in an evaluation that must be propagated.
 * 
 * @author james.brown@hydrosolved.com
 */

public class UnrecoverableEvaluationException extends RuntimeException
{

    private static final long serialVersionUID = 5046708759822468416L;

    /**
     * Constructs a {@link UnrecoverableEvaluationException} with the specified message.
     * 
     * @param message the message.
     */

    public UnrecoverableEvaluationException( final String message )
    {
        super( message );
    }
    
    /**
     * Builds a {@link UnrecoverableEvaluationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public UnrecoverableEvaluationException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
