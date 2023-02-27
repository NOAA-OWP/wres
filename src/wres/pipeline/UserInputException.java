package wres.pipeline;

import java.io.Serial;

/**
 * A high-level exception indicating that user input was responsible for failure.
 */
public class UserInputException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5808905633241832903L;

    /**
     * @param message the message
     * @param cause the cause
     */
    public UserInputException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * @param message the message
     */
    public UserInputException( String message )
    {
        super( message );
    }
}
