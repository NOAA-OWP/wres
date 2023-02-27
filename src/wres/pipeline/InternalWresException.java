package wres.pipeline;

import java.io.Serial;

/**
 * A high-level exception indicating WRES software, configuration, or server was
 * responsible for failure (contrast UserInputException)
 */
public class InternalWresException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -4512919205972024314L;

    /**
     * @param message the message
     * @param cause the cause
     */

    public InternalWresException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * @param message the message
     */
    public InternalWresException( String message )
    {
        super( message );
    }
}
