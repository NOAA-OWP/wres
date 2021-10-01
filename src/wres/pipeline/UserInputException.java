package wres.pipeline;

/**
 * A high-level exception indicating that user input was responsible for failure.
 */
public class UserInputException extends RuntimeException
{
    private static final long serialVersionUID = -5808905633241832903L;

    public UserInputException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public UserInputException( String message )
    {
        super( message );
    }
}
