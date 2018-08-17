package wres.control;

/**
 * A high-level exception indicating WRES software, configuration, or server was
 * responsible for failure (contrast UserInputException)
 */
public class InternalWresException extends RuntimeException
{
    public InternalWresException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
