package wres.control;

/**
 * A high-level exception indicating that user input was responsible for failure.
 */
public class UserInputException extends RuntimeException
{
    UserInputException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
