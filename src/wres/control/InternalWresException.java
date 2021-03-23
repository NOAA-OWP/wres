package wres.control;

/**
 * A high-level exception indicating WRES software, configuration, or server was
 * responsible for failure (contrast UserInputException)
 */
public class InternalWresException extends RuntimeException
{
    private static final long serialVersionUID = -4512919205972024314L;

    public InternalWresException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public InternalWresException( String message )
    {
        super( message );
    }
}
