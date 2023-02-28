package wres.io.writing;

/**
 * A runtime exception associated with writing metric outputs.
 * 
 * @author James Brown
 */

public class WriteException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -8379791452118960970L;

    /**
     * Constructs an {@link WriteException} with no message.
     */

    public WriteException()
    {
        super();
    }

    /**
     * Constructs a {@link WriteException} with the specified message.
     * 
     * @param message the message.
     */

    public WriteException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link WriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public WriteException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
