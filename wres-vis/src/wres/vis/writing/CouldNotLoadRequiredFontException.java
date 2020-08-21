package wres.vis.writing;


public class CouldNotLoadRequiredFontException extends Exception
{
    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = 3189229577290445611L;

    /**
     * Constructs an {@link CouldNotLoadRequiredFontException} with no message.
     */

    public CouldNotLoadRequiredFontException()
    {
        super();
    }

    /**
     * Constructs a {@link CouldNotLoadRequiredFontException} with the specified message.
     * 
     * @param message the message.
     */

    public CouldNotLoadRequiredFontException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link CouldNotLoadRequiredFontException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public CouldNotLoadRequiredFontException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
