package wres.vis.writing;

/**
 * Exception that indicates a required font could not be loaded.
 */

class CouldNotLoadRequiredFontException extends RuntimeException
{
    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = 3189229577290445611L;

    /**
     * Constructs an {@link CouldNotLoadRequiredFontException} with no message.
     */

    CouldNotLoadRequiredFontException()
    {
        super();
    }

    /**
     * Constructs a {@link CouldNotLoadRequiredFontException} with the specified message.
     * 
     * @param message the message.
     */

    CouldNotLoadRequiredFontException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link CouldNotLoadRequiredFontException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    CouldNotLoadRequiredFontException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
