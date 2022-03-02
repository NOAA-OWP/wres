package wres.vis.charts;

/**
 * Exception that indicates a required font could not be loaded.
 */

class FontLoadingException extends RuntimeException
{
    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = 3189229577290445611L;

    /**
     * Constructs an {@link FontLoadingException} with no message.
     */

    FontLoadingException()
    {
        super();
    }

    /**
     * Constructs a {@link FontLoadingException} with the specified message.
     * 
     * @param message the message.
     */

    FontLoadingException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link FontLoadingException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    FontLoadingException( final String message, final Throwable cause )
    {
        super( message, cause );
    }
}
