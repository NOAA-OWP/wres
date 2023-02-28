package wres.io.writing.netcdf;

import java.io.Serial;

/**
 * An exception that indicates a cooordinate could not be found.
 */
public class CoordinateNotFoundException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 194816125215945933L;

    /**
     * Creates an instance.
     * @param message the message
     */
    public CoordinateNotFoundException( final String message )
    {
        super( message );
    }

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public CoordinateNotFoundException( final String message, Throwable cause )
    {
        super( message, cause );
    }

}
