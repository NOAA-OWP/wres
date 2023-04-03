package wres.io.reading;

import java.io.Serial;

/**
 * An unrecoverable exception that occurred when reading a source format.
 */

public class ReadException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 7571399162349184924L;

    /** 
     * @param message the message
     * @param cause the cause
     */
    public ReadException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * @param message the message
     */
    
    public ReadException( String message )
    {
        super( message );
    }
}
