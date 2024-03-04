package wres.reading;

import java.io.Serial;

/**
 * For when we cannot proceed with even starting the ingest/reading due to catastrophic
 * failure during reading. For example, hashing the file failed, or communication with the
 * database failed while getting ready to ingest.
 */
public class PreReadException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5752710252185368485L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public PreReadException( String message, Exception cause )
    {
        super( message, cause );
    }

    /**
     * Creates an instance.
     * @param message the message
     */
    public PreReadException( String message )
    {
        super( message );
    }
}
