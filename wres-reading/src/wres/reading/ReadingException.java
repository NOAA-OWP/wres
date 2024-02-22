package wres.reading;

import java.io.Serial;

/**
 * For when we cannot proceed with even starting the ingest due to catastrophic
 * failure during reading. For example, hashing the file failed, or communication with the
 * database failed while getting ready to ingest.
 */
public class ReadingException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5752710252185368485L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public ReadingException( String message, Exception cause )
    {
        super( message, cause );
    }

    /**
     * Creates an instance.
     * @param message the message
     */
    public ReadingException( String message )
    {
        super( message );
    }
}
