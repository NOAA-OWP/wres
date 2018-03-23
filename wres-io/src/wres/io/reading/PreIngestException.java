package wres.io.reading;

/**
 * For when we cannot proceed with even starting the ingest due to catastrophic
 * failure. For example, hashing the file failed, or communication with the
 * database failed while getting ready to ingest.
 */
public class PreIngestException extends RuntimeException
{
    PreIngestException( String message, Exception cause )
    {
        super( message, cause );
    }

    PreIngestException( String message )
    {
        super( message );
    }
}
