package wres.io.ingesting;

/**
 * Indicates an issue occurred during ingest
 *
 * <p>
 *
 * The issue can be due to file reading, XML parsing, database connectivity,
 * database queries, value parsing, or otherwise.
 *
 * </p>
 * <p>
 *
 * Use this when there are several underlying exceptions including SQL issues
 * that could be explained simply as an IngestException. If it is only a reading
 * or parsing issue, could instead throw an IOException.
 *
 * </p>
 */

public class IngestException extends RuntimeException
{
    private static final long serialVersionUID = -5909142292756194892L;

    public IngestException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public IngestException( String message )
    {
        super( message );
    }
}
