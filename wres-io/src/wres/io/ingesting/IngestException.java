package wres.io.ingesting;

import java.io.Serial;

/**
 * <p>Indicates an issue occurred during ingest
 *
 * <p>The issue can be due to file reading, XML parsing, database connectivity,
 * database queries, value parsing, or otherwise.</p>
 *
 * <p> Use this when there are several underlying exceptions including SQL issues
 * that could be explained simply as an IngestException. If it is only a reading
 * or parsing issue, could instead throw an IOException.</p>
 */

public class IngestException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5909142292756194892L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public IngestException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * Creates an instance.
     * @param message the message
     */
    public IngestException( String message )
    {
        super( message );
    }
}
