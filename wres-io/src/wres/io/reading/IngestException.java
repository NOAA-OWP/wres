package wres.io.reading;

import java.io.IOException;

/**
 * Indicates an issue occurred during ingest
 *
 * <br>
 *
 * The issue can be due to file reading, XML parsing, database connectivity,
 * database queries, value parsing, or otherwise.
 *
 * Use this when there are several underlying exceptions including SQL issues
 * that could be explained simply as an IngestException. If it is only a reading
 * or parsing issue, could instead throw an IOException.
 */

public class IngestException extends IOException
{
    public IngestException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
