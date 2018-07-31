package wres.io.retrieval;

/**
 * Thrown when data retrieval fails.
 */
public class RetrievalFailedException extends RuntimeException
{
    public RetrievalFailedException( String message, Throwable cause )
    {
        super( message, cause );
    }

    public RetrievalFailedException( String message )
    {
        super( message );
    }
}
