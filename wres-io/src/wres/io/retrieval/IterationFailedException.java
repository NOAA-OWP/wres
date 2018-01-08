package wres.io.retrieval;

public class IterationFailedException extends RuntimeException
{
    public IterationFailedException( String message, Throwable t )
    {
        super( message, t );
    }
}
