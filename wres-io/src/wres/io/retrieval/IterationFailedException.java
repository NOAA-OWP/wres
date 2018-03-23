package wres.io.retrieval;

/**
 * Exception indicating that io's iteration could not continue due to some
 * condition outside the control of one of io module's Iterable or Iterator
 * instances.
 */

public class IterationFailedException extends RuntimeException
{
    IterationFailedException( String message, Throwable t )
    {
        super( message, t );
    }

    IterationFailedException( String message )
    {
        super( message );
    }
}
