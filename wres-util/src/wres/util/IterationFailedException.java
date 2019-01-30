package wres.util;

/**
 * Exception indicating that io's iteration could not continue due to some
 * condition outside the control of one of io module's Iterable or Iterator
 * instances.
 */

public class IterationFailedException extends RuntimeException
{
    public IterationFailedException( String message, Throwable t )
    {
        super( message, t );
    }

    public IterationFailedException( String message )
    {
        super( message );
    }
}
