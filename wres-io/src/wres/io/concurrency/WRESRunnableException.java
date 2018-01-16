package wres.io.concurrency;

/**
 * A wrapper for exceptions thrown during a WRESRunnable.
 * Callable already deals with checked exceptions kindly, so we could move
 * toward WRESCallable instead of WRESRunnable and delete this class.
 */
public class WRESRunnableException extends RuntimeException
{
    public WRESRunnableException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
