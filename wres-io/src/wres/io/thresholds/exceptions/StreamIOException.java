package wres.io.thresholds.exceptions;

import java.io.Serial;

/**
 * Used to bubble up exceptions encountered when loading data within a stream
 */
public class StreamIOException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 4507239538782871616L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public StreamIOException( String message, Exception cause )
    {
        super( message, cause );
    }
}