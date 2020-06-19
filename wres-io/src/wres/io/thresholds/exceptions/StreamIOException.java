package wres.io.thresholds.exceptions;

/**
 * Used to bubble up exceptions encountered when loading data within a stream
 */

public class StreamIOException extends RuntimeException
{
    private static final long serialVersionUID = 4507239538782871616L;

    public StreamIOException(String message, Exception cause) {
        super(message, cause);
    }
}