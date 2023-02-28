package wres.pipeline;

import java.io.Serial;

/**
 * An exception representing that execution of a step failed.
 * Needed because Java 8 Function world does not
 * deal kindly with checked Exceptions.
 */
public class WresProcessingException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 6988169716259295343L;

    public WresProcessingException( String message )
    {
        super( message );
    }

    public WresProcessingException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
