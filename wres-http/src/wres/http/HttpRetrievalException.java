package wres.http;

import java.io.Serial;

/**
 * <p>Indicates an issue occurred during a http request
 */

public class HttpRetrievalException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -5909142292756194892L;

    /**
     * Creates an instance.
     * @param message the message
     * @param cause the cause
     */
    public HttpRetrievalException( String message, Throwable cause )
    {
        super( message, cause );
    }

    /**
     * Creates an instance.
     * @param message the message
     */
    public HttpRetrievalException( String message )
    {
        super( message );
    }
}
