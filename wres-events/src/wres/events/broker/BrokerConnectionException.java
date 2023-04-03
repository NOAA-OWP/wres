package wres.events.broker;

import java.io.Serial;

/**
 * A runtime exception indicating a failure to load connect to a broker.
 * 
 * @author James Brown
 */

public class BrokerConnectionException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = 3339437560289832340L;

    /**
     * Constructs a {@link BrokerConnectionException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public BrokerConnectionException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
