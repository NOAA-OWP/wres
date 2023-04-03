package wres.events.subscribe;

import java.io.Serial;

/**
 * Exception to throw when consumption has timed out. This corresponds to HTTP status code 408.
 * 
 * @author James Brown
 */

public class SubscriberTimedOutException extends RuntimeException
{
    /**
     * Serialization id.
     */
    
    @Serial
    private static final long serialVersionUID = 2265382485793031701L;

    /**
     * Constructs a {@link SubscriberTimedOutException} with the specified message.
     * 
     * @param message the message.
     */

    public SubscriberTimedOutException( final String message )
    {
        super( message );
    }
}
