package wres.eventsbroker;

/**
 * A runtime exception indicating a failure to load connect to a broker.
 * 
 * @author james.brown@hydrosolved.com
 */

public class BrokerConnectionException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 3339437560289832340L;

    /**
     * Constructs an {@link BrokerConnectionException} with no message.
     */

    public BrokerConnectionException()
    {
        super();
    }

    /**
     * Constructs a {@link BrokerConnectionException} with the specified message.
     * 
     * @param message the message.
     */

    public BrokerConnectionException( final String message )
    {
        super( message );
    }

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
