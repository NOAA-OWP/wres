package wres.eventsbroker;

/**
 * A runtime exception indicating a failure to start an embedded broker.
 * 
 * @author james.brown@hydrosolved.com
 */

public class CouldNotStartEmbeddedBrokerException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -812564660205671509L;

    /**
     * Constructs an {@link CouldNotStartEmbeddedBrokerException} with no message.
     */

    public CouldNotStartEmbeddedBrokerException()
    {
        super();
    }

    /**
     * Constructs a {@link CouldNotStartEmbeddedBrokerException} with the specified message.
     * 
     * @param message the message.
     */

    public CouldNotStartEmbeddedBrokerException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link CouldNotStartEmbeddedBrokerException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public CouldNotStartEmbeddedBrokerException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
