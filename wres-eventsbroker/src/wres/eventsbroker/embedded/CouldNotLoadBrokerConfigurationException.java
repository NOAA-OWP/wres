package wres.eventsbroker.embedded;

/**
 * A runtime exception indicating a failure to load the configuration needed to connect to a broker.
 * 
 * @author james.brown@hydrosolved.com
 */

public class CouldNotLoadBrokerConfigurationException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 3339437560289832340L;

    /**
     * Constructs an {@link CouldNotLoadBrokerConfigurationException} with no message.
     */

    public CouldNotLoadBrokerConfigurationException()
    {
        super();
    }

    /**
     * Constructs a {@link CouldNotLoadBrokerConfigurationException} with the specified message.
     * 
     * @param message the message.
     */

    public CouldNotLoadBrokerConfigurationException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link CouldNotLoadBrokerConfigurationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public CouldNotLoadBrokerConfigurationException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
