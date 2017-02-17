/**
 * 
 */
package wres.configcontrol.config.project;

/**
 * A runtime exception associated with incorrect configuration.
 * 
 * @author james.brown@hydrosolved.com
 */

@SuppressWarnings("serial")
public class ConfigurationException extends RuntimeException
{

    /**
     * Constructs an {@link ConfigurationException} with no message.
     */

    public ConfigurationException()
    {
        super();
    }

    /**
     * Constructs a {@link ConfigurationException} with the specified message.
     * 
     * @param message the message.
     */

    public ConfigurationException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link ConfigurationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public ConfigurationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
