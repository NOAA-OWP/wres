package wres.engine.statistics.metric.config;

import wres.config.ProjectConfigException;

/**
 * A checked exception associated with a metric calculation.
 * 
 * TODO: consider extending {@link ProjectConfigException}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricConfigurationException extends Exception
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = 6790246291277821024L;
    
    /**
     * Constructs an {@link MetricConfigurationException} with no message.
     */

    public MetricConfigurationException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricConfigurationException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricConfigurationException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricConfigurationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricConfigurationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
