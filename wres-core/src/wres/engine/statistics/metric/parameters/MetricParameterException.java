/**
 * 
 */
package wres.engine.statistics.metric.parameters;

/**
 * A runtime exception associated with incorrect configuration.
 * 
 * @author james.brown@hydrosolved.com
 */

@SuppressWarnings("serial")
public class MetricParameterException extends RuntimeException
{

    /**
     * Constructs an {@link MetricParameterException} with no message.
     */

    public MetricParameterException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricParameterException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricParameterException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricParameterException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricParameterException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
