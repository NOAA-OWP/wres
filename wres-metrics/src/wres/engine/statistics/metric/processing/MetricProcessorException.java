package wres.engine.statistics.metric.processing;

/**
 * A checked exception associated with a {@link MetricProcessor}.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricProcessorException extends Exception
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = 4291762819367673577L;

    /**
     * Constructs an {@link MetricProcessorException} with no message.
     */

    public MetricProcessorException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricProcessorException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricProcessorException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricProcessorException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricProcessorException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
