package wres.engine.statistics.metric.inputs;

/**
 * A runtime exception associated with incorrect metric input.
 * 
 * @author james.brown@hydrosolved.com
 */

@SuppressWarnings("serial")
public class MetricInputException extends RuntimeException
{

    /**
     * Constructs an {@link MetricInputException} with no message.
     */

    public MetricInputException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricInputException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricInputException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricInputException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricInputException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
