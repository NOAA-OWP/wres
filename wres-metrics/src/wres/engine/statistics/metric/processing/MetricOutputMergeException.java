package wres.engine.statistics.metric.processing;

/**
 * A runtime exception that indicates a failure to merge results across successive calls
 * to a {@link MetricProcessor}, indicating a programming error.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricOutputMergeException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 6871453965441110878L;

    /**
     * Constructs an {@link MetricOutputMergeException} with no message.
     */

    public MetricOutputMergeException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricOutputMergeException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricOutputMergeException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricOutputMergeException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricOutputMergeException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
