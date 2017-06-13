package wres.engine.statistics.metric;

/**
 * A runtime exception associated with a metric calculation.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricCalculationException extends RuntimeException
{

    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = 3189229577290445611L;

    /**
     * Constructs an {@link MetricCalculationException} with no message.
     */

    public MetricCalculationException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricCalculationException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricCalculationException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricCalculationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricCalculationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
