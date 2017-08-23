package wres.datamodel;

/**
 * A base class for a runtime exception associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricException extends RuntimeException
{

    /**
     * Serial ID.
     */
    
    private static final long serialVersionUID = -9205464442064407973L;

    /**
     * Constructs an {@link MetricException} with no message.
     */

    public MetricException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
