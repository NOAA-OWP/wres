package wres.engine.statistics.metric;

/**
 * A checked exception associated with a metric parameter.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricParameterException extends Exception
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -905153232324061637L;
    
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
