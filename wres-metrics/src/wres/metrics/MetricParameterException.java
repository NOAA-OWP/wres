package wres.metrics;

import java.io.Serial;

/**
 * An unchecked exception associated with a metric parameter.
 * 
 * @author James Brown
 */

public final class MetricParameterException extends RuntimeException
{
    /**
     * Serial identifier.
     */
    
    @Serial
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
