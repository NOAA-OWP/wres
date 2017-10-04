package wres.datamodel;

/**
 * A runtime exception associated with an incorrect metric output.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricOutputException extends MetricException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -1559780940246528464L;
    
    /**
     * Constructs an {@link MetricOutputException} with no message.
     */

    public MetricOutputException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricOutputException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricOutputException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricOutputException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricOutputException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
