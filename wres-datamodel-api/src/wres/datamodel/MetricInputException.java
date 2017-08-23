package wres.datamodel;

/**
 * A runtime exception associated with incorrect metric input.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricInputException extends MetricException
{

    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = -382138122319205095L;

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
