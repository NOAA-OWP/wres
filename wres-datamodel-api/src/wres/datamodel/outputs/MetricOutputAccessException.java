package wres.datamodel.outputs;

/**
 * A checked exception that is thrown on attempting to access a {@link MetricOutput} that is not available.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricOutputAccessException extends Exception
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 96812230759775484L;

    /**
     * Constructs an {@link MetricOutputAccessException} with no message.
     */

    public MetricOutputAccessException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricOutputAccessException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricOutputAccessException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricOutputAccessException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricOutputAccessException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
