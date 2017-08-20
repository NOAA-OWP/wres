package wres.datamodel;

/**
 * A checked exception to indicate that a subset of data is not available for a {@link MetricInput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricInputSliceException extends Exception
{

    /**
     * 
     */
    private static final long serialVersionUID = -3255052557259378343L;

    /**
     * Constructs an {@link MetricInputSliceException} with no message.
     */

    public MetricInputSliceException()
    {
        super();
    }

    /**
     * Constructs a {@link MetricInputSliceException} with the specified message.
     * 
     * @param message the message.
     */

    public MetricInputSliceException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link MetricInputSliceException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public MetricInputSliceException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
