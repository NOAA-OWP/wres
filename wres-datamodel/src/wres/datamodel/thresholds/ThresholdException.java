package wres.datamodel.thresholds;

/**
 * An unchecked exception related to thresholds.
 * 
 * @author James Brown
 */

public final class ThresholdException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 1005982342026773021L;

    /**
     * Constructs an {@link ThresholdException} with no message.
     */

    public ThresholdException()
    {
        super();
    }

    /**
     * Constructs a {@link ThresholdException} with the specified message.
     * 
     * @param message the message.
     */

    public ThresholdException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link ThresholdException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public ThresholdException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
