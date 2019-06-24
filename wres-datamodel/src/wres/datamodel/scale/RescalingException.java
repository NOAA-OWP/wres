package wres.datamodel.scale;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with rescaling data.
 * 
 * @author james.brown@hydrosolved.com
 */

public class RescalingException extends DataException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 7343085499573898089L;

    /**
     * Constructs an {@link RescalingException} with no message.
     */

    public RescalingException()
    {
        super();
    }

    /**
     * Constructs a {@link RescalingException} with the specified message.
     * 
     * @param message the message.
     */

    public RescalingException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link RescalingException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public RescalingException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
