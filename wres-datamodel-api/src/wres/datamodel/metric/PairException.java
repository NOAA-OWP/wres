package wres.datamodel.metric;

/**
 * A runtime exception associated with an incorrect verification pair.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

@SuppressWarnings("serial")
public class PairException extends RuntimeException
{

    /**
     * Constructs an {@link PairException} with no message.
     */

    public PairException()
    {
        super();
    }

    /**
     * Constructs a {@link PairException} with the specified message.
     * 
     * @param message the message.
     */

    public PairException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link PairException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PairException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
