package wres.datamodel.pools;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect {@link Pool}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PoolException extends DataException
{

    /**
     * Serial identifier.
     */
    private static final long serialVersionUID = -382138122319205095L;

    /**
     * Constructs an {@link PoolException} with no message.
     */

    public PoolException()
    {
        super();
    }

    /**
     * Constructs a {@link PoolException} with the specified message.
     * 
     * @param message the message.
     */

    public PoolException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link PoolException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PoolException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
