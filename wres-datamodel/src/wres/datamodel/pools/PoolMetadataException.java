package wres.datamodel.pools;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PoolMetadataException extends DataException
{

    /**
     * Serial identifier.
     */
    
    private static final long serialVersionUID = -1559780940246528464L;
    
    /**
     * Constructs an {@link PoolMetadataException} with no message.
     */

    public PoolMetadataException()
    {
        super();
    }

    /**
     * Constructs a {@link PoolMetadataException} with the specified message.
     * 
     * @param message the message.
     */

    public PoolMetadataException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link PoolMetadataException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PoolMetadataException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
