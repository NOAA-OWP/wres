package wres.datamodel.pools;

import java.io.Serial;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with incorrect metadata.
 * 
 * @author James Brown
 */

public class PoolMetadataException extends DataException
{
    /**
     * Serial identifier.
     */
    
    @Serial
    private static final long serialVersionUID = -1559780940246528464L;

    /**
     * Constructs a {@link PoolMetadataException} with the specified message.
     * 
     * @param message the message.
     */

    public PoolMetadataException(final String message)
    {
        super(message);
    }
}
