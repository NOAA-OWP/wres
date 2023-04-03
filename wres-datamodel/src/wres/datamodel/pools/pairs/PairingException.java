package wres.datamodel.pools.pairs;

import java.io.Serial;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with pairing data.
 * 
 * @author James Brown
 */

public class PairingException extends DataException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = -2717305674399391470L;

    /**
     * Constructs a {@link PairingException} with the specified message.
     * 
     * @param message the message.
     */

    public PairingException(final String message)
    {
        super(message);
    }
}
