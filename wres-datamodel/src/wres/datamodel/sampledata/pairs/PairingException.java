package wres.datamodel.sampledata.pairs;

import wres.datamodel.DataException;

/**
 * A runtime exception associated with pairing data.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PairingException extends DataException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = -2717305674399391470L;

    /**
     * Constructs an {@link PairingException} with no message.
     */

    public PairingException()
    {
        super();
    }

    /**
     * Constructs a {@link PairingException} with the specified message.
     * 
     * @param message the message.
     */

    public PairingException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link PairingException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PairingException(final String message, final Throwable cause)
    {
        super(message, cause);
    }

}
