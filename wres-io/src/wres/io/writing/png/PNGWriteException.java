package wres.io.writing.png;

import wres.io.writing.WriteException;

/**
 * A runtime exception associated with writing metric outputs in Portable Network Graphics (PNG) format.
 * 
 * @author james.brown@hydrosolved.com
 */

public class PNGWriteException extends WriteException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 1263676402746315184L;

    /**
     * Constructs an {@link PNGWriteException} with no message.
     */

    public PNGWriteException()
    {
        super();
    }

    /**
     * Constructs a {@link PNGWriteException} with the specified message.
     * 
     * @param message the message.
     */

    public PNGWriteException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link PNGWriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PNGWriteException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
