package wres.vis.writing;

import java.io.Serial;

/**
 * A runtime exception associated with writing metric outputs to graphics.
 * 
 * @author James Brown
 */

class GraphicsWriteException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    @Serial
    private static final long serialVersionUID = 1263676402746315184L;

    /**
     * Constructs a {@link GraphicsWriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    GraphicsWriteException( String message, Throwable cause )
    {
        super( message, cause );
    }

}
