package wres.vis.writing;

/**
 * A runtime exception associated with writing metric outputs to graphics.
 * 
 * @author james.brown@hydrosolved.com
 */

public class GraphicsWriteException extends RuntimeException
{

    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 1263676402746315184L;

    /**
     * Constructs an {@link GraphicsWriteException} with no message.
     */

    public GraphicsWriteException()
    {
        super();
    }

    /**
     * Constructs a {@link GraphicsWriteException} with the specified message.
     * 
     * @param message the message.
     */

    public GraphicsWriteException( final String message )
    {
        super( message );
    }

    /**
     * Constructs a {@link GraphicsWriteException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public GraphicsWriteException( final String message, final Throwable cause )
    {
        super( message, cause );
    }

}
