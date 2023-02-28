package wres.pipeline.pooling;

/**
 * Runtime exception associated with pool creation.
 * 
 * @author James Brown
 */

public class PoolCreationException extends RuntimeException
{
    /**
     * Serial identifier.
     */

    private static final long serialVersionUID = 7909727897078743659L;

    /**
     * Constructs an {@link PoolCreationException} with no message.
     */

    public PoolCreationException()
    {
        super();
    }

    /**
     * Constructs a {@link PoolCreationException} with the specified message.
     * 
     * @param message the message.
     */

    public PoolCreationException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link PoolCreationException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public PoolCreationException(final String message, final Throwable cause)
    {
        super(message, cause);
    }    
    
    
}
