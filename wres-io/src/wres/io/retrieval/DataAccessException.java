package wres.io.retrieval;

import java.io.Serial;

/**
 * Runtime exception associated with data access.
 * 
 * @author James Brown
 */

public class DataAccessException extends RuntimeException
{
    /**
     * Serial identifier.
     */
    
    @Serial
    private static final long serialVersionUID = 2826302222876172482L;

    /**
     * Constructs an {@link DataAccessException} with no message.
     */

    public DataAccessException()
    {
        super();
    }

    /**
     * Constructs a {@link DataAccessException} with the specified message.
     * 
     * @param message the message.
     */

    public DataAccessException(final String message)
    {
        super(message);
    }

    /**
     * Constructs a {@link DataAccessException} with the specified message.
     * 
     * @param message the message.
     * @param cause the cause of the exception
     */

    public DataAccessException(final String message, final Throwable cause)
    {
        super(message, cause);
    }    
    
    
}
