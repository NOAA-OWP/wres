package wres.grid.client;

/**
 * Request must contain one or more paths.
 */

public class InvalidRequestException extends RuntimeException
{

    private static final long serialVersionUID = 2892982359002079765L;

    /**
     * Constructs an {@link InvalidRequestException} with the specified message.
     * 
     * @param message the message.
     */

    public InvalidRequestException( String message )
    {
        super(message );
    }
}
