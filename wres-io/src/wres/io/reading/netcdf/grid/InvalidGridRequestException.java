package wres.io.reading.netcdf.grid;

import java.io.Serial;

/**
 * Request must contain one or more paths.
 */

public class InvalidGridRequestException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = 2892982359002079765L;

    /**
     * Constructs an {@link InvalidGridRequestException} with the specified message.
     * 
     * @param message the message.
     */

    public InvalidGridRequestException( String message )
    {
        super(message );
    }
}
