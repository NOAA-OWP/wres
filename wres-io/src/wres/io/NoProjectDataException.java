package wres.io;

import java.io.Serial;

/**
 * An exception that indicates a project fails because no data could be found where data was expected, either because
 * validation didn't happen before a call was made or because the intersection of project declaration and data produced
 * no data whatsoever. All of these are unrecoverable situations.
 */
public class NoProjectDataException extends RuntimeException
{
    @Serial
    private static final long serialVersionUID = -458279831785789017L;

    /**
     * Creates an instance.
     * @param message the message
     */
    public NoProjectDataException( String message )
    {
        super( message );
    }
}