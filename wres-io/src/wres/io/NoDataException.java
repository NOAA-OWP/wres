package wres.io;

/**
 * An exception that indicates no data could be found where data was expected, either because validation didn't happen
 * before a call was made or because the intersection of project declaration and data produced no data whatsoever. All
 * of these are unrecoverable situations.
 */

public class NoDataException extends RuntimeException
{
    private static final long serialVersionUID = -458279831785789017L;

    public NoDataException( String message )
    {
        super(message);
    }

    public NoDataException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
