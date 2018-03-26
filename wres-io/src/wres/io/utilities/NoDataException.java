package wres.io.utilities;

import java.io.IOException;

/**
 * A temporary placeholder exception to be thrown when data is not found.
 * As distinct from a SQLException which the database client library throws.
 * Long run, since no data is a common every day occurrence, we should handle
 * no data without having any exceptions thrown.
 */
public class NoDataException extends IOException
{
    public NoDataException( String message )
    {
        super(message);
    }

    public NoDataException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
