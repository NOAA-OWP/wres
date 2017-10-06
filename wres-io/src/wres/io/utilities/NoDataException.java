package wres.io.utilities;

import java.io.IOException;

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
