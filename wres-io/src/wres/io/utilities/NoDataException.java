package wres.io.utilities;

public class NoDataException extends Exception
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
