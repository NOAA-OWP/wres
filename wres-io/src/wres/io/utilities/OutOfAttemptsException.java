package wres.io.utilities;

import java.io.IOException;

public class OutOfAttemptsException extends IOException
{
    public OutOfAttemptsException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
