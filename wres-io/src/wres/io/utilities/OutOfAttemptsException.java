package wres.io.utilities;

import java.io.IOException;

public class OutOfAttemptsException extends IOException
{
    private static final long serialVersionUID = 2117102581796644973L;

    public OutOfAttemptsException(final String message, final Throwable cause)
    {
        super(message, cause);
    }
}
