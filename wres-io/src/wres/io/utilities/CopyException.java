package wres.io.utilities;

import java.sql.SQLException;

/**
 * Created by ctubbs on 7/6/17.
 */
public class CopyException extends SQLException {
    public CopyException(){}
    public CopyException(String message)
    {
        super(message);
    }

    public CopyException(Throwable cause)
    {
        super(cause);
    }

    public CopyException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
