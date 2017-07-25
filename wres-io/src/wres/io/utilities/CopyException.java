package wres.io.utilities;

import wres.util.Internal;

import java.sql.SQLException;

/**
 * Created by ctubbs on 7/6/17.
 */
@Internal(exclusivePackage = "wres.io")
public class CopyException extends SQLException {
    @Internal(exclusivePackage = "wres.io")
    public CopyException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
