package wres.io.data.details;

import java.sql.SQLException;

/**
 * A SQLException to throw when WRES detects an inconsistency in the database,
 * for example violations of referential integrity or an id missing where one
 * was expected.
 */
public class InconsistentDataException extends SQLException
{
    public InconsistentDataException( String message )
    {
        super( message );
    }
}
