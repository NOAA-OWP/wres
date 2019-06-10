package wres.system;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class DatabaseConnectionSupplier implements Supplier<Connection>
{
    @Override
    public Connection get()
    {
        try
        {
            return SystemSettings.getRawDatabaseConnection();
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( "Could not get a raw Connection.",
                                             se );
        }
    }
}
