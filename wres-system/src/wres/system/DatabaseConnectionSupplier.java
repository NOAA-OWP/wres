package wres.system;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Supplier;

public class DatabaseConnectionSupplier implements Supplier<Connection>
{
    private final SystemSettings systemSettings;

    public DatabaseConnectionSupplier( SystemSettings systemSettings )
    {
        this.systemSettings = systemSettings;
    }

    @Override
    public Connection get()
    {
        try
        {
            return systemSettings.getRawDatabaseConnection();
        }
        catch ( SQLException se )
        {
            throw new IllegalStateException( "Could not get a raw Connection.",
                                             se );
        }
    }
}
