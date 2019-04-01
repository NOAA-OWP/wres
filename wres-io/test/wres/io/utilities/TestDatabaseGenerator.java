package wres.io.utilities;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class TestDatabaseGenerator
{
    public static ComboPooledDataSource createDatabase( String connectionString )
    {
        ComboPooledDataSource dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl( connectionString );
        dataSource.setAutoCommitOnClose( true );
        dataSource.setInitialPoolSize( 10 );
        dataSource.setMaxIdleTime( 30 );
        dataSource.setMaxPoolSize( 20 );
        return dataSource;
    }

    public static String getConnectionString( String databaseName )
    {
        try
        {
            Class.forName( "org.h2.Driver" );
        }
        catch ( ClassNotFoundException cnfe )
        {
            throw new IllegalStateException( "Couldn't find h2 driver.", cnfe );
        }
        return "jdbc:h2:mem:TestDatabase" + databaseName
               + ";MODE=PostgreSQL;";
    }
}
