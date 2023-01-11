package wres.io.database;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.ingesting.database.IncompleteIngest;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseLockManagerPostgres;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;

/**
 * A helper class for performing maintenance operations on a {@link Database}.
 * 
 * @author James Brown
 */

public class DatabaseOperations
{
    /** Logger. **/
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseOperations.class );

    /**
     * Attempts a connection to the database and throws an exception if the connection fails.
     * 
     * @param database the database
     * @throws SQLException if the connection could not be established
     * @throws IOException if the host is invalid
     * @throws NullPointerException if the database is null
     */

    public static void testDatabaseConnection( Database database ) throws SQLException, IOException
    {
        Objects.requireNonNull( database );

        DatabaseSettings databaseSettings = database.getSettings();
        String host = databaseSettings.getHost();
        int port = databaseSettings.getPort();
        String username = databaseSettings.getUsername();
        String connectionString = databaseSettings.getConnectionString();
        Properties properties = databaseSettings.getConnectionProperties();

        boolean validURL = DatabaseOperations.isHostValid( host, port );

        if ( !validURL )
        {
            throw new IOException( "The given database host:port combination ('"
                                   + host
                                   + ":"
                                   + port
                                   + "') is not accessible." );
        }

        try ( Connection connection = database.getRawConnection( connectionString, properties );
              Statement test = connection.createStatement() )
        {
            test.execute( "SELECT 1;" );
        }
        catch ( SQLException sqlError )
        {
            String message = "The database could not be reached for connection verification: "
                             + System.lineSeparator()
                             + System.lineSeparator();
            message += sqlError.getMessage() + System.lineSeparator() + System.lineSeparator();
            message += "Please ensure that you have:" + System.lineSeparator();
            message += "1) The correct URL to your database: " + connectionString
                       + System.lineSeparator();
            message += "2) The correct username for your database: " + username + System.lineSeparator();
            message += "3) The correct password for your user in the database" + System.lineSeparator();
            message += "4) An active connection to a network that may reach the requested database server"
                       + System.lineSeparator();
            message += "5) The correct database driver class on the application classpath"
                       + System.lineSeparator()
                       + System.lineSeparator();
            message += "The application will now exit.";
            throw new SQLException( message, sqlError );
        }
    }

    /**
     * Updates the statistics and removes all dead rows from the database. Assumes caller has already obtained 
     * exclusive lock on database.
     * 
     * @param database The database to use.
     * @throws SQLException if the orphaned data could not be removed or the refreshing of statistics fails
     */
    public static void refreshDatabase( Database database ) throws SQLException
    {
        IncompleteIngest.removeOrphanedData( database );
        database.refreshStatistics( true );
    }

    /**
     * Removes all loaded user information from the database. Assumes that the caller has already gotten an exclusive 
     * lock for modify.
     * 
     * @param database The database to use.
     * @throws SQLException when cleaning or refreshing stats fails
     * @throws NullPointerException if the database is null
     */

    public static void cleanDatabase( Database database ) throws SQLException
    {
        Objects.requireNonNull( database );

        LOGGER.info( "Cleaning and refreshing the database. This may take some time..." );

        Instant start = Instant.now();

        database.clean();
        database.refreshStatistics( true );

        Instant stop = Instant.now();
        Duration elapsed = Duration.between( start, stop );

        LOGGER.info( "Finished cleaning and refreshing the database in {}.", elapsed );
    }

    /**
     * Attempts to migrate the database.
     * 
     * @param database the database
     * @throws IOException if the migration fails
     * @throws SQLException if cleaning fails after migration
     * @throws NullPointerException if the database is null
     */

    public static void migrateDatabase( Database database ) throws SQLException, IOException
    {
        Objects.requireNonNull( database );

        DatabaseSettings databaseSettings = database.getSettings();
        DatabaseType type = databaseSettings.getDatabaseType();
        String databaseName = databaseSettings.getDatabaseName();

        LOGGER.info( "Beginning database migration. This takes time." );
        DatabaseLockManager lockManager;

        if ( type == DatabaseType.POSTGRESQL )
        {
            lockManager = new DatabaseLockManagerPostgres( database::getRawConnection );
        }
        else if ( type == DatabaseType.H2 )
        {
            lockManager = new DatabaseLockManagerNoop();
        }
        else
        {
            throw new UnsupportedOperationException( "Only postgresql and h2 are currently supported" );
        }

        try ( DatabaseSchema schema = new DatabaseSchema( databaseName, lockManager );
              Connection connection = database.getRawConnection(); )
        {
            schema.applySchema( connection );
        }
        finally
        {
            lockManager.shutdown();
        }

        DatabaseOperations.cleanPriorRuns( database );
        LOGGER.info( "Finished database migration." );
    }

    /**
     * Cleans prior executions. Not to be confused with cleaning a database, i.e., {@link #cleanDatabase(Database)}.
     * @param database the database
     * @throws SQLException if the clean failed
     * @throws NullPointerException if the database is null
     */

    private static void cleanPriorRuns( Database database ) throws SQLException
    {
        Objects.requireNonNull( database );

        DatabaseSettings databaseSettings = database.getSettings();
        DatabaseType type = databaseSettings.getDatabaseType();
        String username = databaseSettings.getUsername();
        String databaseName = databaseSettings.getDatabaseName();

        if ( type == DatabaseType.POSTGRESQL )
        {
            final String NEWLINE = System.lineSeparator();

            String script = "";
            script += "SELECT pg_terminate_backend(PT.pid)" + NEWLINE;
            script += "FROM pg_locks L" + NEWLINE;
            script += "INNER JOIN pg_stat_all_tables T" + NEWLINE;
            script += "    ON L.relation = t.relid" + NEWLINE;
            script += "INNER JOIN pg_stat_activity PT" + NEWLINE;
            script += "    ON L.pid = PT.pid" + NEWLINE;
            script += "WHERE T.schemaname <> 'pg_toast'::name" + NEWLINE;
            script += "    AND t.schemaname < 'pg_catalog'::name" + NEWLINE;
            script += "    AND usename = '" + username + "'" + NEWLINE;
            script += "    AND datname = '" + databaseName + "'" + NEWLINE;
            script += "GROUP BY PT.pid;";

            try ( Connection connection = database.getRawConnection();
                  Statement clean = connection.createStatement() )
            {
                clean.execute( script );
                try ( ResultSet resultSet = clean.getResultSet() )
                {
                    if ( resultSet.isBeforeFirst() )
                    {
                        LOGGER.debug( "Lock(s) from previous runs of this applications "
                                      + "have been released." );
                    }
                }
            }
        }
    }

    /**
     * @param host the host, not null
     * @param port the port
     * @return whether the database host is valid
     */

    private static boolean isHostValid( String host, int port )
    {
        Objects.requireNonNull( host );

        if ( host.equalsIgnoreCase( "localhost" ) )
        {
            return true;
        }

        try ( Socket socket = new Socket() )
        {
            socket.connect( new InetSocketAddress( host, port ), 2000 );
            return true;
        }
        catch ( IOException ioe )
        {
            String message = "The intended host:port combination ({" + host + "}:{" + port + "}) is not accessible.";
            LOGGER.warn( message, ioe );
            return false;
        }
    }

    /**
     * Do not construct.
     */

    private DatabaseOperations()
    {
        //  Do not construct
    }

}
