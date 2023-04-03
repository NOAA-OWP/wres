package wres.io.database;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.ZoneOffset.UTC;

import wres.io.ingesting.database.IncompleteIngest;
import wres.system.DatabaseLockManager;
import wres.system.DatabaseLockManagerNoop;
import wres.system.DatabaseLockManagerPostgres;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;

/**
 * A helper class for performing operations on a {@link Database}.
 *
 * @author James Brown
 */

public class DatabaseOperations
{
    /** Logger. **/
    private static final Logger LOGGER = LoggerFactory.getLogger( DatabaseOperations.class );

    /**
     * The log parameters.
     * @param arguments the arguments used to run the application, at least two
     * @param projectName the project name
     * @param hash the hash of the project datasets
     * @param startTime the start of the execution interval
     * @param endTime the end of the execution interval
     * @param failed whether the execution failed
     * @param exception any exception that caused the application to exit
     * @param version the top-level version of the application (module versions vary), not null
     */
    public record LogParameters( List<String> arguments,
                                 String projectName,
                                 String hash,
                                 Instant startTime,
                                 Instant endTime,
                                 boolean failed,
                                 Exception exception,
                                 String version ) {}

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
        boolean removed = IncompleteIngest.removeOrphanedData( database );
        database.refreshStatistics( true );
        LOGGER.debug( "Upon refreshing the database, orphaned data was removed: {}.", removed );
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
              Connection connection = database.getRawConnection() )
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
     * Logs information about the execution of the WRES into the database for aid in remote debugging.
     * @param database the database
     * @param logParameters the log parameters
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if there are zero arguments
     */
    public static void logExecution( Database database,
                                     LogParameters logParameters )
    {
        Objects.requireNonNull( logParameters );
        Objects.requireNonNull( logParameters.arguments() );
        Objects.requireNonNull( logParameters.version() );
        Objects.requireNonNull( logParameters.startTime() );
        Objects.requireNonNull( logParameters.endTime() );

        List<String> arguments = logParameters.arguments();
        if ( arguments.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot log an execution with zero arguments." );
        }

        try
        {
            LocalDateTime startedAtZulu = LocalDateTime.ofInstant( logParameters.startTime(), UTC );
            LocalDateTime endedAtZulu = LocalDateTime.ofInstant( logParameters.endTime, UTC );

            // For any arguments that happen to be regular files, read the
            // contents of the first file into the "project" field. Maybe there
            // is an improvement that can be made, but this should cover the
            // common case of a single file in the args.
            String project = "";

            // The two operations that might perform a project related operation are 'execute' and 'ingest'
            // these are the only cases where we might be interested in a project configuration
            String testArg = arguments.get( 0 )
                                      .toLowerCase();
            if ( "execute".equals( testArg ) || "ingest".equals( testArg ) )
            {

                // Go ahead and assign the second argument as the project
                // if this instance is in server mode,
                // this will be the raw project text and a file path will not be involved
                project = arguments.get( 1 );

                // Look through the arguments to find the path to a file
                // this is more than likely our project configuration
                for ( String arg : arguments )
                {
                    Path path = Paths.get( arg );

                    if ( path.toFile().isFile() )
                    {
                        project = String.join( System.lineSeparator(),
                                               Files.readAllLines( path ) );
                        break;
                    }
                }
            }

            DataScripter script = new DataScripter( database );

            script.addLine( "INSERT INTO wres.ExecutionLog (" );
            script.addTab().addLine( "arguments," );
            script.addTab().addLine( "system_version," );
            script.addTab().addLine( "project," );
            script.addTab().addLine( "project_name," );
            script.addTab().addLine( "hash," );
            script.addTab().addLine( "username," );
            script.addTab().addLine( "address," );
            script.addTab().addLine( "start_time," );
            script.addTab().addLine( "end_time," );
            script.addTab().addLine( "failed," );
            script.addTab().addLine( "error" );
            script.addLine( ")" );
            script.addLine( "VALUES (" );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );

            if ( database.getType() == DatabaseType.POSTGRESQL )
            {
                script.addTab().addLine( "inet_client_addr()," );
            }
            else if ( database.getType().hasUserFunction() )
            {
                script.addTab().addLine( "user()," );
            }
            else
            {
                script.addTab().addLine( "NULL," );
            }

            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?," );
            script.addTab().addLine( "?" );
            script.addLine( ");" );

            String exception = null;

            if ( Objects.nonNull( logParameters.exception() ) )
            {
                exception = ExceptionUtils.getStackTrace( logParameters.exception() );
            }

            script.execute( String.join( " ", arguments ),
                            logParameters.version(),
                            project,
                            logParameters.projectName(),
                            logParameters.hash(),
                            System.getProperty( "user.name" ),
                            // Let server find and report network address
                            startedAtZulu,
                            endedAtZulu,
                            logParameters.failed(),
                            exception );
        }
        catch ( SQLException | IOException e )
        {
            LOGGER.warn( "Execution metadata could not be logged to the database.",
                         e );
        }
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
