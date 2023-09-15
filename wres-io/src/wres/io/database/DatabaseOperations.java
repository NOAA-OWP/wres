package wres.io.database;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.ZoneOffset.UTC;

import wres.io.ingesting.IngestException;
import wres.io.ingesting.database.IncompleteIngest;
import wres.io.database.locking.DatabaseLockManager;
import wres.io.database.locking.DatabaseLockManagerNoop;
import wres.io.database.locking.DatabaseLockManagerPostgres;
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

    /** Re-used string. */
    private static final String TRUNCATE_TABLE = "TRUNCATE TABLE ";

    /**
     * The log parameters.
     * @param arguments the arguments used to run the application, at least two
     * @param projectName the project name
     * @param declaration the declaration string
     * @param hash the hash of the project datasets
     * @param startTime the start of the execution interval
     * @param endTime the end of the execution interval
     * @param failed whether the execution failed
     * @param exception any exception that caused the application to exit
     * @param version the top-level version of the application (module versions vary), not null
     */

    public record LogParameters( List<String> arguments,
                                 String projectName,
                                 String declaration,
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
        String connectionString = database.getConnectionString();
        Properties properties = database.getConnectionProperties();

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
        DatabaseOperations.refreshStatistics( database );
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

        DatabaseOperations.clean( database );
        DatabaseOperations.refreshStatistics( database );

        Instant stop = Instant.now();
        Duration elapsed = Duration.between( start, stop );

        LOGGER.info( "Finished cleaning and refreshing the database in {}.", elapsed );
    }

    /**
     * Attempts to migrate the database.
     *
     * @param database the database
     * @throws SQLException if cleaning fails after migration
     * @throws NullPointerException if the database is null
     */

    public static void migrateDatabase( Database database ) throws SQLException
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
     * Inserts data into the database (or copies in the case of postgres).
     * @param database the database
     * @param tableName The table name for the copy or insert statement.
     * @param columnNames The column names in the order the values appear.
     * @param values The values in the order the columnNames appear.
     * @param charColumns True and false in the order of column names/values. When true, this is a column needs quoting
     *                    on insert.
     * @throws NullPointerException if any input is null
     * @throws IllegalArgumentException if any of the inputs are empty or inconsistent with each other
     */

    public static void insertIntoDatabase( Database database,
                                           String tableName,
                                           List<String> columnNames,
                                           List<String[]> values,
                                           boolean[] charColumns )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( tableName );
        Objects.requireNonNull( columnNames );
        Objects.requireNonNull( values );
        Objects.requireNonNull( charColumns );

        if ( columnNames.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot insert data without column names." );
        }

        if ( values.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot insert values unless some values are provided." );
        }

        // Check the rows in advance of calling either internal method.
        for ( String[] row : values )
        {
            if ( row.length != columnNames.size() )
            {
                throw new IllegalArgumentException( "Every row length (found "
                                                    + row.length
                                                    + ") needs to match column count "
                                                    + columnNames.size()
                                                    + " or it won't work. "
                                                    + "Column names: "
                                                    + columnNames
                                                    + "Values: "
                                                    + Arrays.toString( row ) );
            }

            if ( row.length != charColumns.length )
            {
                throw new IllegalArgumentException( "Every row length (found "
                                                    + row.length
                                                    + ") needs to match char column count "
                                                    + charColumns.length
                                                    + " or it won't work. "
                                                    + "Char columns: "
                                                    + Arrays.toString( charColumns )
                                                    + "Values: "
                                                    + Arrays.toString( row ) );
            }
        }

        if ( database.getSettings().getDatabaseType() == DatabaseType.POSTGRESQL )
        {
            DatabaseOperations.pgCopy( database,
                                       tableName,
                                       columnNames,
                                       values );
        }
        else
        {
            DatabaseOperations.insert( database,
                                       tableName,
                                       columnNames,
                                       values,
                                       charColumns );
        }
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
            LocalDateTime endedAtZulu = LocalDateTime.ofInstant( logParameters.endTime(), UTC );

            // Log the declaration if available
            String project = "";
            if ( Objects.nonNull( logParameters.declaration() ) )
            {
                project = logParameters.declaration();
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
        catch ( SQLException e )
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
     * Refreshes statistics that the database uses to optimize queries. Performance suffers if the operation is told to
     * vacuum missing values, but the performance of the system as a whole is improved if many values were removed
     * prior to running. Thus, if a vacuum is available for a given database implementation, the database is vacuumed.
     * @param database the database whose statistics should be refreshed
     * @throws SQLException when refresh or adding indices goes wrong
     */
    private static void refreshStatistics( Database database ) throws SQLException
    {
        String sql;

        final String optionalVacuum;

        if ( database.getType()
                     .hasVacuumAnalyze() )
        {
            optionalVacuum = "VACUUM ";
        }
        else
        {
            optionalVacuum = "";
        }

        if ( database.getType()
                     .hasAnalyze() )
        {
            sql = optionalVacuum + "ANALYZE;";
            LOGGER.info( "Analyzing data for efficient execution..." );

            Query query = new Query( database.getSystemSettings(), sql );

            try ( Connection connection = database.getConnection() )
            {
                query.execute( connection );
            }
            catch ( SQLException se )
            {
                throw new SQLException( "Data in the database could not be "
                                        + "analyzed for efficient execution.",
                                        se );
            }

            LOGGER.info( "Database statistical analysis is now complete." );
        }
        else
        {
            LOGGER.info( "WRES skipping analysis for efficient execution for db {}",
                         database.getType() );
        }
    }

    /**
     * Removes all user data from the database
     * TODO: This should probably accept an object or list to allow for the removal of business logic.
     * Assumes that locking has already been done at a higher level by caller(s)
     * @throws SQLException Thrown if successful communication with the
     * database could not be established
     */
    private static void clean( Database database ) throws SQLException
    {
        StringJoiner builder;

        if ( database.getType() == DatabaseType.H2 )
        {
            builder = new StringJoiner( System.lineSeparator(),
                                        "SET REFERENTIAL_INTEGRITY FALSE;"
                                        + System.lineSeparator(),
                                        System.lineSeparator()
                                        + "SET REFERENTIAL_INTEGRITY TRUE;" );
        }
        else
        {
            builder = new StringJoiner( System.lineSeparator() );
        }

        List<String> tables = List.of( "wres.Source",
                                       "wres.TimeSeries",
                                       "wres.TimeSeriesValue",
                                       "wres.Ensemble",
                                       "wres.Project",
                                       "wres.ProjectSource",
                                       "wres.Feature",
                                       "wres.MeasurementUnit" );

        for ( String table : tables )
        {
            if ( database.getType()
                         .hasTruncateCascade() )
            {
                builder.add( TRUNCATE_TABLE + table + " CASCADE;" );
            }
            else
            {
                builder.add( TRUNCATE_TABLE + table + ";" );
            }
        }

        builder.add( "INSERT INTO wres.Ensemble ( ensemble_name ) VALUES ('default');" );

        Query query = new Query( database.getSystemSettings(), builder.toString() );

        try
        {
            database.execute( query, false );
        }
        catch ( final SQLException e )
        {
            String message = "WRES data could not be removed from the database."
                             + System.lineSeparator()
                             + System.lineSeparator()
                             + builder;
            // Decorate with contextual information.
            throw new SQLException( message, e );
        }
    }

    /**
     * Inserts data into the database.
     * @param database the database
     * @param tableName the table name
     * @param columnNames the column names
     * @param values the data values
     * @param charColumns whether the columns are char columns
     */

    private static void insert( Database database,
                                String tableName,
                                List<String> columnNames,
                                List<String[]> values,
                                boolean[] charColumns )
    {
        StringJoiner columns = new StringJoiner( ",", " ( ", " ) " );

        for ( String column : columnNames )
        {
            columns.add( column );
        }

        String insertHeader = "INSERT INTO " + tableName
                              + columns
                              + "VALUES\n";
        StringJoiner insertsJoiner = new StringJoiner( ",\n", insertHeader, ";\n" );

        for ( String[] row : values )
        {
            StringJoiner insertForRowJoiner = new StringJoiner( ",", "( ", " )" );

            for ( int i = 0; i < row.length; i++ )
            {
                // When it's labeled as a charColumn, add quotes to insert.
                if ( charColumns[i] )
                {
                    insertForRowJoiner.add( "'" + row[i] + "'" );
                }
                else
                {
                    // It's numeric, no need for quotes.
                    insertForRowJoiner.add( row[i] );
                }
            }

            String insertForRow = insertForRowJoiner.toString();
            insertsJoiner.add( insertForRow );
        }

        String insertsQuery = insertsJoiner.toString();
        Query query = new Query( database.getSystemSettings(), insertsQuery );
        int rowsModified;

        try ( Connection connection = database.getConnection() )
        {
            rowsModified = query.execute( connection );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to insert data into "
                                       + tableName,
                                       se );
        }

        if ( rowsModified != values.size() )
        {
            LOGGER.warn( "Expected to insert {} rows but {} were inserted.",
                         values.size(),
                         rowsModified );
        }
    }

    /**
     * Sends a copy statement to the indicated table within a postgres db.
     * @param database the database
     * @param tableName The table name.
     * @param columnNames The columns consistent with the order of values.
     * @param values The values to copy, outer array is a tuple/row,
     *               inner array is each value in the row (one for each col).
     * @throws IngestException Thrown if an error was encountered when trying to
     * copy data to the database.
     */

    private static void pgCopy( Database database,
                                String tableName,
                                List<String> columnNames,
                                List<String[]> values )
    {
        StringJoiner columns = new StringJoiner( ",", " ( ", " )" );

        columnNames.forEach( columns::add );

        String tableDefinition = tableName + columns;
        String delimiter = "|";

        // The format of the copy statement needs to be of the format
        // "COPY wres.TimeSeriesValue_xxxx FROM STDIN WITH DELIMITER '|'"
        String copyDefinition = "COPY "
                                + tableDefinition
                                + " FROM STDIN WITH DELIMITER '"
                                + delimiter
                                + "'";

        final byte[] nullBytes = "\\N".getBytes( StandardCharsets.UTF_8 );
        CopyIn copyIn = null;

        try ( Connection connection = database.getConnection() )
        {
            PGConnection pgConnection = connection.unwrap( PGConnection.class );

            // We need specialized functionality to copy, so we need to create a manager object that will
            // handle the copy operation from the postgresql driver
            CopyManager manager = pgConnection.getCopyAPI();

            // Use the manager to stream the data through to the database
            copyIn = manager.copyIn( copyDefinition );
            byte[] valueDelimiterBytes = delimiter.getBytes( StandardCharsets.UTF_8 );
            byte[] valueRowDelimiterBytes = "\n".getBytes( StandardCharsets.UTF_8 );

            for ( String[] row : values )
            {
                DatabaseOperations.copyRow( row,
                                            valueDelimiterBytes,
                                            valueRowDelimiterBytes,
                                            nullBytes,
                                            copyIn );
            }

            copyIn.endCopy();
        }
        catch ( SQLException e )
        {
            // If we are in a non-production environment, it would help to see the format of the data
            // that couldn't be added
            if ( LOGGER.isDebugEnabled() )
            {
                String allValues = values.toString();
                int subStringMax = Math.min( allValues.length(), 5000 );
                LOGGER.debug( "Data could not be copied to the database:{}{}...",
                              copyDefinition,
                              allValues.substring( 0, subStringMax ),
                              e );
            }

            // From https://www.postgresql.org/message-id/8D1E8D0DC762E82-1320-C263%40webmail-vm124.sysops.aol.com
            // Quoting Brett Wooldridge, author of HikariCP, "call cancelCopy()"
            if ( copyIn != null )
            {
                try
                {
                    copyIn.cancelCopy();
                }
                catch ( SQLException se )
                {
                    LOGGER.warn( "Failed to cancel copy operation on table {}.",
                                 tableName,
                                 se );
                }
            }

            throw new IngestException( "Data could not be copied to the database.", e );
        }
    }

    /**
     * Copies a row to the database.
     * @param row the row
     * @param valueDelimiterBytes the value delimiter bytes
     * @param valueRowDelimiterBytes the value row delimiter bytes
     * @param nullBytes the null bytes
     * @param copyIn the copy operation
     * @throws SQLException if the copy failed for any reason
     */
    private static void copyRow( String[] row,
                                 byte[] valueDelimiterBytes,
                                 byte[] valueRowDelimiterBytes,
                                 byte[] nullBytes,
                                 CopyIn copyIn ) throws SQLException
    {
        for ( int i = 0; i < row.length; i++ )
        {
            if ( Objects.nonNull( row[i] ) )
            {
                byte[] bytes = row[i].getBytes( StandardCharsets.UTF_8 );
                copyIn.writeToCopy( bytes, 0, bytes.length );

            }
            else
            {
                copyIn.writeToCopy( nullBytes, 0, nullBytes.length );
            }

            if ( i < row.length - 1 )
            {
                copyIn.writeToCopy( valueDelimiterBytes,
                                    0,
                                    valueDelimiterBytes.length );
            }
        }

        copyIn.writeToCopy( valueRowDelimiterBytes,
                            0,
                            valueDelimiterBytes.length );
    }

    /**
     * Do not construct.
     */

    private DatabaseOperations()
    {
        //  Do not construct
    }

}
