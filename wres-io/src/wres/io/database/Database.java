package wres.io.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zaxxer.hikari.HikariDataSource;

import wres.io.data.TabularDataset;
import wres.io.retrieving.DataAccessException;
import wres.io.data.DataProvider;
import wres.system.DatabaseSettings;
import wres.system.DatabaseType;
import wres.system.SettingsHelper;
import wres.system.SystemSettings;

/**
 * Provides database connections and thread pools against which database activities can be performed. For common
 * operations on a {@link Database} see {@link DatabaseOperations}.
 */
public class Database
{
    /** System settings. */
    private final SystemSettings systemSettings;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Database.class );

    private final ConnectionSupplier connectionSupplier;

    /**
     * A separate thread executor used to schedule database communication
     * outside of other threads
     */
    private ThreadPoolExecutor sqlTasks;

    /**
     * Creates an instance
     * @param connectionSupplier A helper class to supply connection information as well as the SystemSettings
     */
    public Database( ConnectionSupplier connectionSupplier )
    {
        this.systemSettings = connectionSupplier.getSystemSettings();
        this.connectionSupplier = connectionSupplier;
        this.sqlTasks = createService();
    }

    /**
     * Shuts down the database in an orderly sequence.
     */
    public void shutdown()
    {
        LOGGER.info( "Shutting down the database..." );
        try
        {
            if ( !this.sqlTasks.isShutdown() )
            {
                // Shutdown
                this.sqlTasks.shutdown();

                // Await termination
                boolean died = this.sqlTasks.awaitTermination( 5, TimeUnit.SECONDS );

                if ( !died )
                {
                    List<Runnable> tasks = this.sqlTasks.shutdownNow();

                    if ( !tasks.isEmpty() && LOGGER.isInfoEnabled() )
                    {
                        LOGGER.info( "Abandoned {} tasks from {}.",
                                     tasks.size(),
                                     this.sqlTasks );
                    }
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while shutting down {}.", this.sqlTasks, ie );
            Thread.currentThread().interrupt();
        }

        this.closePools();
    }

    /**
     * Shuts down after all tasks have completed, or after timeout is reached,
     * whichever comes first. Tasks may be interrupted and abandoned.
     * @param timeOut the desired maximum wait, measured in timeUnit
     * @param timeUnit the unit for timeOut
     * @return the list of abandoned tasks
     */

    public List<Runnable> forceShutdown( long timeOut,
                                         TimeUnit timeUnit )
    {
        LOGGER.info( "Forcefully shutting down the database..." );
        List<Runnable> abandoned = new ArrayList<>();

        this.sqlTasks.shutdown();
        try
        {
            boolean result = this.sqlTasks.awaitTermination( timeOut, timeUnit );
            LOGGER.debug( "Database executor terminated: {}.", result );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Forced shutdown of the database interrupted.", ie );
            List<Runnable> abandonedDbTasks = this.sqlTasks.shutdownNow();
            abandoned.addAll( abandonedDbTasks );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedMore = this.sqlTasks.shutdownNow();
        abandoned.addAll( abandonedMore );

        LOGGER.info( "Database was forcefully shut down. "
                     + "Abandoned around {} database tasks.",
                     abandoned.size() );

        this.closePools();
        return abandoned;
    }

    /**
     * Checks out a database connection
     * @return A database connection from the standard connection pool
     * @throws SQLException Thrown if a connection could not be retrieved
     */
    public Connection getConnection() throws SQLException
    {
        return this.connectionSupplier.getConnectionPool().getConnection();
    }

    /**
     * Checks out a high priority database connection
     * @return A database connection that should have little to no contention
     * @throws SQLException Thrown if a connection could not be retrieved
     */
    public Connection getHighPriorityConnection() throws SQLException
    {
        LOGGER.debug( "Retrieving a high priority database connection..." );
        return this.connectionSupplier.getHighPriorityConnectionPool().getConnection();
    }

    /**
     * @return a raw database connection that does not originate from a pool
     * @throws IllegalStateException if the connection could not be obtained
     */

    public Connection getRawConnection()
    {
        String connectionString = this.getConnectionString();
        Properties properties = getConnectionProperties();

        try
        {
            return this.getRawConnection( connectionString, properties );
        }
        catch ( SQLException e )
        {
            throw new IllegalStateException( "Could not get a raw database connection.", e );
        }
    }

    /**
     * Creates the connection string used to access the database
     * @return The connection string used to connect to the database of interest
     */

    public String getConnectionString()
    {
        DatabaseSettings databaseSettings = this.getSettings();
        if ( Objects.nonNull( databaseSettings.getJdbcUrl() )
             && !databaseSettings.getJdbcUrl().isBlank() )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Using the jdbc url specified verbatim: '{}'",
                              databaseSettings.getJdbcUrl() );
            }

            return databaseSettings.getJdbcUrl();
        }

        StringBuilder connectionString = new StringBuilder();
        connectionString.append( "jdbc:" );
        connectionString.append( databaseSettings.getDatabaseType() );

        if ( databaseSettings.getDatabaseType() == DatabaseType.H2
             && databaseSettings.isUseSSL() )
        {
            connectionString.append( ":ssl" );
        }

        connectionString.append( "://" );
        connectionString.append( databaseSettings.getHost() );

        connectionString.append( ":" );
        connectionString.append( databaseSettings.getPort() );

        connectionString.append( "/" );
        connectionString.append( databaseSettings.getDatabaseName() );

        return connectionString.toString();
    }


    /**
     * @return the database connection properties
     */

    public Properties getConnectionProperties()
    {
        DatabaseSettings databaseConfiguration = this.systemSettings.getDatabaseConfiguration();
        return SettingsHelper.getDatasourceProperties( databaseConfiguration );
    }

    /**
     * @return whether database migration should be attempted
     */

    public boolean getAttemptToMigrate()
    {
        // Stop-gap measure between always-migrate and never-migrate.
        boolean migrate = this.systemSettings.getDatabaseConfiguration().isAttemptToMigrate();
        String attemptToMigrateSetting = System.getProperty( "wres.attemptToMigrate" );

        if ( attemptToMigrateSetting != null
             && !attemptToMigrateSetting.isBlank() )
        {
            if ( attemptToMigrateSetting.equalsIgnoreCase( "true" ) )
            {
                migrate = true;
            }
            else if ( attemptToMigrateSetting.equalsIgnoreCase( "false" ) )
            {
                migrate = false;
            }
            else
            {
                LOGGER.warn( "Value for wres.attemptToMigrate must be 'true' or 'false', not '{}'",
                             attemptToMigrateSetting );
            }
        }

        return migrate;
    }

    /**
     * @return the database settings
     */

    public DatabaseSettings getSettings()
    {
        return this.systemSettings.getDatabaseConfiguration();
    }

    /**
     * For system-level monitoring information, return the number of tasks in
     * the database queue.
     * @return the count of tasks waiting to be performed by the db workers.
     */

    public int getDatabaseQueueTaskCount()
    {
        if ( this.sqlTasks != null
             && this.sqlTasks.getQueue() != null )
        {
            return this.sqlTasks.getQueue().size();
        }

        return 0;
    }

    /**
     * @return the database type
     */
    public DatabaseType getType()
    {
        return this.getSystemSettings().getDatabaseConfiguration()
                   .getDatabaseType();
    }

    /**
     * Runs a single query in the database
     * @param query The query to run
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @return The number of rows modified or returned by the query
     * @throws SQLException Thrown if an issue was encountered while communicating with the database
     */
    int execute( final Query query, final boolean isHighPriority ) throws SQLException
    {
        int modifiedRows;

        try ( Connection connection = this.getConnection( isHighPriority ) )
        {
            modifiedRows = query.execute( connection );
        }

        return modifiedRows;
    }

    /**
     * Creates an in-memory record of the results from a database call
     * @param query The query that holds the information needed to call the database
     * @param isHighPriority Whether or not a high priority connection is required
     * @return A record of the results of the database call
     * @throws SQLException Thrown if there was an error when connecting to the database
     */
    DataProvider getData( final Query query, final boolean isHighPriority ) throws SQLException
    {
        // Since Database.buffer performs all the heavy lifting, we can just rely on that. Setting that
        // call in the try statement ensures that it is closed once the in-memory results are created
        try ( Connection connection = this.getConnection( isHighPriority );
              DataProvider rawProvider = this.buffer( connection, query ) )
        {
            return TabularDataset.from( rawProvider );
        }
    }

    /**
     * Opens a streaming connection to the results of a database call
     * <br><br>
     *     <p>
     *         Since the data contained within results will still be connected to the database, make sure
     *         you close the results to ensure that the resources required to provide the data are freed.
     *         Failure to do so will result in a leak.
     *     </p>
     * @param query The query that holds the information needed to call the database
     * @return A record of the results of the database call
     * @throws SQLException Thrown if there was an error when connecting to the database
     */
    DataProvider buffer( Connection connection, Query query ) throws SQLException
    {
        return new DatabaseDataProvider( connection, query.call( connection ) );
    }

    /**
     * Retrieves a single value from a field from a query
     * @param query The query to run that will retrieve a value from the database
     * @param label The name of the field that will contain the requested value
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @param <V> The type of value to retrieve from the query
     * @return null if no data could be loaded, the value of the retrieved field otherwise
     * @throws SQLException Thrown if an issue was encountered while communicating with the database
     */
    <V> V retrieve( final Query query, final String label, final boolean isHighPriority ) throws SQLException
    {
        try ( Connection connection = this.getConnection( isHighPriority );
              DataProvider data = this.buffer( connection, query ) )
        {
            if ( data.isEmpty() )
            {
                return null;
            }
            return data.getValue( label );
        }
    }

    /**
     * Schedules a query to run asynchronously with no regard to a result
     * @param query The query to schedule
     * @param isHighPriority Whether or not the query should be run on a high priority connection
     * @return The record for the scheduled task
     */
    Future<?> issue( final Query query, final boolean isHighPriority )
    {
        return this.issue( () -> {
            try
            {
                this.execute( query, isHighPriority );
            }
            catch ( SQLException e )
            {
                throw new DataAccessException( "Encountered an error while executing a database query.", e );
            }
        } );
    }

    /**
     * Creates and returns a raw connection without using a connection pool.
     * @param connectionString the connection string
     * @param properties the connection properties
     * @return the connection
     * @throws SQLException if the connection could not be acquired
     */

    Connection getRawConnection( String connectionString, Properties properties ) throws SQLException
    {
        return DriverManager.getConnection( connectionString,
                                            properties );
    }

    /**
     * Expose SystemSettings for the sake of DataScripter
     * @return the system settings this database instance uses.
     */
    SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }

    /**
     * Creates a new thread executor
     * @return A new thread executor that may run the maximum number of configured threads
     */
    private ThreadPoolExecutor createService()
    {
        // Ensures that all created threads will be labeled "Database Thread"
        ThreadFactory factory = runnable -> new Thread( runnable, "Database Thread" );
        ThreadPoolExecutor executor = new ThreadPoolExecutor( this.systemSettings.getDatabaseConfiguration()
                                                                                 .getMaxPoolSize(),
                                                              this.systemSettings.getDatabaseConfiguration()
                                                                                 .getMaxPoolSize(),
                                                              this.systemSettings.getPoolObjectLifespan(),
                                                              TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<>( this.systemSettings
                                                                                                .getDatabaseConfiguration()
                                                                                                .getMaxPoolSize()
                                                                                        * 5 ),
                                                              factory );

        // Ensures that the calling thread runs the new thread logic itself if
        // the upper bound of the executor's internal queue has been hit
        executor.setRejectedExecutionHandler( new ThreadPoolExecutor.CallerRunsPolicy() );
        return executor;
    }

    /**
     * Get a connection from either the normal or high priority pool
     * @param highPriority Whether to get from the high priority pool.
     * @return A connection
     * @throws SQLException When something goes wrong
     */

    private Connection getConnection( boolean highPriority ) throws SQLException
    {
        if ( highPriority )
        {
            return this.getHighPriorityConnection();
        }

        return this.getConnection();
    }

    /**
     * Closes the connection pools if they are {@link HikariDataSource}. Ideally, whatever created it should close it,
     * so should probably abstract this to {@link SystemSettings}, but leaving it here for now. Ideally, it should not
     * be necessary at all. See #61680. 
     */

    private void closePools()
    {
        LOGGER.info( "Closing database connection pools." );

        // Close out our database connection pools
        try
        {
            if ( this.connectionSupplier.getConnectionPool().isWrapperFor( HikariDataSource.class ) )
            {
                this.connectionSupplier.getConnectionPool().unwrap( HikariDataSource.class )
                                       .close();
            }
        }
        catch ( SQLException e )
        {
            LOGGER.warn( "Unable to close the connection pool." );
        }

        try
        {
            if ( this.connectionSupplier.getHighPriorityConnectionPool().isWrapperFor( HikariDataSource.class ) )
            {
                this.connectionSupplier.getHighPriorityConnectionPool().unwrap( HikariDataSource.class )
                                       .close();
            }
        }
        catch ( SQLException e )
        {
            LOGGER.warn( "Unable to close the high priority connection pool." );
        }
    }

    /**
     * Submits the passed in runnable task for execution
     * @param task The thread whose task to execute
     * @return the result of the execution wrapped in a {@link Future}
     */
    private Future<?> issue( final Runnable task )
    {
        if ( this.sqlTasks == null || this.sqlTasks.isShutdown() )
        {
            this.sqlTasks = createService();
        }

        return this.sqlTasks.submit( task );
    }
}
