package wres.io.utilities;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyIn;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.Range;
import com.zaxxer.hikari.HikariDataSource;

import static java.time.ZoneOffset.UTC;

import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

/**
 * An Interface structure used for organizing database operations and providing
 * common database operations
 */
public class Database {

    private final SystemSettings systemSettings;

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    /**
     * Database types supporting "analyze".
     */

    private static final Set<String> DBMS_WITH_ANALYZE = Set.of( "postgresql",
                                                                 "h2" );

    /**
     * Database types having a "user" function.
     */

    private static final Set<String> DBMS_WITH_USER_FUNCTION = Set.of( "h2",
                                                                       "mariadb",
                                                                       "mysql" );

    /**
     * Database types supporting "limit" clauses.
     */

    private static final Set<String> DBMS_WITH_LIMIT = Set.of( "postgresql",
                                                               "h2",
                                                               "mariadb",
                                                               "sqlite",
                                                               "mysql" );

	/**
	 * The standard priority set of connections to the database
	 */
    private final DataSource connectionPool;

	/**
	 * A higher priority set of connections to the database used for operations
	 * that absolutely need to operate within the database with little to no
	 * competition for resources. Should be used sparingly
	 */
    private final DataSource highPriorityConnectionPool;
    
	/**
	 * A separate thread executor used to schedule database communication
	 * outside of other threads
	 */
    private ThreadPoolExecutor sqlTasks;

	/**
	 * System agnostic newline character used to make generated queries human
	 * readable
	 */
    private static final String NEWLINE = System.lineSeparator();

	/**
	 * A queue containing tasks used to ingest data into the database
     * <br><br>
     * TODO: Make this a collection of futures, not future lists of ingest results.
     * Other things need to occupy this collection that don't contain ingest results
	 */
    private final LinkedBlockingQueue<Future<List<IngestResult>>> storedIngestTasks =
			new LinkedBlockingQueue<>();

    /**
     * Mapping between the number of a forecast value partition and its name
     */
    private final Map<Integer, String> timeSeriesValuePartitionNames =
            new ConcurrentHashMap<>( 163 );

    public Database( SystemSettings systemSettings )
    {
        this.systemSettings = systemSettings;
        this.connectionPool = systemSettings.getConnectionPool();
        this.highPriorityConnectionPool = systemSettings.getHighPriorityConnectionPool();
        this.sqlTasks = createService();
    }

    /**
	 * Adds a task to the ingest queue
	 * @param task The ingest task to add to the queue
	 */
    private void storeIngestTask(Future task)
	{
        this.storedIngestTasks.add(task);
	}

    /**
     * Stores a simple task that will ingest data. The stored task will later be
     * evaluated for completion.
     * @param ingestTask A task that will ingest source data into the database
     * @return The future result of the task
     */
    public Future<?> ingest(WRESRunnable ingestTask)
	{
        Future<?> result = this.execute( ingestTask );
        this.storeIngestTask( result );
		return result;
	}

    /**
     * Stores a simple task that will ingest data. The stored task will later be
     * evaluated for completion.
     * @param <U> The type of value that the ingestTask should return
     * @param ingestTask A task that will ingest source data into the database
     * @return The future result of the task
     */
    public <U> Future<U> ingest(WRESCallable<U> ingestTask)
	{
        Future<U> result = this.submit( ingestTask );
        this.storeIngestTask( result );
		return result;
	}


	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
    private ThreadPoolExecutor createService()
	{
		// Ensures that all created threads will be labeled "Database Thread"
		ThreadFactory factory = runnable -> new Thread(runnable, "Database Thread");
        ThreadPoolExecutor executor = new ThreadPoolExecutor( this.getSystemSettings().getMaximumPoolSize(),
                                                              this.getSystemSettings().getMaximumPoolSize(),
                                                              systemSettings.poolObjectLifespan(),
                                                              TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<>(
                                                                      this.getSystemSettings()
                                                                              .getMaximumPoolSize() * 5),
                                                              factory
		);

		// Ensures that the calling thread runs the new thread logic itself if
        // the upper bound of the executor's internal queue has been hit
		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;
	}

    /**
     * Loops through all stored ingest tasks and ensures that they all complete
	 * @return the list of resulting ingested file identifiers
     * @throws IngestException if the ingest fails
     */
    public List<IngestResult> completeAllIngestTasks() throws IngestException
    {
        LOGGER.trace( "Now completing all issued ingest tasks..." );

        List<IngestResult> result = new ArrayList<>();

        try
        {
            // Make sure that feedback gets displayed
            ProgressMonitor.setShouldUpdate( this.getSystemSettings()
                                                 .getUpdateProgressMonitor() );

            // Process every stored task
            for ( Future<List<IngestResult>> task : this.storedIngestTasks )
            {
                // Tell the client that we're moving on to the next part of work
                ProgressMonitor.increment();

                // If the task hasn't completed, we want to get the results and propagate them
                if ( !task.isDone() )
                {
                    // Get the task
                    List<IngestResult> singleResult = task.get();

                    // Update the monitor, saying that a task has completed
                    ProgressMonitor.completeStep();

                    // If there was a result, add it to the list
                    if ( singleResult != null )
                    {
                        result.addAll( singleResult );
                    }
                    else if ( LOGGER.isTraceEnabled() )
                    {
                        LOGGER.trace( "A null value was returned in the "
                                      + "Database class. Task: {}", task );
                    }
                }
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Ingest task completion was interrupted.", ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            String message = "Could not complete all ingest tasks.";
            throw new IngestException( message, ee );
        }


        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "completeAllIngestTasks returning {} results.",
                          result.size() );
        }

        return Collections.unmodifiableList( result );
    }

	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return the result of the execution wrapped in a {@link Future}
	 */
    public Future<?> execute(final Runnable task)
	{
        if ( sqlTasks == null || sqlTasks.isShutdown())
		{
            sqlTasks = createService();
		}

        return sqlTasks.submit( task);
	}

    /**
     * Submits the passed in Callable for execution
     * @param task The logic to execute
     * @param <V> The return type encompassed by the future result
     * @return The future result of the passed in logic
     */
    public <V> Future<V> submit(Callable<V> task)
	{
        if ( sqlTasks == null || sqlTasks.isShutdown())
		{
            sqlTasks = createService();
		}
        return sqlTasks.submit( task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
    public void shutdown()
	{
        if (!sqlTasks.isShutdown())
		{
            sqlTasks.shutdown();

			// The wait functions for the executor aren't 100% reliable, so we spin until it's done
            while (!sqlTasks.isTerminated());
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
        List<Runnable> abandoned = new ArrayList<>();

        sqlTasks.shutdown();
        try
        {
            sqlTasks.awaitTermination( timeOut, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Database forceShutdown interrupted.", ie );
            List<Runnable> abandonedDbTasks = sqlTasks.shutdownNow();
            abandoned.addAll( abandonedDbTasks );
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedMore = sqlTasks.shutdownNow();
        abandoned.addAll( abandonedMore );
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
        return connectionPool.getConnection();
	}

    /**
     * Checks out a high priority database connection
     * @return A database connection that should have little to no contention
     * @throws SQLException Thrown if a connection could not be retrieved
     */
    public Connection getHighPriorityConnection() throws SQLException
    {
        LOGGER.debug("Retrieving a high priority database connection...");
        return highPriorityConnectionPool.getConnection();
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
            if ( this.connectionPool.isWrapperFor( HikariDataSource.class ) )
            {
                this.connectionPool.unwrap( HikariDataSource.class )
                                   .close();
            }
        }
        catch ( SQLException e )
        {
            LOGGER.warn( "Unable to close the connection pool." );
        }

        try
        {
            if ( this.highPriorityConnectionPool.isWrapperFor( HikariDataSource.class ) )
            {
                this.highPriorityConnectionPool.unwrap( HikariDataSource.class )
                                               .close();
            }
        }
        catch ( SQLException e )
        {
            LOGGER.warn( "Unable to close the high priority connection pool." );
        }
    }
	
    /**
     * Returns a high priority connection to the connection pool
     * @param connection The connection to return
     */
	public static void returnHighPriorityConnection(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                // The implementation of the C3P0 Connection option returns the
                // connection to the pool when "close"d. Despite seeming
                // unneccessary, extra logic may be needed if the implementation
                // changes (for instance, extra logic must be present if C3PO is not
                // used) or for further diagnostic purposes
                connection.close();
                LOGGER.debug("A high priority database operation has completed.");
            }
            catch ( SQLException se )
            {
                // Exception on close should not affect primary outputs.
                LOGGER.warn( "A high priority connection could not be "
                             + "returned to the connection pool properly.",
                             se );
            }
        }
    }


    /**
     * Inserts data into the database (or copies in the case of postgres).
     * @param tableName The table name for the copy or insert statement.
     * @param columnNames The column names in the order the values appear.
     * @param values The values in the order the columnNames appear.
     * @param charColumns True and false in the order of column names/values.
     *                    When true, this is a column needs quoting on insert.
     */

    public void copy( String tableName,
                      List<String> columnNames,
                      List<String[]> values,
                      boolean[] charColumns )
    {
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

        if ( this.getSystemSettings()
                 .getDatabaseType()
                 .equalsIgnoreCase( "postgresql" ) )
        {
            this.pgCopy( tableName,
                         columnNames,
                         values );
        }
        else
        {
            this.insert( tableName,
                         columnNames,
                         values,
                         charColumns );
        }
    }

    private void insert( String tableName,
                         List<String> columnNames,
                         List<String[]> values,
                         boolean[] charColumns )
    {
        StringJoiner columns = new StringJoiner( ",", " ( ", " ) " );

        for ( String column : columnNames )
        {
            columns.add( column );
        }

        String insertHeader = "INSERT INTO " + tableName + columns.toString()
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
        Query query = new Query( this.getSystemSettings(), insertsQuery );
        int rowsModified = -1;

        try ( Connection connection = this.getConnection() )
        {
            rowsModified = query.execute( connection );
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed to insert data into "
                                       + tableName, se );
        }

        if ( rowsModified != values.size() )
        {
            LOGGER.warn( "Expected to insert {} rows but {} were inserted.",
                         values.size(), rowsModified );
        }
    }

    /**
     * Sends a copy statement to the indicated table within a postgres db.
     * @param tableName The table name.
     * @param columnNames The columns consistent with the order of values.
     * @param values The values to copy, outer array is a tuple/row,
     *               inner array is each value in the row (one for each col).
     * @throws IngestException Thrown if an error was encountered when trying to
     * copy data to the database.
     */

    private void pgCopy( String tableName,
                         List<String> columnNames,
                         List<String[]> values )
	{
        StringJoiner columns = new StringJoiner( ",", " ( ", " )" );

        for ( String column : columnNames )
        {
            columns.add( column );
        }

        String table_definition = tableName + columns.toString();
        String delimiter = "|";

        // The format of the copy statement needs to be of the format
        // "COPY wres.TimeSeriesValue_xxxx FROM STDIN WITH DELIMITER '|'"
        String copy_definition = "COPY "
                                 + table_definition
                                 + " FROM STDIN WITH DELIMITER '"
                                 + delimiter + "'";

        final byte[] NULL = "\\N".getBytes( StandardCharsets.UTF_8 );

        try ( Connection connection = this.getConnection() )
		{
            PGConnection pgConnection = connection.unwrap( PGConnection.class );

			// We need specialized functionality to copy, so we need to create a manager object that will
            // handle the copy operation from the postgresql driver
            CopyManager manager = pgConnection.getCopyAPI();

			// Use the manager to stream the data through to the database
            CopyIn copyIn = manager.copyIn( copy_definition );
            byte[] valueDelimiterBytes = delimiter.getBytes( StandardCharsets.UTF_8 );
            byte[] valueRowDelimiterBytes = "\n".getBytes( StandardCharsets.UTF_8 );

            for( String[] row : values )
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
                        copyIn.writeToCopy( NULL, 0, NULL.length );
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
                              copy_definition, allValues.substring( 0, subStringMax ) );
            }

            throw new IngestException( "Data could not be copied to the database.",
                                       e );
		}
	}

    /**
     * Refreshes statistics that the database uses to optimize queries.
     * Performance suffers if the operation is told to vacuum missing values,
     * but the performance of the system as a whole is improved if many values
     * were removed prior to running.
     * @param vacuum Whether or not to remove records pertaining to deleted
     *               values as well
     * @throws SQLException when refresh or adding indices goes wrong
     */
    public void refreshStatistics(boolean vacuum)
            throws SQLException
	{
        String sql;

        final String optionalVacuum;

        if ( vacuum && this.supportsVacuumAnalyze() )
        {
            optionalVacuum = "VACUUM ";
        }
        else
        {
            optionalVacuum = "";
        }

        if ( this.supportsAnalyze() )
        {
            sql = optionalVacuum + "ANALYZE;";
            LOGGER.info( "Analyzing data for efficient execution..." );

            Query query = new Query( this.systemSettings, sql );

            try ( Connection connection = this.getConnection() )
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
                         this.getType() );
        }
    }


    /**
     * Returns the name of the partition of where values for this timeseries
     * should be saved based on lead time.
     * Must be kept in sync with liquibase scripts.
     * @param lead The lead time of this time series where values of interest
     *             should be saved
     * @return The name of the partition where values for the indicated lead time
     * should be saved.
     */

    public String getTimeSeriesValuePartition( int lead )
    {
        // The number of unique lead durations contained within a partition
        // within a postgres database for values linked to a time series.
        final short timeSeriesValuePartitionSpan = 1200;
        int partitionNumber = lead / timeSeriesValuePartitionSpan;

        String name = timeSeriesValuePartitionNames.get( partitionNumber );

        if ( name == null )
        {
            if ( !this.getSystemSettings()
                      .getDatabaseType()
                      .equalsIgnoreCase( "postgresql" ) )
            {
                // WRES only supports partitions on postgresql. In other cases,
                // simply use the plain wres.TimeSeriesValue table.
                return "wres.TimeSeriesValue";
            }

            String partitionNumberWord;

            // Sometimes the lead times are negative, but the dash is not a
            // valid character in a name in sql, so we replace with a word.
            if ( partitionNumber < -10 )
            {
                partitionNumberWord = "Below_Negative_10";
            }
            else if ( partitionNumber > 150 )
            {
                partitionNumberWord = "Above_150";
            }
            else if ( partitionNumber < 0 )
            {
                partitionNumberWord = "Negative_"
                                      + Math.abs( partitionNumber );
            }
            else
            {
                partitionNumberWord = Integer.toString( partitionNumber );
            }

            name = "wres.TimeSeriesValue_Lead_" + partitionNumberWord;

            this.timeSeriesValuePartitionNames.putIfAbsent( partitionNumber, name);
        }

        return name;
    }

    /**
     * Get all partition table names.
     * Needs to be kept in sync with assumptions about liquibase scripts and
     * presence/absence of partition tables.
     * @return The names of all partition tables
     */
    public Set<String> getPartitionTables()
    {
        Set<String> partitionTables = new HashSet<>( 163 );

        // Assumes that there is a fixed quantity of partition tables already.
        // Assumes that the step is 1200, step at half that to hit all tables
        // at least once without worrying about the exact edges.
        for ( int i = -15000; i < 183000; i += 600 )
        {
            String partitionName = this.getTimeSeriesValuePartition( i );
            partitionTables.add( partitionName );
        }

        return Collections.unmodifiableSet( partitionTables );
    }

    /**
     * Runs a single query in the database
     * @param query The query to run
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @return The number of rows modified or returned by the query
     * @throws SQLException Thrown if an issue was encountered while communicating with the database
     */
    int execute( final Query query, final boolean isHighPriority) throws SQLException
    {
        int modifiedRows = 0;

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
    DataProvider getData( final Query query, final boolean isHighPriority) throws SQLException
    {
        // Since Database.buffer performs all the heavy lifting, we can just rely on that. Setting that
        // call in the try statement ensures that it is closed once the in-memory results are created
        try ( Connection connection = this.getConnection( isHighPriority);
              DataProvider rawProvider = this.buffer( connection, query ) )
        {
            return DataSetProvider.from(rawProvider);
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
        return new SQLDataProvider( connection, query.call( connection ) );
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
    <V> V retrieve( final Query query, final String label, final boolean isHighPriority) throws SQLException
    {
        try ( Connection connection = this.getConnection( isHighPriority );
              DataProvider data = this.buffer( connection, query ) )
        {
            if (data.isEmpty())
            {
                return null;
            }
            return data.getValue( label );
        }
    }

    /**
     * Runs the passed in method on every entry within a generated {@link DataProvider}
     * @param query The query that will collect data to feed into the passed method
     * @param consumer The method that will consume the query results
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @throws SQLException Thrown if an error was encountered while communicating with the database
     */
    void consume(
            final Query query,
            ExceptionalConsumer<DataProvider, SQLException> consumer,
            final boolean isHighPriority)
            throws SQLException
    {
        try ( Connection connection = this.getConnection( isHighPriority );
              DataProvider data = this.buffer( connection, query ) )
        {
            data.consume( consumer );
        }
    }

    /**
     * Transforms the results of a query into a list of objects
     * @param query The query to will return data
     * @param interpretor A function that will transform values from a {@link DataProvider} into the desired object
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @param <U> The type of object that the {@link DataProvider} entry will be transformed into
     * @return A list of the transformed items
     * @throws SQLException Thrown if the query encounters an error while communicating with the database
     */
    <U> List<U> interpret( final Query query, ExceptionalFunction<DataProvider, U, SQLException> interpretor, final boolean isHighPriority) throws SQLException
    {
        List<U> result;

        try ( Connection connection = this.getConnection( isHighPriority );
              DataProvider data = this.buffer( connection, query ) )
        {
            result = new ArrayList<>( data.interpret( interpretor ) );
        }

        return result;
    }

    /**
     * Schedules a query to run asynchronously and return a single value
     * @param query The query to run
     * @param label The name of the field containing the value to return
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @param <V> The type of value to return
     * @return A scheduled task that will return the value from the named field
     */
    <V> Future<V> submit( final Query query, final String label, final boolean isHighPriority)
    {
        Database database = this;
        WRESCallable<V> queryToSubmit = new WRESCallable<V>() {
            @Override
            protected V execute() throws SQLException
            {
                return database.retrieve( this.query, this.label, this.isHighPriority );
            }

            @Override
            protected Logger getLogger()
            {
                return LOGGER;
            }

            private WRESCallable<V> init( Database database,
                                          final Query query, final boolean isHighPriority, final String label)
            {
                this.database = database;
                this.query = query;
                this.isHighPriority = isHighPriority;
                this.label = label;
                return this;
            }

            private Database database;
            private Query query;
            private boolean isHighPriority;
            private String label;
        }.init( database, query, isHighPriority, label );

        return this.submit( queryToSubmit );
    }

    /**
     * Schedules a query to run asynchronously with no regard to an result
     * @param query The query to schedule
     * @param isHighPriority Whether or not the query should be run on a high priority connection
     * @return The record for the scheduled task
     */
    Future<?> issue(final Query query, final boolean isHighPriority)
    {
        Database database = this;
        WRESRunnable queryToIssue = new WRESRunnable() {
            @Override
            protected void execute() throws SQLException
            {
                database.execute( this.query, this.isHighPriority );
            }

            @Override
            protected Logger getLogger()
            {
                return LOGGER;
            }

            WRESRunnable init( Database database,
                               final Query query, final boolean isHighPriority)
            {
                this.database = database;
                this.query = query;
                this.isHighPriority = isHighPriority;
                return this;
            }

            private Database database;
            private Query query;
            private boolean isHighPriority;
        }.init( database, query, isHighPriority);

        return this.execute( queryToIssue );
    }

    /**
     * Removes all user data from the database
     * TODO: This should probably accept an object or list to allow for the removal of business logic
     * Assumes that locking has already been done at a higher level by caller(s)
     * @throws SQLException Thrown if successful communication with the
     * database could not be established
     */
    public void clean() throws SQLException
    {
        StringJoiner builder;
        Set<String> partitions = this.getPartitionTables();

        if ( this.getType()
                 .equals( "h2" ) )
        {
             builder = new StringJoiner( NEWLINE,
                                         "SET REFERENTIAL_INTEGRITY FALSE;" + NEWLINE,
                                         NEWLINE + "SET REFERENTIAL_INTEGRITY TRUE;" );
        }
        else
        {
            builder = new StringJoiner( NEWLINE );
        }

		for (String partition : partitions)
        {
            builder.add( "TRUNCATE TABLE " + partition + ";" );
        }

        List<String> tables = List.of( "wres.Source",
                                       "wres.TimeSeries",
                                       "wres.Ensemble",
                                       "wres.Project",
                                       "wres.ProjectSource",
                                       "wres.Feature",
                                       "wres.NetcdfCoordinate",
                                       "wres.GridProjection" );

        for ( String table : tables )
        {
            if ( this.supportsTruncateCascade() )
            {
                builder.add( "TRUNCATE TABLE " + table + " CASCADE;" );
            }
            else
            {
                builder.add( "TRUNCATE TABLE " + table + ";" );
            }
        }

        builder.add( "INSERT INTO wres.Ensemble ( ensemble_name ) VALUES ('default');" );

        Query query = new Query( this.systemSettings, builder.toString() );

		try
        {
            this.execute( query, false );
		}
		catch (final SQLException e)
        {
			String message = "WRES data could not be removed from the database."
                             + NEWLINE + NEWLINE
                             + builder.toString();
			// Decorate with contextual information.
			throw new SQLException( message, e );
		}
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
     * Expose SystemSettings for the sake of DataScripter
     * @return the system settings this database instance uses.
     */
    SystemSettings getSystemSettings()
    {
        return this.systemSettings;
    }


    /**
     * Logs information about the execution of the WRES into the database for
     * aid in remote debugging.
     * Moved from {@link wres.io.Operations} 2021-03-15, see history there.
     * @param arguments The arguments used to run the WRES, at least two
     * @param projectName the project name
     * @param hash the hash of the project datasets
     * @param executionInterval The instants at which the WRES began and ended execution, not null
     * @param failed Whether or not the execution failed
     * @param error Any error that caused the WRES to crash
     * @param version The top-level version of WRES (module versions vary), not null
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if there are zero arguments
     */
    public void logExecution( String[] arguments,
                              String projectName,
                              String hash,
                              Range<Instant> executionInterval,
                              boolean failed,
                              String error,
                              String version )
    {
        Objects.requireNonNull( arguments );
        Objects.requireNonNull( version );
        Objects.requireNonNull( executionInterval );

        if( arguments.length < 1 )
        {
            throw new IllegalArgumentException( "Cannot log an execution with zero arguments." );
        }
        
        try
        {
            LocalDateTime startedAtZulu = LocalDateTime.ofInstant( executionInterval.lowerEndpoint(), UTC );
            LocalDateTime endedAtZulu = LocalDateTime.ofInstant( executionInterval.upperEndpoint(), UTC );

            // For any arguments that happen to be regular files, read the
            // contents of the first file into the "project" field. Maybe there
            // is an improvement that can be made, but this should cover the
            // common case of a single file in the args.
            String project = "";

            // The two operations that might perform a project related operation are 'execute' and 'ingest';
            // these are the only cases where we might be interested in a project configuration
            String testArg = arguments[0].toLowerCase();
            if ( "execute".equals( testArg ) || "ingest".equals( testArg ) ) 
            {

                // Go ahead and assign the second argument as the project;
                // if this instance is in server mode,
                // this will be the raw project text and a file path will not be involved
                project = arguments[1];

                // Look through the arguments to find the path to a file;
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

            DataScripter script = new DataScripter( this );

            script.addLine("INSERT INTO wres.ExecutionLog (");
            script.addTab().addLine("arguments,");
            script.addTab().addLine("system_version,");
            script.addTab().addLine("project,");
            script.addTab().addLine( "project_name," );
            script.addTab().addLine("hash,");
            script.addTab().addLine("username,");
            script.addTab().addLine("address,");
            script.addTab().addLine("start_time,");
            script.addTab().addLine("end_time,");
            script.addTab().addLine("failed,");
            script.addTab().addLine("error");
            script.addLine(")");
            script.addLine("VALUES (");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");

            if ( this.getType()
                     .equals( "postgresql" ) )
            {
                script.addTab().addLine( "inet_client_addr()," );
            }
            else if ( this.supportsUserFunction() )
            {
                script.addTab().addLine( "user()," );
            }
            else
            {
                script.addTab().addLine( "NULL," );
            }

            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?,");
            script.addTab().addLine("?");
            script.addLine(");");

            script.execute(
                    String.join(" ", arguments),
                    version,
                    project,
                    projectName,
                    hash,
                    System.getProperty( "user.name" ),
                    // Let server find and report network address
                    startedAtZulu,
                    endedAtZulu,
                    failed,
                    error
            );
        }
        catch ( SQLException | IOException e )
        {
            LOGGER.warn( "Execution metadata could not be logged to the database.",
                         e );
        }
    }

    private String getType()
    {
        return this.getSystemSettings()
                   .getDatabaseType()
                   .toLowerCase();
    }

    private boolean supportsVacuumAnalyze()
    {
        return this.getType()
                   .equals( "postgresql" );
    }

    private boolean supportsAnalyze()
    {
        String type = this.getType();
        return DBMS_WITH_ANALYZE.contains( type );
    }

    private boolean supportsUserFunction()
    {
        String type = this.getType();
        return DBMS_WITH_USER_FUNCTION.contains( type );
    }

    private boolean supportsTruncateCascade()
    {
        return this.getType()
                   .equals( "postgresql" );
    }

    boolean supportsLimit()
    {
        String type = this.getType();
        return DBMS_WITH_LIMIT.contains( type );
    }
}
