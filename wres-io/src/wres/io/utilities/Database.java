package wres.io.utilities;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.data.details.TimeSeries;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.Strings;
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
	 * The standard priority set of connections to the database
	 */
    private final HikariDataSource connectionPool;

	/**
	 * A higher priority set of connections to the database used for operations
	 * that absolutely need to operate within the database with little to no
	 * competition for resources. Should be used sparingly
	 */
    private final HikariDataSource highPriorityConnectionPool;

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
        ThreadPoolExecutor executor = new ThreadPoolExecutor( connectionPool.getMaximumPoolSize(),
                                                              connectionPool.getMaximumPoolSize(),
                                                              systemSettings.poolObjectLifespan(),
                                                              TimeUnit.MILLISECONDS,
                                                              new ArrayBlockingQueue<>(
                                                                      connectionPool
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
            ProgressMonitor.setShouldUpdate( true );

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

		// Close out our database connection pools
        connectionPool.close();
        highPriorityConnectionPool.close();
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
            connectionPool.close();
            highPriorityConnectionPool.close();
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedMore = sqlTasks.shutdownNow();
        abandoned.addAll( abandonedMore );
        connectionPool.close();
        highPriorityConnectionPool.close();
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
	 * Returns the connection to the connection pool.
	 * @param connection The connection to return
	 */
	private static void returnConnection( Connection connection )
	{
	    if (connection != null) {
	        // The implementation of the C3P0 Connection option returns the
            // connection to the pool when "close"d. Despite seeming
            // unneccessary, extra logic may be needed if the implementation
            // changes (for instance, extra logic must be present if C3PO is not
            // used) or for further diagnostic purposes
	        try
            {
                connection.close();
            }
            catch( SQLException se )
            {
                // Exception on close should not affect primary outputs.
               LOGGER.warn( "A connection could not be returned to the "
                            + "connection pool properly.",
                             se );
            }
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
     * Sends a copy statement to the indicated table within the database
     * @param table_definition The definition of a table and its columns (i.e.
     *                         "table_1 (column_1, column_2, column_3)"
     * @param values The series of values to copy, delimited by the passed in
     *               delimiter, with each prospective row separated by new lines.
     *               (i.e. "val1|val2|val2")
     * @param delimiter The delimiter separating values per line. (i.e. "|").
     *                  Despite being common, commas should be avoided.
     * @throws CopyException Thrown if an error was encountered when trying to
     * copy data to the database.
     */
    public void copy( final String table_definition,
                      final String values,
                      String delimiter )
            throws CopyException
	{
	    // TODO: This is Postgres specific; this needs to either come from a different source or switch
        //  to an insert statement if the database is non-postgresql (i.e. H2)

        try ( Connection connection = this.getConnection();
              PushbackReader reader = new PushbackReader(new StringReader(""), values.length() + 1000) )
		{
            PGConnection pgConnection = connection.unwrap( PGConnection.class );

			// We need specialized functionality to copy, so we need to create a manager object that will
            // handle the copy operation from the postgresql driver
            CopyManager manager = pgConnection.getCopyAPI();

			// The format of the copy statement needs to be of the format
            // "COPY wres.TimeSeriesValue_xxxx FROM STDIN WITH DELIMITER '|'"
			String copy_definition = "COPY " + table_definition + " FROM STDIN WITH DELIMITER ";

			// Make sure that the delimiter starts with a single quote
			if (!delimiter.startsWith("'"))
			{
				delimiter = "'" + delimiter;
			}

			// Make sure the delimiter ends with a single quote
			if (!delimiter.endsWith("'"))
			{
				delimiter += "'";
			}

			// Add the delimiter to finish the definition
			copy_definition += delimiter;

			// Create the reader that we'll feed the data through; make sure there is plenty of space to help
            // ensure that all of the data makes it through

			reader.unread(values.toCharArray());

			// Use the manager to stream the data through to the database
			manager.copyIn(copy_definition, reader);
		}
        catch ( SQLException | IOException e )
		{
		    // If we are in a non-production environment, it would help to see the format of the data
            // that couldn't be added
		    if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Data could not be copied to the database:{}{}",
                              Strings.truncate( values ), NEWLINE );
            }
			throw new CopyException( "Data could not be copied to the database.",
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
        List<String> sql = new ArrayList<>();

        final String optionalVacuum;

        if (vacuum)
        {
            optionalVacuum = "VACUUM ";
        }
        else
        {
            optionalVacuum = "";
        }

		LOGGER.info("Analyzing data for efficient execution...");


        String script =
                "SELECT 'ANALYZE '||n.nspname ||'.'|| c.relname||';' AS alyze"
                + NEWLINE +
                "FROM pg_catalog.pg_class c" + NEWLINE +
                "INNER JOIN pg_catalog.pg_namespace n" + NEWLINE +
                "     ON N.oid = C.relnamespace" + NEWLINE +
                "WHERE relchecks > 0" + NEWLINE +
                "     AND (nspname = 'wres' OR nspname = 'partitions')"
                + NEWLINE +
                "     AND relkind = 'r';";

        this.consume(
                new Query( this.systemSettings, script),
                provider -> sql.add(optionalVacuum + provider.getString( "alyze" )),
                false
        );

        // TODO: We should probably just analyze/optional vacuum everything in the WRES schema rather than picking and choosing
        sql.add(optionalVacuum + "ANALYZE wres.TimeSeries;");
        sql.add(optionalVacuum + "ANALYZE wres.ProjectSource;");
        sql.add(optionalVacuum + "ANALYZE wres.Source;");
        sql.add(optionalVacuum + "ANALYZE wres.Ensemble;");

        List<Future<?>> queries = new ArrayList<>();

        for (String statement : sql)
        {
            Query query = new Query( this.systemSettings, statement );
            queries.add( this.issue( query, false ) );
        }

        boolean analyzeFailed = true;

        for (Future<?> query : queries)
        {
            try
            {
                query.get();
                analyzeFailed = false;
            }
            catch ( ExecutionException e )
            {
                LOGGER.warn( "A data optimization statement could not be completed.",
                             e);
            }
            catch (InterruptedException e)
			{
				LOGGER.warn( "Interrupted while running a data optimization statement.",
                             e );
				Thread.currentThread().interrupt();
			}
        }

        if (analyzeFailed)
        {
            throw new SQLException( "Data in the database could not be "
                                    + "analyzed for efficient execution." );
        }

        LOGGER.info("Database statistical analysis is now complete.");
	}

    /**
     * Get all partition table names.
     * Needs to be kept in sync with assumptions about liquibase scripts and
     * presence/absence of partition tables.
     * @return The names of all partition tables
     */
    public static Set<String> getPartitionTables()
    {
        Set<String> partitionTables = new HashSet<>( 163 );

        // Assumes that there is a fixed quantity of partition tables already.
        // Assumes that the step is 1200, step at half that to hit all tables
        // at least once without worrying about the exact edges.
        for ( int i = -15000; i < 183000; i += 600 )
        {
            String partitionName = TimeSeries.getTimeSeriesValuePartition( i );
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
        Connection connection = null;

        try
        {
            if (isHighPriority)
            {
                connection = this.getHighPriorityConnection();
            }
            else
            {
                connection = this.getConnection();
            }

            modifiedRows = query.execute( connection );
        }
        finally
        {
            if (connection != null)
            {
                if (isHighPriority)
                {
                    Database.returnHighPriorityConnection( connection );
                }
                else
                {
                    Database.returnConnection( connection );
                }
            }
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
        try ( DataProvider rawProvider = this.buffer(query, isHighPriority) )
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
     * @param isHighPriority Whether or not a high priority connection is required
     * @return A record of the results of the database call
     * @throws SQLException Thrown if there was an error when connecting to the database
     */
    DataProvider buffer(final Query query, final boolean isHighPriority) throws SQLException
    {
        Connection connection;

        if (isHighPriority)
        {
            connection = this.getHighPriorityConnection();
        }
        else
        {
            connection = this.getConnection();
        }

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
        try( DataProvider data = this.buffer( query, isHighPriority ) )
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
        try ( DataProvider data = this.buffer( query, isHighPriority) )
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

        try ( DataProvider data = this.buffer( query, isHighPriority) )
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
    Future issue(final Query query, final boolean isHighPriority)
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
		StringBuilder builder = new StringBuilder();

        Set<String> partitions = Database.getPartitionTables();

		for (String partition : partitions)
        {
            builder.append( "TRUNCATE TABLE " )
				   .append( partition)
				   .append( ";" )
				   .append( NEWLINE );
        }

		builder.append("TRUNCATE wres.TimeSeriesValue CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Source RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeries RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Variable RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.VariableFeature RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Ensemble RESTART IDENTITY CASCADE;");
		builder.append("INSERT INTO wres.Ensemble(ensemble_name) VALUES ('default');");
		builder.append("TRUNCATE wres.Project RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.ProjectSource RESTART IDENTITY CASCADE;").append(NEWLINE);

		try
        {
            this.execute( new Query( this.systemSettings, builder.toString() ), false );
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
     * @return A reference to the standard connection pool
     */
    HikariDataSource getPool()
    {
        return this.connectionPool;
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
}
