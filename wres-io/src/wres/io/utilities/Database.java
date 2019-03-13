package wres.io.utilities;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.FormattedStopwatch;
import wres.util.FutureQueue;
import wres.util.Strings;
import wres.util.functional.ExceptionalConsumer;
import wres.util.functional.ExceptionalFunction;

/**
 * An Interface structure used for organizing database operations and providing
 * common database operations
 */
public final class Database {
    
    private Database(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

	private static final Integer MUTATION_LOCK_KEY = 126357;

	private static final DatabaseLockManager DATABASE_LOCK_MANAGER =
            new DatabaseLockManager( new DatabaseConnectionSupplier() );

	/**
	 * The standard priority set of connections to the database
	 */
	private static final ComboPooledDataSource CONNECTION_POOL =
			SystemSettings.getConnectionPool();

	/**
	 * A higher priority set of connections to the database used for operations
	 * that absolutely need to operate within the database with little to no
	 * competition for resources. Should be used sparingly
	 */
    private static final ComboPooledDataSource HIGH_PRIORITY_CONNECTION_POOL =
			SystemSettings.getHighPriorityConnectionPool();

	/**
	 * A separate thread executor used to schedule database communication
	 * outside of other threads
	 */
	private static ThreadPoolExecutor SQL_TASKS = createService();

	/**
	 * System agnostic newline character used to make generated queries human
	 * readable
	 */
    private static final String NEWLINE = System.lineSeparator();

	/**
	 * The function used to copy data to the database
	 */
	private static Method copyAPI = null;

	/**
	 * @return The function used within the given implementation of the
	 * JDBC PostgreSQL driver to copy values directly into the database rather
	 * than inserting them
	 * @throws NoSuchMethodException Thrown if the current implementation of
	 * the JDBC PostgreSQL driver does not have the ability to copy values
	 */
    private static Method getCopyAPI() throws NoSuchMethodException
    {
		if (copyAPI == null)
		{
			copyAPI = PGConnection.class.getMethod("getCopyAPI");
		}
		return copyAPI;
	}

	/**
	 * A queue containing tasks used to ingest data into the database
     * <br><br>
     * TODO: Make this a collection of futures, not future lists of ingest results.
     * Other things need to occupy this collection that don't contain ingest results
	 */
    private static final LinkedBlockingQueue<Future<List<IngestResult>>> storedIngestTasks =
			new LinkedBlockingQueue<>();

	/**
	 * @return Either the first value in the ingest queue or null if none exist
	 */
    public static Future<List<IngestResult>> getStoredIngestTask()
    {
		return storedIngestTasks.poll();
	}

	/**
	 * Adds a task to the ingest queue
	 * @param task The ingest task to add to the queue
	 */
	public static void storeIngestTask(Future task)
	{
		Database.storedIngestTasks.add(task);
	}

    /**
     * Stores a simple task that will ingest data. The stored task will later be
     * evaluated for completion.
     * @param ingestTask A task that will ingest source data into the database
     * @return The future result of the task
     */
	public static Future<?> ingest(WRESRunnable ingestTask)
	{
		Future<?> result = Database.execute( ingestTask );
		Database.storeIngestTask( result );
		return result;
	}

    /**
     * Stores a simple task that will ingest data. The stored task will later be
     * evaluated for completion.
     * @param <U> The type of value that the ingestTask should return
     * @param ingestTask A task that will ingest source data into the database
     * @return The future result of the task
     */
	public static <U> Future<U> ingest(WRESCallable<U> ingestTask)
	{
		Future<U> result = Database.submit( ingestTask );
		Database.storeIngestTask( result );
		return result;
	}


	/**
	 * Loads the metadata for each saved index and reinstates them within the
	 * database
     * TODO: It might be appropriate to move this out into another class
     * @throws SQLException when script execution or gets from resultsets fail
     * @throws InterruptedException when an underlying task is interrupted
     * @throws ExecutionException when an underlying task throws an exception
	 */

	public static void addNewIndexes() throws SQLException,
            InterruptedException, ExecutionException
	{
	    final boolean isHighPriority = false;

	    try
        {
            // Remove queued indexes for tables that don't exist
            ScriptBuilder script = new ScriptBuilder(  );
            script.addLine("DELETE FROM wres.IndexQueue IQ");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM INFORMATION_SCHEMA.TABLES T");
            script.addTab().addLine("WHERE LOWER(T.table_schema || '.' || T.table_name) = LOWER(IQ.table_name)");
            script.addLine(");");

            Database.execute( Query.withScript( script.toString() ), isHighPriority );
        }
        catch ( SQLException e )
        {
            // We don't rethrow here because it just means that there was some
            // garbage floating around that we don't care about
            LOGGER.warn( "Invalid dynamic indexes could not be removed from the queue." );
        }

	    // TODO: See if a FutureQueue would work
        LinkedList<Future<?>> indexTasks = new LinkedList<>();

        // If we're tracing, we want to have a stopwatch we can start and stop
        FormattedStopwatch watch = null;

        if (LOGGER.isTraceEnabled())
		{
			watch = new FormattedStopwatch();
			watch.start();
		}

        try(DataProvider data = Database.getData( Query.withScript( "SELECT * FROM wres.IndexQueue;" ), isHighPriority ))
        {
            while ( data.next() )
            {
                ScriptBuilder script = new ScriptBuilder(  );
                script.addLine("CREATE INDEX IF NOT EXISTS ", data.getString( "index_name" ));
                script.addTab().addLine("ON ", data.getString( "table_name" ));
                script.addTab().addLine("USING ", data.getString( "method" ));
                script.addTab().addLine(data.getString("column_definition"), ";");

                indexTasks.add(Database.issue( Query.withScript( script.toString() ), isHighPriority ));

                script = new ScriptBuilder(  );
                script.addLine("DELETE FROM wres.IndexQueue");
                script.add("WHERE indexqueue_id = ", data.getInt( "indexqueue_id" ), ";");

                indexTasks.add(Database.issue( Query.withScript( script.toString() ), isHighPriority ));
            }
        }

        // If there's at least one command to add an index, tell the client
        if (indexTasks.peek() != null)
        {
            LOGGER.info("Restoring Indices...");
            ProgressMonitor.setSteps( (long)indexTasks.size() );
        }

		Future<?> task;

        // Complete each index creation task
		while ((task = indexTasks.poll()) != null)
        {
            task.get();
            ProgressMonitor.completeStep();
        }

		// If we're tracing and a stopwatch was created express the amount of time that has elapsed
        if (LOGGER.isTraceEnabled() && watch != null)
        {
            watch.stop();
            LOGGER.trace("It took {} to restore all indexes in the database.",
                         watch.getFormattedDuration());
        }
	}

    /**
     * Saves metadata about an index that needs to be added to the database
     * @param tableName The name of the table that the index will belong to
     * @param indexName The name of the index to instate
     * @param indexDefinition The definition of the index
     * @throws SQLException when query fails
     */
	public static void saveIndex(String tableName, String indexName, String indexDefinition)
            throws SQLException
    {
        // Index definitions are wrapped in parenthesis, so ensure it has both a front and a back
		if (!indexDefinition.startsWith("("))
		{
			indexDefinition = "(" + indexDefinition;
		}

		if (!indexDefinition.endsWith(")"))
        {
            indexDefinition += ")";
        }

		// Create the insert script
        ScriptBuilder script = new ScriptBuilder(  );
		script.addLine("INSERT INTO wres.IndexQueue (table_name, index_name, column_definition, method)");
		script.addLine("VALUES (");
		script.addTab().addLine("'", tableName, "',");
		script.addTab().addLine("'", indexName, "',");
		script.addTab().addLine("'", indexDefinition, "',");
		script.addTab().addLine("'btree'");
		script.add(");");

		try
        {
            Database.execute( Query.withScript( script.toString() ), false);
		}
		catch ( SQLException e )
		{
		    // Whether or not this is a failure state is debatable
            String message = "Could not store metadata about the index '"
                             + indexName + "' in the database.";
            // Decorate with some additional information, propagate.
            throw new SQLException( message, e );
		}
    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ThreadPoolExecutor createService()
	{
		// Ensures that all created threads will be labeled "Database Thread"
		ThreadFactory factory = runnable -> new Thread(runnable, "Database Thread");
		ThreadPoolExecutor executor = new ThreadPoolExecutor(CONNECTION_POOL.getMaxPoolSize(),
                                                             CONNECTION_POOL.getMaxPoolSize(),
                                                             SystemSettings.poolObjectLifespan(),
                                                             TimeUnit.MILLISECONDS,
															 new ArrayBlockingQueue<>(CONNECTION_POOL.getMaxPoolSize() * 5),
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
    public static List<IngestResult> completeAllIngestTasks() throws IngestException
    {
        LOGGER.trace( "Now completing all issued ingest tasks..." );

        List<IngestResult> result = new ArrayList<>();

		Future<List<IngestResult>> task;

		try
		{
		    // Make sure that feedback gets displayed
		    ProgressMonitor.setShouldUpdate( true );

		    // Grab the first task
            task = getStoredIngestTask();

            // While there are tasks to execute,
            while ( task != null )
            {
                // Tell the client that we're moving on to the next part of work
                ProgressMonitor.increment();

                // If the task hasn't completed, we want to get the results and propagate them
                if (!task.isDone())
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

                // Get the next task
                task = getStoredIngestTask();
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
	public static Future<?> execute(final Runnable task)
	{
		if (SQL_TASKS == null || SQL_TASKS.isShutdown())
		{
			SQL_TASKS = createService();
		}

		return SQL_TASKS.submit(task);
	}

    /**
     * Submits the passed in Callable for execution
     * @param task The logic to execute
     * @param <V> The return type encompassed by the future result
     * @return The future result of the passed in logic
     */
	public static <V> Future<V> submit(Callable<V> task)
	{
		if (SQL_TASKS == null || SQL_TASKS.isShutdown())
		{
			SQL_TASKS = createService();
		}
		return SQL_TASKS.submit(task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
	public static void shutdown()
	{
		if (!SQL_TASKS.isShutdown())
		{
			SQL_TASKS.shutdown();

			// The wait functions for the executor aren't 100% reliable, so we spin until it's done
			while (!SQL_TASKS.isTerminated());
        }

		// Close out our database connection pools
        CONNECTION_POOL.close();
        HIGH_PRIORITY_CONNECTION_POOL.close();
        DATABASE_LOCK_MANAGER.shutdown();
	}


    /**
     * Shuts down after all tasks have completed, or after timeout is reached,
     * whichever comes first. Tasks may be interrupted and abandoned.
     * @param timeOut the desired maximum wait, measured in timeUnit
     * @param timeUnit the unit for timeOut
     * @return the list of abandoned tasks
     */

    public static List<Runnable> forceShutdown( long timeOut,
                                                TimeUnit timeUnit )
    {
        List<Runnable> abandoned = new ArrayList<>();

        SQL_TASKS.shutdown();
        try
        {
            SQL_TASKS.awaitTermination( timeOut, timeUnit );
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Database forceShutdown interrupted.", ie );
            List<Runnable> abandonedDbTasks = SQL_TASKS.shutdownNow();
            abandoned.addAll( abandonedDbTasks );
            CONNECTION_POOL.close();
            HIGH_PRIORITY_CONNECTION_POOL.close();
            Thread.currentThread().interrupt();
        }

        List<Runnable> abandonedMore = SQL_TASKS.shutdownNow();
        abandoned.addAll( abandonedMore );
        CONNECTION_POOL.close();
        HIGH_PRIORITY_CONNECTION_POOL.close();
        DATABASE_LOCK_MANAGER.shutdown();
        return abandoned;
    }

    /**
     * Checks out a database connection
     * @return A database connection from the standard connection pool
     * @throws SQLException Thrown if a connection could not be retrieved
     */
	public static Connection getConnection() throws SQLException
	{
		return CONNECTION_POOL.getConnection();
	}

    /**
     * Checks out a high priority database connection
     * @return A database connection that should have little to no contention
     * @throws SQLException Thrown if a connection could not be retrieved
     */
	public static Connection getHighPriorityConnection() throws SQLException
    {
        LOGGER.debug("Retrieving a high priority database connection...");
        return HIGH_PRIORITY_CONNECTION_POOL.getConnection();
    }
	
	/**
	 * Returns the connection to the connection pool.
	 * @param connection The connection to return
	 */
	public static void returnConnection(Connection connection)
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
	public static void copy(final String table_definition,
                            final String values,
                            String delimiter)
            throws CopyException
	{
	    // TODO: This is Postgres specific; this needs to either come from a different source or switch
        //  to an insert statement if the database is non-postgresql (i.e. H2)

		Connection connection = null;
		PushbackReader reader = null;
		final String copyAPIMethodName = "getCopyAPI";

		try
		{
			connection = getConnection();
			C3P0ProxyConnection proxy = (C3P0ProxyConnection)connection;
			Object[] arg = new Object[]{};

			// We need specialized functionality to copy, so we need to create a manager object that will
            // handle the copy operation from the postgresql driver
			CopyManager manager = (CopyManager)proxy.rawConnectionOperation(getCopyAPI(),
																			C3P0ProxyConnection.RAW_CONNECTION, arg);

			// The format of the copy statement needs to be of the format
            // "COPY wres.Observation FROM STDIN WITH DELIMITER '|'"
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
			reader = new PushbackReader(new StringReader(""), values.length() + 1000);
			reader.unread(values.toCharArray());

			// Use the manager to stream the data through to the database
			manager.copyIn(copy_definition, reader);

		}
		catch (NoSuchMethodException noMethod)
		{
		    // We want to make sure we record the incorrect API name so that we can cross-reference it for debugging
		    String message = "The method used to create the copy manager ('" +
                    copyAPIMethodName +
                    "') could not be retrieved from the PostgreSQL connection class.";
			throw new CopyException(message, noMethod);
		}
        catch (InvocationTargetException e)
        {
            // While similar to the above, we want to know for sure that we were at least able to access the method
            String message = "The dynamically retrieved method '" + copyAPIMethodName +
                             "' threw an exception upon execution.";
            throw new CopyException(message, e);
        }
		catch (SQLException | IOException | IllegalAccessException error)
		{
		    // If we are in a non-production environment, it would help to see the format of the data
            // that couldn't be added
		    if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Data could not be copied to the database:{}{}",
                              Strings.truncate( values ), NEWLINE );
            }
			throw new CopyException( "Data could not be copied to the database.",
                                     error);
		}
        finally
		{
		    // If we managed to create the reader, we need to close it so we don't have a memory leak
			if (reader != null)
			{
				try
                {
					reader.close();
				}
				catch (IOException e)
                {
                    // While it isn't optimal that we couldn't close the reader, it's not necessarily a showstopper
					LOGGER.warn("The reader for copy values could not be properly closed.");
				}
			}

			// If we did indeed get a connection, we need to make sure it ends up returning to the pool
			if (connection != null)
			{
				Database.returnConnection(connection);
			}
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
	public static void refreshStatistics(boolean vacuum)
            throws SQLException
	{
	    // If we plan to reclaim dead tuples, we want to first make sure that all indexes planned
        // for addition have been properly added
        if (vacuum)
        {
            try
            {
                Database.addNewIndexes();
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while refreshing database statistics.",
                             ie );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                // This might not actually be a SQLException underneath, but
                // for now, translate to one until we figure out a better way.
                throw new SQLException( "Could not add indices.", ee );
            }
        }

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

        Database.consume(
                Query.withScript(script),
                provider -> sql.add(optionalVacuum + provider.getString( "alyze" )),
                false
        );

        // TODO: We should probably just analyze/optional vacuum everything in the WRES schema rather than picking and choosing
        sql.add(optionalVacuum + "ANALYZE wres.Observation;");
        sql.add(optionalVacuum + "ANALYZE wres.TimeSeries;");
        sql.add(optionalVacuum + "ANALYZE wres.TimeSeriesSource;");
        sql.add(optionalVacuum + "ANALYZE wres.ProjectSource;");
        sql.add(optionalVacuum + "ANALYZE wres.Source;");
        sql.add(optionalVacuum + "ANALYZE wres.Ensemble;");

        List<Future<?>> queries = new ArrayList<>();

        for (String statement : sql)
        {
            queries.add( Database.issue( Query.withScript( statement ), false ) );
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
     * Get all partition tables matching the given pattern
     * TODO: Evaluate how strongly coupled this is to business logic and remove it if it isn't appropriate here
     * @param tablePattern A pattern that will match the name of all partition tables of interest
     * @return The names of all partition tables
     * @throws SQLException Thrown if the query could not complete
     */
	private static Collection<String> getPartitionTables(final String tablePattern) throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT N.nspname || '.' || C.relname AS table_name" );
        script.addLine( "FROM pg_catalog.pg_class C" );
        script.addLine( "INNER JOIN pg_catalog.pg_namespace N" );
        script.addTab().addLine( "ON N.oid = C.relnamespace" );
        script.addLine( "WHERE relchecks > 0" );
        script.addTab().addLine( "AND (N.nspname = 'wres')" );

        if (tablePattern != null)
        {
            script.addTab().addLine( "AND C.relname LIKE '", tablePattern, "'" );
        }

        script.addTab().addLine( "AND relkind = 'r';" );

        try
        {
            return Database.interpret(
                    Query.withScript(script.toString()),
                    tableRow -> tableRow.getString( "table_name" ),
                    false
            );
        }
        catch ( SQLException e )
        {
            throw new SQLException(
                    "A list of partition tables to evaluate "
                    + "could not be loaded.",
                    e );
        }
    }

    /**
     * Removes all data from the database that isn't properly linked to a project
     * TODO: Remove the business logic from this class
     * @return Whether or not values were removed
     * @throws SQLException Thrown when one of the required scripts could not complete
     */
    @SuppressWarnings( "unchecked" )
	public static boolean removeOrphanedData() throws SQLException
    {
        try
        {
            if (!Database.thereAreOrphanedValues())
            {
                return false;
            }

            // Can't lock for mutation here because we'd also need to unlock, but this
            // operation will occur alongside other operations that need that lock.

            LOGGER.info("Incomplete data has been detected. Incomplete data "
                        + "will now be removed to ensure that all data operated "
                        + "upon is valid.");

            Collection<String> partitionTables = Database.getPartitionTables( "timeseriesvalue_lead%" );

            // We aren't actually going to collect the results so raw types are fine.
            FutureQueue removalQueue = new FutureQueue(  );

            for (String partition : partitionTables)
            {
                ScriptBuilder valueRemover = new ScriptBuilder();
                valueRemover.addLine( "DELETE FROM ", partition, " P" );
                valueRemover.addLine( "WHERE NOT EXISTS (");
                valueRemover.addTab().addLine( "SELECT 1");
                valueRemover.addTab().addLine( "FROM wres.TimeSeriesSource TSS");
                valueRemover.addTab().addLine( "INNER JOIN wres.ProjectSource PS");
                valueRemover.addTab(  2  ).addLine( "ON PS.source_id = TSS.source_id");
                valueRemover.addTab().addLine( "WHERE TSS.timeseries_id = P.timeseries_id");
                valueRemover.addTab(  2  ).addLine( "AND (TSS.lead IS NULL OR TSS.lead = P.lead)");
                valueRemover.add(");");

                removalQueue.add(Database.issue( Query.withScript( valueRemover.toString() ), false));

                LOGGER.debug("Started task to remove orphaned values in {}...", partition);
            }

            ScriptBuilder removalScript = new ScriptBuilder(  );
            removalScript.addLine("DELETE FROM wres.Observation O");
            removalScript.addLine("WHERE NOT EXISTS (");
            removalScript.addTab().addLine("SELECT 1");
            removalScript.addTab().addLine("FROM wres.ProjectSource PS");
            removalScript.addTab().addLine("WHERE PS.source_id = O.source_id");
            removalScript.add(");");

            removalQueue.add(Database.issue( Query.withScript( removalScript.toString() ), false));

            LOGGER.debug("Started task to remove orphaned observations...");

            try
            {
                removalQueue.loop();
            }
            catch ( ExecutionException e )
            {
                throw new SQLException( "Orphaned observed and forecasted values could not be removed.", e );
            }

            removalScript = new ScriptBuilder(  );

            removalScript.addLine("DELETE FROM wres.TimeSeriesSource TSS");
            removalScript.addLine("WHERE NOT EXISTS (");
            removalScript.addTab().addLine("SELECT 1");
            removalScript.addTab().addLine("FROM wres.ProjectSource PS");
            removalScript.addTab().addLine("WHERE PS.source_id = TSS.source_id");
            removalScript.addLine(");");

            LOGGER.debug("Removing orphaned TimeSeriesSource Links...");
            Database.execute( Query.withScript( removalScript.toString() ), false);

            LOGGER.debug("Removed orphaned TimeSeriesSource Links");

            removalScript = new ScriptBuilder(  );

            removalScript.addLine("DELETE FROM wres.TimeSeries TS");
            removalScript.addLine("WHERE NOT EXISTS (");
            removalScript.addTab().addLine("SELECT 1");
            removalScript.addTab().addLine("FROM wres.TimeSeriesSource TSS");
            removalScript.addTab().addLine("WHERE TS.timeseries_id = TS.timeseries_id");
            removalScript.add(");");

            removalQueue.add(Database.issue( Query.withScript( removalScript.toString() ), false));

            LOGGER.debug("Added Task to remove orphaned time series...");

            removalScript = new ScriptBuilder(  );

            removalScript.addLine("DELETE FROM wres.Source S");
            removalScript.addLine("WHERE NOT EXISTS (");
            removalScript.addTab().addLine("SELECT 1");
            removalScript.addTab().addLine("FROM wres.ProjectSource PS");
            removalScript.addTab().addLine("WHERE PS.source_id = S.source_id");
            removalScript.add(");");

            removalQueue.add(Database.issue( Query.withScript( removalScript.toString() ), false));

            LOGGER.debug("Added task to remove orphaned sources...");

            removalScript = new ScriptBuilder(  );

            removalScript.addLine("DELETE FROM wres.Project P");
            removalScript.addLine("WHERE NOT EXISTS (");
            removalScript.addTab().addLine("SELECT 1");
            removalScript.addTab().addLine("FROM wres.ProjectSource PS");
            removalScript.addTab().addLine("WHERE PS.project_id = P.project_id");
            removalScript.add(");");

            LOGGER.debug("Added task to remove orphaned projects...");

            removalQueue.add(Database.issue( Query.withScript( removalScript.toString() ), false));

            try
            {
                removalQueue.loop();
            }
            catch ( ExecutionException e )
            {
                throw new SQLException( "Orphaned forecast, project, and source metadata could not be removed.", e );
            }

            LOGGER.info("Incomplete data has been removed from the system.");
        }
        catch ( SQLException | ExecutionException databaseError )
        {
            throw new SQLException( "Orphaned data could not be removed", databaseError );
        }

        return true;
    }

    /**
     * Runs a single query in the database
     * @param query The query to run
     * @param isHighPriority Whether or not to run the query on a high priority connection
     * @return The number of rows modified or returned by the query
     * @throws SQLException Thrown if an issue was encountered while communicating with the database
     */
    static int execute( final Query query, final boolean isHighPriority) throws SQLException
    {
        int modifiedRows = 0;
        Connection connection = null;

        try
        {
            if (isHighPriority)
            {
                connection = Database.getHighPriorityConnection();
            }
            else
            {
                connection = Database.getConnection();
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
    static DataProvider getData( final Query query, final boolean isHighPriority) throws SQLException
    {
        // Since Database.buffer performs all the heavy lifting, we can just rely on that. Setting that
        // call in the try statement ensures that it is closed once the in-memory results are created
        try (DataProvider rawProvider = Database.buffer(query, isHighPriority))
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
    static DataProvider buffer(final Query query, final boolean isHighPriority) throws SQLException
    {
        Connection connection;

        if (isHighPriority)
        {
            connection = Database.getHighPriorityConnection();
        }
        else
        {
            connection = Database.getConnection();
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
    static <V> V retrieve( final Query query, final String label, final boolean isHighPriority) throws SQLException
    {
        try(DataProvider data = Database.buffer( query, isHighPriority ))
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
    static void consume(
            final Query query,
            ExceptionalConsumer<DataProvider, SQLException> consumer,
            final boolean isHighPriority)
            throws SQLException
    {
        try (DataProvider data = Database.buffer( query, isHighPriority))
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
    static <U> List<U> interpret( final Query query, ExceptionalFunction<DataProvider, U, SQLException> interpretor, final boolean isHighPriority) throws SQLException
    {
        List<U> result;

        try (DataProvider data = Database.buffer( query, isHighPriority))
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
    static <V> Future<V> submit( final Query query, final String label, final boolean isHighPriority)
    {
        WRESCallable<V> queryToSubmit = new WRESCallable<V>() {
            @Override
            protected V execute() throws Exception
            {
                return Database.retrieve( this.query, this.label, this.isHighPriority );
            }

            @Override
            protected Logger getLogger()
            {
                return LOGGER;
            }

            private WRESCallable<V> init(final Query query, final boolean isHighPriority, final String label)
            {
                this.query = query;
                this.isHighPriority = isHighPriority;
                this.label = label;
                return this;
            }

            private Query query;
            private boolean isHighPriority;
            private String label;
        }.init( query, isHighPriority, label );

        return Database.submit( queryToSubmit );
    }

    /**
     * Schedules a query to run asynchronously with no regard to an result
     * @param query The query to schedule
     * @param isHighPriority Whether or not the query should be run on a high priority connection
     * @return The record for the scheduled task
     */
    static Future issue(final Query query, final boolean isHighPriority)
    {
        WRESRunnable queryToIssue = new WRESRunnable() {
            @Override
            protected void execute() throws SQLException
            {
                Database.execute( this.query, this.isHighPriority );
            }

            @Override
            protected Logger getLogger()
            {
                return LOGGER;
            }

            WRESRunnable init(final Query query, final boolean isHighPriority)
            {
                this.query = query;
                this.isHighPriority = isHighPriority;
                return this;
            }

            private Query query;
            private boolean isHighPriority;
        }.init(query, isHighPriority);

        return Database.execute( queryToIssue );
    }

    /**
     * Checks to see if the database contains orphaned data
     * TODO: Remove business logic
     * @return True if orphaned data exists within the database; false otherwise
     * @throws SQLException Thrown if the query used to detect orphaned data failed
     */
    private static boolean thereAreOrphanedValues() throws SQLException
    {
        ScriptBuilder scriptBuilder = new ScriptBuilder(  );

        scriptBuilder.addLine("SELECT EXISTS (");
        scriptBuilder.addTab().addLine("SELECT 1");
        scriptBuilder.addTab().addLine("FROM wres.Source S");
        scriptBuilder.addTab().addLine("WHERE NOT EXISTS (");
        scriptBuilder.addTab(  2  ).addLine("SELECT 1");
        scriptBuilder.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
        scriptBuilder.addTab(  2  ).addLine("WHERE PS.source_id = S.source_id");
        scriptBuilder.addTab().addLine(")");
        scriptBuilder.addLine(") AS orphans_exist;");

        Boolean thereAreOrphans = Database.retrieve(
                Query.withScript( scriptBuilder.toString() ), "orphans_exist", false
        );

        return thereAreOrphans != null && thereAreOrphans;
    }

    /**
     * Removes all user data from the database
     * TODO: This should probably accept an object or list to allow for the removal of business logic
     * @throws SQLException Thrown if successful communication with the
     * database could not be established
     */
    public static void clean() throws SQLException
    {
		StringBuilder builder = new StringBuilder();

		Collection<String> partitions = Database.getPartitionTables( "timeseriesvalue_lead%" );

		for (String partition : partitions)
        {
            builder.append("DROP TABLE ").append( partition).append(";").append(NEWLINE);
        }

        partitions = Database.getPartitionTables( "variablefeature_variable%" );

        for (String partition : partitions)
        {
            builder.append("DROP TABLE ").append( partition).append(";").append(NEWLINE);
        }

		builder.append("TRUNCATE wres.TimeSeriesSource;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeriesValue CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Observation;").append(NEWLINE);
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
            Database.execute( Query.withScript( builder.toString() ), false );
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
    static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }


    /**
     * Lock the database for mutation so that we can do clean up accurately
     * or ingest without another process cleaning up while this process ingests.
     *
     * Try once to get the lock. If it fails, fail quickly.
     *
     * @throws IOException when db communication fails
     * @throws IllegalStateException when lock could not be acquired
     */

    public static void lockForMutation() throws IOException
    {
        try
        {
            DATABASE_LOCK_MANAGER.lock( MUTATION_LOCK_KEY );
            LOGGER.info( "Successfully acquired database change privileges." );
        }
        catch ( SQLException se )
        {
            throw new IOException( "Could not acquire database change privileges.",
                                   se );
        }
    }


    /**
     * Release the lock for database mutation. The lock should also be released
     * automatically by postgres if our process dies.
     * @throws IOException when db communication fails
     * @throws IllegalStateException when lock could not be released
     */

    public static void releaseLockForMutation() throws IOException
    {
        try
        {
            DATABASE_LOCK_MANAGER.unlock( MUTATION_LOCK_KEY );
            LOGGER.info( "Successfully released database change privileges." );
        }
        catch ( SQLException se )
        {
            throw new IOException( "Could not release database change privileges.",
                                   se );
        }
    }

    /**
     * For system-level monitoring information, return the number of tasks in
     * the database queue.
     * @return the count of tasks waiting to be performed by the db workers.
     */

    public static int getDatabaseQueueTaskCount()
    {
        if ( Database.SQL_TASKS != null
             && Database.SQL_TASKS.getQueue() != null )
        {
            return Database.SQL_TASKS.getQueue().size();
        }

        return 0;
    }
}
