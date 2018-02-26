package wres.io.utilities;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.TimeSeriesValues;
import wres.util.FormattedStopwatch;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * An Interface structure used for organizing database operations and providing
 * common database operations
 */
public final class Database {
    
    private Database(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

	// The ingest lock key kind of resembles word INGEST, doesn't it?
	private static final long MUTATION_LOCK_KEY = 126357;
	private static final long MUTATION_LOCK_WAIT_MS = 32000;

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
	private static void storeIngestTask(Future task)
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
	 */
	public static void addNewIndexes()
	{
		StringBuilder builder;
        LinkedList<Future<?>> indexTasks = new LinkedList<>();

        Connection connection = null;
        ResultSet indexes = null;

        FormattedStopwatch watch = null;

        if (LOGGER.isTraceEnabled())
		{
			watch = new FormattedStopwatch();
			watch.start();
		}

        ProgressMonitor.resetMonitor();

        try
        {
            connection = Database.getConnection();
            indexes = Database.getResults( connection,
                                           "SELECT * FROM public.IndexQueue;" );

            while ( indexes.next())
            {
                builder = new StringBuilder(  );
                builder.append( "CREATE INDEX IF NOT EXISTS " )
                       .append( indexes.getString( "index_name" ) )
                       .append( NEWLINE );
                builder.append("    ON ")
                       .append(indexes.getString( "table_name" ))
                       .append(NEWLINE);
                builder.append("    USING ")
                       .append(indexes.getString( "method" ))
                       .append(NEWLINE);
                builder.append("    ")
                       .append(indexes.getString( "column_definition" ))
                       .append(";").append(NEWLINE);

                // Creates an asynchronous task to reinstate the index
                WRESRunnable restore = new SQLExecutor( builder.toString(), false);
                restore.setOnRun(ProgressMonitor.onThreadStartHandler());
                restore.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                indexTasks.add(Database.execute(restore));

                builder = new StringBuilder(  );
                builder.append("DELETE FROM public.IndexQueue").append(NEWLINE);
                builder.append("WHERE indexqueue_id = ")
                       .append(indexes.getInt( "indexqueue_id" ))
                       .append(";");

                // Creates an asynchronous task to remove the record indicating
                // that the index needs to be reinstated from the database
                restore = new SQLExecutor( builder.toString());
                restore.setOnRun(ProgressMonitor.onThreadStartHandler());
                restore.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                indexTasks.add(Database.execute(restore));
            }
        }
        catch ( SQLException e )
        {
            LOGGER.error(Strings.getStackTrace( e ));
        }
        finally
        {
            if (indexes != null)
            {
                try
                {
                    indexes.close();
                }
                catch ( SQLException e )
                {
                    LOGGER.error("The result set containing the collection of indexes could not be closed");
                    LOGGER.error( Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        if (indexTasks.peek() != null)
        {
            LOGGER.info("Restoring Indices...");
        }

		Future<?> task;
		while ((task = indexTasks.poll()) != null)
        {
            try
            {
                task.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }

        if (LOGGER.isTraceEnabled())
        {
            watch.stop();
            LOGGER.trace("It took {} to restore all indexes in the database.",
                         watch.getFormattedDuration());
        }
	}

    /**
     * Saves the definition of an index to the database, organized as a btree
     * @param tableName The name of the table to index
     * @param indexName The name of the index to instate
     * @param indexDefinition The definition of the index to instate
     */
	public static void saveIndex(String tableName, String indexName, String indexDefinition)
	{
		saveIndex( tableName, indexName, indexDefinition, "btree" );
	}

    /**
     * Saves metadata about an index to instate into the database
     * @param tableName The name of the table that the index will belong to
     * @param indexName The name of the index to instate
     * @param indexDefinition The definition of the index
     * @param indexType The organizational method for the index
     */
	public static void saveIndex(String tableName,
                                 String indexName,
                                 String indexDefinition,
                                 String indexType)
    {
		if (!indexDefinition.startsWith("("))
		{
			indexDefinition = "(" + indexDefinition;
		}

		if (!indexDefinition.endsWith(")"))
        {
            indexDefinition += ")";
        }

		StringBuilder script = new StringBuilder(  );
		script.append("INSERT INTO public.IndexQueue (table_name, index_name, column_definition, method)").append(NEWLINE);
		script.append("VALUES('")
			  .append(tableName)
			  .append("', '")
			  .append(indexName)
			  .append("', '")
			  .append(indexDefinition)
			  .append("', '")
			  .append(indexType)
			  .append("');");

		try
		{
			Database.execute( script.toString() );
		}
		catch ( SQLException e )
		{
			LOGGER.error( "Could not store metadata about the index '{}' in the database", indexName );
			LOGGER.error(Strings.getStackTrace( e ));
		}

    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ThreadPoolExecutor createService()
	{
		if (SQL_TASKS != null)
		{
			SQL_TASKS.shutdown();
			while (!SQL_TASKS.isTerminated());
		}

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
     * @throws IngestException if the ingest fails or is interrupted
     */
    public static List<IngestResult> completeAllIngestTasks() throws IngestException
    {
        LOGGER.trace( "Now completing all issued ingest tasks..." );

        List<IngestResult> result = new ArrayList<>();

        // This will gather all left over timeseries values that haven't
        // been sent to the database yet.
        TimeSeriesValues.complete();

		Future<List<IngestResult>> task;

		try
		{
            task = getStoredIngestTask();

            while ( task != null )
            {
				ProgressMonitor.increment();

				if (!task.isDone())
				{
					try
                    {
						List<IngestResult> singleResult = task.get();

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
					catch (ExecutionException e)
                    {
						LOGGER.error(Strings.getStackTrace(e));
					}
				}

				ProgressMonitor.completeStep();
                task = getStoredIngestTask();
			}
		}
		catch (InterruptedException e)
        {
		    LOGGER.error("Ingest task completion was interrupted.");
			LOGGER.error(Strings.getStackTrace(e));
			throw new IngestException( "The ingest could not be completed; " +
                                       "the operation was interupted." );
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
	public static Future<?> execute(Runnable task)
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
			while (!SQL_TASKS.isTerminated());
        }

        CONNECTION_POOL.close();
        HIGH_PRIORITY_CONNECTION_POOL.close();
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
            LOGGER.warn( "Database forceShutdown interrupted." );
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
            catch(SQLException error)
            {
                LOGGER.error("A connection could not be returned to the connection pool properly." + System.lineSeparator());
                LOGGER.error(Strings.getStackTrace(error));
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
            catch (SQLException error)
            {
                LOGGER.error("A high priority connection could not be returned to the connection pool properly.");
                LOGGER.error(Strings.getStackTrace(error));
            }
        }
    }

    /**
     * Executes the passed in query in the current thread
     * @param query The query to execute
     * @throws SQLException Thrown if an error occurred while attempting to
     * communicate with the database
     */
	public static void execute(final String query) throws SQLException
	{
		Connection connection = null;
		Statement statement = null;
		Timer timer = null;

        LOGGER.trace( "{}{}{}", NEWLINE, query, NEWLINE );

		try
		{
		    if (LOGGER.isDebugEnabled())
            {
                timer = createScriptTimer( query );
            }
			connection = getConnection();
			statement = connection.createStatement();
			statement.execute(query);
			if (LOGGER.isDebugEnabled())
            {
                timer.cancel();
            }
		}
		catch (SQLException error)
        {
            LOGGER.error( "The following SQL call failed:{}{}",
                          NEWLINE,
                          query,
                          error );
			throw error;
		}
		finally
		{
            if (statement != null)
            {
                statement.close();
            }

			if (connection != null)
			{
				returnConnection(connection);
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
		Connection connection = null;
		PushbackReader reader = null;
		final String copyAPIMethodName = "getCopyAPI";

		try
		{
			connection = getConnection();
			C3P0ProxyConnection proxy = (C3P0ProxyConnection)connection;
			Object[] arg = new Object[]{};
			CopyManager manager = (CopyManager)proxy.rawConnectionOperation(getCopyAPI(),
																			C3P0ProxyConnection.RAW_CONNECTION, arg);
			
			String copy_definition = "COPY " + table_definition + " FROM STDIN WITH DELIMITER ";
			if (!delimiter.startsWith("'")){
				delimiter = "'" + delimiter;
			}
			
			if (!delimiter.endsWith("'"))
			{
				delimiter += "'";
			}
			
			copy_definition += delimiter;


			reader = new PushbackReader(new StringReader(""), values.length() + 1000);
			reader.unread(values.toCharArray());

			manager.copyIn(copy_definition, reader);

		}
		catch (NoSuchMethodException noMethod)
		{
		    String message = "The method used to create the copy manager ('" +
                    copyAPIMethodName +
                    "') could not be retrieved from the PostgreSQL connection class.";
			LOGGER.error(message);
			throw new CopyException(message, noMethod);
		}
		catch (SQLException | IOException error)
		{
			LOGGER.debug("Data could not be copied to the database:");
			LOGGER.debug(Strings.truncate(values));
			LOGGER.debug("");

			throw new CopyException("Data could not be copied: " + error.getMessage(), error);
		}
        catch (IllegalAccessException e) {
			LOGGER.error(Strings.getStackTrace(e));
        }
        catch (InvocationTargetException e) {
		    String message = "The dynamically retrieved method '" + copyAPIMethodName + "' threw an exception upon execution.";
            LOGGER.error(message);
            throw new CopyException(message, e);
        }
        finally
		{
			if (reader != null)
			{
				try
                {
					reader.close();
				}
				catch (IOException e)
                {
					LOGGER.warn("The reader for copy values could not be properly closed.");
				}
			}

			if (connection != null)
			{
				returnConnection(connection);
			}
		}

	}

    /**
     * Executes the configured Liquibase scripts to keep the database up to date
     */
	public static synchronized void buildInstance()
	{
		throw new NotImplementedException("Database.buildInstance() is not ready for execution.");

        /*Connection connection = null;
		try {
			connection = Database.getConnection();
			liquibase.database.Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
			// TODO: MUEY BAD; there needs to be a better solution than accessing the file via a hardcoded path. Maybe a system setting?
			Liquibase liquibase = new Liquibase("nonsrc/database/db.changelog-master.xml", new ClassLoaderResourceAccessor(), database);
			liquibase.update(new Contexts(), new LabelExpression());
			database.close();
		}
		catch (SQLException e) {
            LOGGER.error(Strings.getStackTrace(e));
		}
		catch (DatabaseException e) {
            LOGGER.error(Strings.getStackTrace(e));
		}
		catch (LiquibaseException e) {
            LOGGER.error(Strings.getStackTrace(e));
		}
		finally
		{
			if (connection != null)
			{
				Database.returnConnection(connection);
			}
		}*/
	}


    /**
     * Returns the first value in the labeled column from the query
     * @param query The query used to select the value
     * @param label The name of the column containing the data to retrieve
     * @param <T> The type of data that should exist within the indicated column
     * @return The value in the indicated column from the first row of data.
     * Null is returned if no data was found.
     * @throws SQLException Thrown if communication with the database was
     * unsuccessful.
     */
	@SuppressWarnings("unchecked")
    public static <T> T getResult( final String query,
                                   final String label )
            throws SQLException
    {
        return (T) Database.getResult( query, label, null ).getLeft();
    }


    /**
     * Returns the first value in the labeled column from the query
     * @param query The query used to select the value
     * @param label The name of the column containing the data to retrieve
     * @param <T> The type of data that should exist within the indicated column
     * @param tablesToLock acquire an exclusive lock on these tables (can be
     *                     either null or empty if caller desires no locks)
     * @return The value in the indicated column from the first row of data.
     * Null is returned if no data was found.
     * The right hand boolean value of the pair indicates whether an insert was
     * performed during retrieval: true if insert happened, false otherwise
     * @throws SQLException Thrown if communication with the database was
     * unsuccessful.
     */
    @SuppressWarnings("unchecked")
    public static <T> Pair<T,Boolean> getResult( final String query,
                                                 final String label,
                                                 final String[] tablesToLock )
            throws SQLException
	{
		Connection connection = null;
        Statement lockStatement = null;
		Statement statement = null;
		ResultSet results = null;
		T result = null;
        String lockQuery = null;
        Boolean wasRowInserted = false;

        boolean shouldLock = tablesToLock != null && tablesToLock.length > 0;

        try
        {
            connection = getConnection();

            if ( shouldLock )
            {
                connection.setAutoCommit( false );
                lockStatement = connection.createStatement();
                lockStatement.setFetchSize( 1 );

                StringJoiner tables = new StringJoiner(", ");

                // Caller is responsible for correct order of locking
                for ( String table : tablesToLock )
                {
                    tables.add( table );
                }
                lockQuery = "LOCK TABLE " + tables.toString()
                            + " IN ACCESS EXCLUSIVE MODE";

                LOGGER.trace( lockQuery );
            }

            if ( LOGGER.isTraceEnabled() )
            {
                LOGGER.trace( query );
            }

            statement = connection.createStatement();
            statement.setFetchSize(1);

            // Must this happen after the last statement is created? Doesn't
            // seem to matter.
            if ( shouldLock )
            {
                lockStatement.execute( lockQuery );
            }

            Timer scriptTimer = null;

            if (LOGGER.isDebugEnabled())
            {
                scriptTimer = createScriptTimer( query );
            }

            results = statement.executeQuery(query);

            if (LOGGER.isDebugEnabled())
            {
                scriptTimer.cancel();
            }

            ResultSetMetaData metaData = results.getMetaData();

            if ( results.isBeforeFirst() )
            {
                results.next();
                result = (T) results.getObject(label);
            }

            // If the CTE results in a second column, it should be "wasInserted"
            if ( metaData.getColumnCount() == 2)
            {
                wasRowInserted =
                        ( Boolean ) results.getObject( "wasInserted" );
            }

            if ( shouldLock )
            {
                connection.commit();
            }
        }
        catch ( SQLException error )
        {
            LOGGER.error("The following SQL call failed: {}{}", NEWLINE, query, error );

            if ( shouldLock && connection != null )
            {
                if ( lockQuery != null )
                {
                    LOGGER.error( "The lock statement may have failed: {}{}",
                                  NEWLINE, lockQuery );
                }
                LOGGER.warn( "About to roll back" );
                connection.rollback();
                LOGGER.warn( "Finished rolling back" );
            }
            throw error;
        }
        finally
        {
            if (results != null)
            {
                results.close();
            }

            if ( lockStatement != null && !lockStatement.isClosed() )
            {
                lockStatement.close();
            }

            if ( statement != null && !statement.isClosed() )
            {
                statement.close();
            }

            if (connection != null)
            {
                if ( shouldLock )
                {
                    connection.setAutoCommit( true );
                }
                returnConnection(connection);
            }
        }

        LOGGER.trace( "Result of query: {}", result );

        return Pair.of( result, wasRowInserted );
	}

    /**
     * Gets a value from the set of results with the given field name
     * <p>
     *     <b>Note:</b> If you attempt to pull a primitive value from the
     *     result set and the value is null, then you will get the default
     *     primitive value back. This will attempt to cast the non-existent value
     * </p>
     *
     * @param resultSet The set to get the results from
     * @param fieldName The name of the field for the value to get
     * @param <U> The type of value to retrieve
     * @return The value if it is in the result set, null otherwise
     * @throws SQLException Thrown if the field is not in the result set
     */
	public static <U> U getValue(ResultSet resultSet, String fieldName)
			throws SQLException
	{
		return (U)resultSet.getObject( fieldName);
	}

    /**
     * Converts either a database Timestamp, epoch seconds (represented by an
     * Integer, Long, or Double), or text into an Instant
     * @param resultSet The resultSet containing the data to convert
     * @param fieldName The field that holds the value to convert
     * @return An Instant in time
     * @throws SQLException Thrown if the column for the field name doesn't exist
     * @throws SQLException Thrown if the data type held in the field cannot
     * be converted to an instant
     */
	public static Instant getInstant(ResultSet resultSet, String fieldName)
            throws SQLException
    {
        Instant result = null;

        if (!Database.hasColumn( resultSet, fieldName ))
        {
            throw new SQLException( "The field '" + fieldName + "' is not "
                                    + "present in the result set. An instant "
                                    + "cannot be created." );
        }

        // Timestamps are interpretted as strings in order to avoid the 'help'
		// that JDBC provides by converting timestamps to local times and
		// applying daylight savings changes
        if (resultSet.getObject( fieldName ) instanceof String ||
			resultSet.getObject( fieldName ) instanceof java.sql.Timestamp)
        {
            result = Instant.parse( resultSet.getString( fieldName )
                                             .replace( " ", "T" )
                                             .concat( "Z" )
            );
        }
        else if (resultSet.getObject( fieldName ) instanceof Integer)
        {
			result = Instant.ofEpochSecond( resultSet.getInt( fieldName ) );
        }
        else if (resultSet.getObject( fieldName ) instanceof Long)
        {
			result = Instant.ofEpochSecond( resultSet.getLong( fieldName ) );
        }
        else if (resultSet.getObject( fieldName ) instanceof Double)
        {
			Double epochSeconds = (Double)resultSet.getObject( fieldName );
			result = Instant.ofEpochSecond( epochSeconds.longValue() );
        }
        else
        {
            throw new SQLException( "The column type for '" +
                                    fieldName +
                                    "' cannot be converted into an Instance." );
        }

        return result;
    }

    /**
     * Checks if the specified column is in the given result set
     * @param resultSet The set of data retrieved from the database
     * @param columnName The name of the column to check for
     * @return Whether or not the column exists
     */
	public static boolean hasColumn(ResultSet resultSet, String columnName)
	{
		boolean columnExists = false;

		// If the column exists, it will just return the index, otherwise it errors.
		try
		{
			columnExists = resultSet.findColumn( columnName ) > -1;
		}
		catch (SQLException e)
		{
			LOGGER.trace( "The column '{}' was not found in the result set.",
						  columnName );
		}

		return columnExists;
	}

    /**
     * Populates the passed in collection with values of the indicated data type
     * originating from the column with the name of the passed in fieldLabel
     * in the passed in query
     * @param collection The collection to populate
     * @param query The query used to retrieve data
     * @param fieldLabel The name of the column containing data
     * @return The updated collection
     * @throws SQLException Thrown if the database could not be communicated
     * with successfully
     */
	public static Collection populateCollection(final Collection collection,
                                                final String query,
                                                final String fieldLabel)
            throws SQLException
	{
		Connection connection = null;
		ResultSet results = null;

		if (collection == null)
        {
            throw new NullPointerException("The collection passed into 'populateCollection' was null.");
        }

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "" );
            LOGGER.trace(query);
            LOGGER.trace("");
            LOGGER.trace("The value is '{}'", fieldLabel);
            LOGGER.trace("");
        }

		try
		{
            connection = Database.getConnection();
            results = Database.getResults(connection, query);

            while (results.next())
            {
            	if (results.getObject(fieldLabel) != null)
            	{
					collection.add(results.getObject(fieldLabel));
				}
            }
		}
		catch (SQLException error)
		{
            LOGGER.error( "The following query failed:{}{}", NEWLINE, query );
			throw error;
		}
		finally
		{
			if (results != null)
			{
				results.close();
			}

			if (connection != null)
			{
				Database.returnConnection(connection);
			}
		}

		return collection;
	}

    /**
     * Populates the given map with the key column and value column from the
     * given query
     *
     * <p>
     *     If a query returns:
     * </p>
     *
     * <table>
     *     <caption>Query Results</caption>
     *     <tr>
     *         <th>key</th>
     *         <th>value</th>
     *     </tr>
     *     <tr>
     *         <td>1</td>
     *         <td>"Alabama"</td>
     *     </tr>
     *     <tr>
     *         <td>2</td>
     *         <td>"Delaware"</td>
     *     </tr>
     *     <tr>
     *         <td>3</td>
     *         <td>"Kansas"</td>
     *     </tr>
     *     <tr>
     *         <td>4</td>
     *         <td>"Arkansas"</td>
     *     </tr>
     * </table>
	 *
     * <p>
     *     and the caller dictates that the label for the key is "key" and the
     *     label for the value is "value", it may populate the map such that it
     *     looks like:
     * </p>
     *
     * <table>
     *     <caption>Resulting Map</caption>
     *     <tr>
     *         <td>{</td>
     *         <td></td>
     *     </tr>
     *     <tr>
     *         <td></td>
     *         <td>1 -&gt; "Alabama"</td>
     *     </tr>
     *     <tr>
     *         <td></td>
     *         <td>2 -&gt; "Delaware"</td>
     *     </tr>
     *     <tr>
     *         <td></td>
     *         <td>3 -&gt; "Kansas"</td>
     *     </tr>
     *     <tr>
     *         <td></td>
     *         <td>4 -&gt; "Arkansas"</td>
     *     </tr>
     *     <tr>
     *         <td>}</td>
     *         <td></td>
     *     </tr>
     * </table>
     *
     * @param map The map to populate
     * @param query The script that will retrieve the values
     * @param keyLabel The label for the column that will serve as the key
     * @param valueLabel The label for the column that will serve as the value
     * @return The updated map
     * @throws SQLException Thrown if the given query fails
     * @throws SQLException Thrown if the expected columns don't exist
     */
	public static Map populateMap(final Map map,
								  final String query,
								  final String keyLabel,
								  final String valueLabel)
		throws SQLException
	{
		Connection connection = null;
		ResultSet results = null;

        Objects.requireNonNull(map,"The map passed into 'populateMap' was null." );

		if (LOGGER.isTraceEnabled())
		{
			LOGGER.trace("{}{}{}", NEWLINE, query, NEWLINE);
			LOGGER.trace("The key is '{}' and the value is '{}'{}",
						 keyLabel,
						 valueLabel,
                         NEWLINE);
		}

		try
		{
			connection = Database.getConnection();
			results = Database.getResults( connection, query );
			while (results.next())
			{
				Object key = results.getObject(keyLabel);
				if (key != null)
				{
					map.put(key, results.getObject( valueLabel));
				}
			}
		}
		catch (SQLException error)
		{
            LOGGER.error( "The following query failed:{}{}", NEWLINE, query );
			throw error;
		}
		finally
		{
			if (results != null)
			{
				results.close();
			}

			if (connection != null)
			{
				Database.returnConnection( connection );
			}
		}

		return map;
	}

    /**
     * Stores all values from a query in a format divorced from any connections
     * to the database
     *
     * <p>
     *     A database result set is closed if the statement that creates it is
     *     closed and that statement is closed if the connection is closed. If
     *     a result set needs to exist outside the scope of its connection, it
     *     needs to be stored in a different object
     * </p>
     * @param query The query which will create the resulting set of data
     * @return All data resulting from the query
     * @throws SQLException Thrown if the query fails
     */
	public static DataSet getDataSet(String query) throws SQLException
	{
		Connection connection = null;
		ResultSet resultSet = null;
		DataSet dataSet = null;
		Timer scriptTimer = null;

		try
		{
		    if (LOGGER.isDebugEnabled())
            {
                scriptTimer = Database.createScriptTimer( query );
            }

			connection = Database.getConnection();
			resultSet = Database.getResults( connection, query );

			if (LOGGER.isDebugEnabled())
            {
                scriptTimer.cancel();
            }

			dataSet = new DataSet( resultSet );
		}
		finally
		{
			if (resultSet != null)
			{
				resultSet.close();
			}

			if (connection != null)
			{
				Database.returnConnection( connection );
			}
		}

		return dataSet;
	}

    /**
     * Refreshes statistics that the database uses to optimize queries.
     * Performance suffers if the operation is told to vacuum missing values,
     * but the performance of the system as a whole is improved if many values
     * were removed prior to running.
     * @param vacuum Whether or not to remove records pertaining to deleted
     *               values as well
     */
	public static void refreshStatistics(boolean vacuum)
	{
        if (vacuum)
        {
            Database.addNewIndexes();
        }

		Connection connection = null;
		ResultSet results;

		// TODO: Thread this operation such that each table is analyzed simultaneously
        try
        {
            connection = getConnection();

            StringBuilder script = new StringBuilder();

            script.append("SELECT 'ANALYZE '||n.nspname ||'.'|| c.relname||';' AS alyze").append(NEWLINE);
            script.append("FROM pg_catalog.pg_class c").append(NEWLINE);
            script.append("INNER JOIN pg_catalog.pg_namespace n").append(NEWLINE);
            script.append("     ON N.oid = C.relnamespace").append(NEWLINE);
            script.append("WHERE relchecks > 0").append(NEWLINE);
            script.append("     AND (nspname = 'wres' OR nspname = 'partitions')").append(NEWLINE);
            script.append("     AND relkind = 'r';");

            results = getResults(connection, script.toString());

            script = new StringBuilder();

            while (results.next())
            {
                if (vacuum)
                {
                    script.append("VACUUM ");
                }
                script.append(results.getString("alyze")).append(NEWLINE);
            }

            if (vacuum)
            {
                script.append("VACUUM ");
            }
            script.append("ANALYZE wres.Observation;").append(NEWLINE);

            LOGGER.info("Now refreshing the statistics within the database.");
            Database.execute(script.toString());

        }
        catch (SQLException e)
        {
			LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
        	Database.returnConnection(connection);
		}
	}
    
    /**
     * Creates set of results from the given query through the given connection
	 *
     * @param connection The connection used to connect to the database
     * @param query The text for the query to call
     * @return The results of the query
     * @throws SQLException Any issue caused by running the query in the database
     */
    public static ResultSet getResults(final Connection connection, String query) throws SQLException
    {
        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "" );
            LOGGER.trace(query);
            LOGGER.trace("");
        }

        ResultSet results;
        Statement statement = connection.createStatement();
		statement.setFetchSize(SystemSettings.fetchSize());

		try
		{
			Timer timer = null;

			if (LOGGER.isDebugEnabled())
			{
				timer = Database.createScriptTimer( query );
			}

			results = statement.executeQuery(query);

			if (LOGGER.isDebugEnabled())
			{
				timer.cancel();
			}

		}
		catch (SQLException error)
		{
            LOGGER.error( "The following SQL query failed:{}{}", NEWLINE, query, error );
			throw error;
		}
        return results; 
    }

    /**
     * Creates a timer object that will write a query to the log after a short
     * amount of time
     *
     * <p>
     *     If the caller calls "timer.cancel()" before the timer goes to store
     *     the query, nothing is ever written. If this is used, only queries that
     *     take longer than that short amount of time will be written to the log.
     *     This can be used to spot long running queries.
     * </p>
     * @param query The query to log
     * @return A timer that will log the given script after a short period of time
     */
    public static Timer createScriptTimer(final String query)
	{
		TimerTask task = new TimerTask() {
			@Override
			public void run()
			{
				LOGGER.debug( "A long running query has been encountered:{}{}",
                              NEWLINE,
                              query );
				this.cancel();
			}
		};

		Timer timer = new Timer( "Script Timer" );

		// Sets the delay for 2 seconds; if a script takes this long, it will be written
		timer.schedule( task, 2000L );
		return timer;
	}

    /**
     * Removes all user data from the database
     * @throws SQLException Thrown if successful communication with the
     * database could not be established
     */
    public static void clean() throws SQLException
    {
		StringBuilder builder = new StringBuilder();

		builder.append("DROP SCHEMA partitions CASCADE;").append(NEWLINE);
		builder.append("CREATE SCHEMA partitions;").append(NEWLINE);
		builder.append("TRUNCATE wres.ForecastSource;").append(NEWLINE);
		builder.append("TRUNCATE wres.ForecastValue;").append(NEWLINE);
		builder.append("TRUNCATE wres.Observation;").append(NEWLINE);
		builder.append("TRUNCATE wres.Source RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeries RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Variable RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("DELETE FROM wres.VariablePosition VP").append(NEWLINE);
		builder.append("WHERE EXISTS(").append(NEWLINE);
		builder.append("    SELECT 1").append(NEWLINE);
		builder.append("    FROM wres.Feature F").append(NEWLINE);
		builder.append("    WHERE F.feature_id = VP.x_position").append(NEWLINE);
		builder.append(");").append(NEWLINE);
		builder.append("DELETE FROM wres.Feature WHERE parent_feature_id IS NOT NULL;").append(NEWLINE);
		builder.append("TRUNCATE wres.Project RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.ProjectSource RESTART IDENTITY CASCADE;").append(NEWLINE);

		try
        {
			Database.execute(builder.toString());
		}
		catch (final SQLException e)
        {
			LOGGER.error("WRES data could not be removed from the database." + NEWLINE);
			LOGGER.error("");
			LOGGER.error(builder.toString());
			LOGGER.error("");
			LOGGER.error(Strings.getStackTrace(e));
			throw e;
		}
	}

    /**
     * @return A reference to the standard connection pool
     */
    public static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }

    /**
     * Lock the database for mutation so that we can do clean up accurately.
     * @throws IOException when lock cannot be acquired or db has an error
     */

    public static void lockForMutation() throws IOException
    {
        final String RESULT_COLUMN = "pg_try_advisory_lock";
        final String TRY_LOCK_SCRIPT = "SELECT pg_try_advisory_lock( "
                                       + MUTATION_LOCK_KEY
                                       + " )";
        final long START_TIME_MILLIS = System.currentTimeMillis();

        final long BACKOFF_START_MILLIS = 1000;
        final long BACKOFF_MULTIPLIER = 2;
        long backoff = BACKOFF_START_MILLIS;

        try
        {
            boolean shouldTryAgain;

            do
            {
                boolean successfullyLocked =
                        Database.getResult( TRY_LOCK_SCRIPT, RESULT_COLUMN );

                backoff = backoff * BACKOFF_MULTIPLIER;

                shouldTryAgain =  START_TIME_MILLIS + MUTATION_LOCK_WAIT_MS
                                  >= System.currentTimeMillis() + backoff;

                if ( successfullyLocked )
                {
                    LOGGER.info( "Successfully acquired database change privileges." );
                    break;
                }
                else
                {
                    if ( shouldTryAgain )
                    {
                        LOGGER.info( "Waiting for another wres process to finish modifying the database..." );
                        Thread.sleep( backoff );
                    }
                    else
                    {
                        throw new IOException( "Another wres process is taking "
                                               + "a while. Gave up trying to "
                                               + "start ingest. Wait a bit "
                                               + "and try again." );
                    }
                }
            }
            while ( shouldTryAgain );
        }
        catch ( InterruptedException ie )
        {
            Thread.currentThread().interrupt();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "While attempting to acquire database change privileges",
                                       se );
        }
    }


    /**
     * Release the lock for database mutation. The lock should also be released
     * automatically by postgres if our process dies.
     * @throws IOException when anything goes wrong
     */

    public static void releaseLockForMutation() throws IOException
    {
        final String RELEASE_LOCK_SCRIPT = "SELECT pg_advisory_unlock( "
                                           + MUTATION_LOCK_KEY
                                           + " )";
        try
        {
            Database.execute( RELEASE_LOCK_SCRIPT );
            LOGGER.info( "Successfully released database change privileges." );
        }
        catch ( SQLException se )
        {
            throw new IOException( "Failed to release database change privileges.",
                                   se );
        }
    }

}
