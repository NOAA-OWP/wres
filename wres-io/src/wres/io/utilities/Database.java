package wres.io.utilities;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.FormattedStopwatch;
import wres.util.NotImplementedException;
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

	// The ingest lock key kind of resembles word INGEST, doesn't it?
	private static final long MUTATION_LOCK_KEY = 126357;
	private static final long MUTATION_LOCK_WAIT_MS = 32000;

	/**
	 * An advisory lock only lasts for the duration of a connection. If we
	 * open and create an advisory lock in a connection, it is lost  when the
	 * connection is lost. To keep it up, we're going to attempt to store the
	 * connection and close it to release the lock
	 */
	private static Connection advisoryLockConnection = null;

    /**
     * @Guards advisoryLockConnection
     */
	private static final Object ADVISORY_LOCK = new Object();

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
     * @throws SQLException when script execution or gets from resultsets fail
     * @throws InterruptedException when an underlying task is interrupted
     * @throws ExecutionException when an underlying task throws an exception
	 */

	public static void addNewIndexes() throws SQLException,
            InterruptedException, ExecutionException
	{
	    try
        {
            // Remove queued indexes for tables that don't exist
            ScriptBuilder script = new ScriptBuilder(  );
            script.addLine("DELETE FROM IndexQueue IQ");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM INFORMATION_SCHEMA.TABLES T");
            script.addTab().addLine("WHERE LOWER(T.table_schema || '.' || T.table_name) = LOWER(IQ.table_name)");
            script.addLine(");");

            script.execute();
        }
        catch ( SQLException e )
        {
            // We don't rethrow here because it just means that there was some
            // garbage floating around that we don't care about
            LOGGER.warn( "Invalid dynamic indexes could not be removed from the queue." );
        }

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

        //ProgressMonitor.resetMonitor();

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
                restore.setOnComplete( ProgressMonitor.onThreadCompleteHandler());
                indexTasks.add(Database.execute(restore));
            }
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
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "The result set containing the collection of "
                                 + "indexes could not be closed",
                                 e );
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
            ProgressMonitor.setSteps( (long)indexTasks.size() );
        }

		Future<?> task;
		while ((task = indexTasks.poll()) != null)
        {
            task.get();
            ProgressMonitor.completeStep();
        }

        if (LOGGER.isTraceEnabled())
        {
            watch.stop();
            LOGGER.trace("It took {} to restore all indexes in the database.",
                         watch.getFormattedDuration());
        }
	}

    /**
     * Saves metadata about an index to instate into the database
     * @param tableName The name of the table that the index will belong to
     * @param indexName The name of the index to instate
     * @param indexDefinition The definition of the index
     * @throws SQLException when query fails
     */
	public static void saveIndex(String tableName,
                                  String indexName,
                                  String indexDefinition)
            throws SQLException
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
			  .append("', 'btree');");

		try
        {
			Database.execute( script.toString() );
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
		    ProgressMonitor.setShouldUpdate( true );
            task = getStoredIngestTask();

            while ( task != null )
            {
                ProgressMonitor.increment();

                if (!task.isDone())
                {
                    List<IngestResult> singleResult = task.get();
                    ProgressMonitor.completeStep();

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

                task = getStoredIngestTask();
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Ingest task completion was interrupted." );
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
     * Executes the passed in query in the current thread
     * @param query The query to execute
     * @throws SQLException Thrown if an error occurred while attempting to
     * communicate with the database
     */
	public static void execute(final String query) throws SQLException
	{
	    Database.execute( query, false );
	}

    /**
     * Executes the passed in query in the current thread
     * @param query The query to execute
     * @param forceTransaction The force transaction state
     * @throws SQLException Thrown if an error occurred while attempting to
     * communicate with the database
     */
    public static void execute(final String query, final boolean forceTransaction) throws SQLException
    {
        Connection connection = null;
        Statement statement = null;
        Timer timer = null;

        LOGGER.trace( "{}{}{}", NEWLINE, query, NEWLINE );

        try
        {
            if (LOGGER.isDebugEnabled())
            {
                timer = Database.createScriptTimer( query );
            }
            connection = getConnection();
            connection.setAutoCommit( !forceTransaction );
            statement = connection.createStatement();
            statement.execute(query);

            if (forceTransaction)
            {
                connection.commit();
            }

            if (LOGGER.isDebugEnabled() && timer != null)
            {
                timer.cancel();
            }
        }
        catch (SQLException error)
        {
            String message = query;
            if (query.length() > 1000)
            {
                message = query.substring( 0, 1000 );
            }

            if (forceTransaction && connection != null)
            {
                connection.rollback();
            }

            LOGGER.error( "The following SQL call failed:{}{}",
                          NEWLINE,
                          message,
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
                connection.setAutoCommit( true );
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
		catch (SQLException | IOException | IllegalAccessException error)
		{
		    if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Data could not be copied to the database:{}{}",
                              Strings.truncate( values ), NEWLINE );
            }
			throw new CopyException( "Data could not be copied to the database.",
                                     error);
		}
        catch (InvocationTargetException e) {
		    String message = "The dynamically retrieved method '" + copyAPIMethodName + "' threw an exception upon execution.";
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
    public static <T> T getResult( final String query, final String label )
            throws SQLException
	{
	    Connection connection = null;
	    T result;

	    try
        {
            connection = Database.getConnection();
            result = Database.getResult( connection, query, label );
        }
        finally
        {
            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        return result;
	}


	@SuppressWarnings("unchecked")
	private static <T> T getResult(final Connection connection,
								   final String query,
								   final String label) throws SQLException
	{
		Statement statement = null;
		ResultSet results = null;
		T result = null;

		try
		{

			if ( LOGGER.isTraceEnabled() )
			{
				LOGGER.trace( query );
			}

			statement = connection.createStatement();
			statement.setFetchSize(1);

			Timer scriptTimer = null;

			if (LOGGER.isDebugEnabled())
			{
				scriptTimer = createScriptTimer( query );
			}

			results = statement.executeQuery(query);

			if (LOGGER.isDebugEnabled() && scriptTimer != null)
			{
				scriptTimer.cancel();
			}

			if ( results.isBeforeFirst() )
			{
				results.next();
				result = Database.getValue (results, label);
			}
		}
		catch ( SQLException error )
		{
			String message = "The following SQL query failed:" + NEWLINE + query;
			// Decorate SQLException with additional information
			throw new SQLException( message, error );
		}
		finally
		{
			if (results != null)
			{
			    try
                {
                    results.close();
                }
                catch ( SQLException se )
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "Could not close results {}.", results, se );
                }
			}

			if ( statement != null && !statement.isClosed() )
			{
				statement.close();
			}
		}

		LOGGER.trace( "Result of query: {}", result );

		return result;
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
    @SuppressWarnings( "unchecked" )
	public static <U> U getValue(ResultSet resultSet, String fieldName)
			throws SQLException
	{
	    // Jump to the first row if the jump hasn't already been made
	    if (resultSet.isBeforeFirst())
        {
            resultSet.next();
        }

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
        Instant result;

        if (!Database.hasColumn( resultSet, fieldName ))
        {
            throw new SQLException( "The field '" + fieldName + "' is not "
                                    + "present in the result set. An instant "
                                    + "cannot be created." );
        }

        // Jump to the first row if the jump hasn't already been made
        if (resultSet.isBeforeFirst())
        {
            resultSet.next();
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

    public static LocalDateTime getLocalDateTime(ResultSet resultSet, String fieldName) throws SQLException
	{
		Instant instant = Database.getInstant( resultSet, fieldName );
		return instant.atOffset( ZoneOffset.UTC ).toLocalDateTime();
	}

    /**
     * Retrieves results from the database over a high priority connection and
     * consumes the results
     * @param script The script used to retrieve the results
     * @param rowConsumer A function used to consume each row
     * @throws SQLException Thrown if the retrieval script fails
     * @throws SQLException Thrown if the consumer function fails
     */
	static void highPriorityConsume(String script, ExceptionalConsumer<ResultSet, SQLException> rowConsumer)
            throws SQLException
    {
        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults( connection, script );

            while (resultSet.next())
            {
                rowConsumer.accept( resultSet );
            }
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException closeException)
                {
                    LOGGER.debug("The result set used for interpretation could "
                                 + "not be closed.", closeException);
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }
    }

    public static void consume(String script, ExceptionalConsumer<ResultSet, SQLException> rowConsumer) throws SQLException
    {
        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getConnection();
            resultSet = Database.getResults( connection, script );

            while (resultSet.next())
            {
                rowConsumer.accept( resultSet );
            }
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException closeException)
                {
                    LOGGER.debug("The result set used for consumption could "
                                 + "not be closed.", closeException);
                }
            }

            if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }
    }

    static <U> List<U> interpret( String query,
                                  ExceptionalFunction<ResultSet, U, SQLException> interpretor,
                                  boolean priorityIsHigh)
            throws SQLException
    {
        List<U> result = new ArrayList<>();

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            if (priorityIsHigh)
            {
                connection = Database.getHighPriorityConnection();
            }
            else
            {
                connection = Database.getConnection();
            }

            resultSet = Database.getResults( connection, query );

            while (resultSet.next())
            {
                result.add(interpretor.call( resultSet ));
            }
        }
        finally
        {
            if (resultSet != null)
            {
                try
                {
                    resultSet.close();
                }
                catch (SQLException closeException)
                {
                    LOGGER.debug("The result set used for interpretation could "
                                 + "not be closed.", closeException);
                }
            }

            if (connection != null && priorityIsHigh)
            {
                Database.returnHighPriorityConnection( connection );
            }
            else if (connection != null)
            {
                Database.returnConnection( connection );
            }
        }

        return result;
    }

    /**
     * Checks if the specified column is in the given result set
     * @param resultSet The set of data retrieved from the database
     * @param columnName The name of the column to check for
     * @return Whether or not the column exists
     * @throws SQLException when resultSet metadata cannot be retrieved
     * @throws NullPointerException when any arg is null
     */
	public static boolean hasColumn(ResultSet resultSet, String columnName)
            throws SQLException
	{
	    Objects.requireNonNull( resultSet );
	    Objects.requireNonNull( columnName );

		boolean columnExists = false;

        try
        {
            // JDBC has a findColumn function that can do this, but it throws
            // an exception if it isn't found. This means that an exception is
            // thrown everytime it returns false. If you have thousands of
            // calls and they all return false, you'll hit a massive slowdown
            // due to exceptions. Instead, we perform a simple loop which will
            // prevent that
            int columnCount = resultSet.getMetaData().getColumnCount();
            for ( int index = 1; index <= columnCount; ++index )
            {
                if ( resultSet.getMetaData()
                              .getColumnLabel( index )
                              .equals( columnName ) )
                {
                    columnExists = true;
                    break;
                }
            }
        }
        catch ( SQLException e )
        {
            // We don't rethrow because it already answers the question;
            // it just means we can't query the column.
            LOGGER.debug( "A database result set could not be queried.", e );
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
    @SuppressWarnings( "unchecked" )
	public static Collection populateCollection(final Collection collection,
                                                final String query,
                                                final String fieldLabel)
            throws SQLException
	{
        Objects.requireNonNull( collection,
                                "The collection passed into "
                                + "'populateCollection' must have been "
                                + "instantiated." );

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
            Database.consume( query, collectionRow -> {
                if (collectionRow.getObject( fieldLabel ) != null)
                {
                    collection.add(collectionRow.getObject( fieldLabel ));
                }
            } );
		}
		catch (SQLException error)
		{
            String message = "The following query failed:" + NEWLINE + query;
            // Decorate SQLException with additional information.
			throw new SQLException( message, error );
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
     * @throws SQLException Thrown if the given query fails
     * @throws SQLException Thrown if the expected columns don't exist
     */
    @SuppressWarnings( "unchecked" )
	public static void populateMap(final Map map,
                                   final String query,
                                   final String keyLabel,
                                   final String valueLabel)
		throws SQLException
	{
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
			Database.consume( query, entryRow -> {
			    if (entryRow.getObject( keyLabel ) != null)
                {
                    map.put(entryRow.getObject( keyLabel ),
                            entryRow.getObject( valueLabel ));
                }
            });
		}
		catch (SQLException error)
		{
            String message = "The following query failed:" + NEWLINE + query;
            // Decorate SQLException with additional information.
			throw new SQLException( message, error );
		}
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
		DataSet dataSet;
		Timer scriptTimer = null;

		try
		{
		    if (LOGGER.isDebugEnabled())
            {
                scriptTimer = Database.createScriptTimer( query );
            }

			connection = Database.getConnection();
			resultSet = Database.getResults( connection, query );

			if (LOGGER.isDebugEnabled() && scriptTimer != null)
            {
                scriptTimer.cancel();
            }

			dataSet = new DataSet( resultSet );
		}
		finally
		{
			if (resultSet != null)
			{
			    try
                {
                    resultSet.close();
                }
                catch ( SQLException se )
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "Could not close result set {}.", resultSet, se );
                }
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
     * @throws SQLException when refresh or adding indices goes wrong
     */
	public static void refreshStatistics(boolean vacuum)
            throws SQLException
	{
        if (vacuum)
        {
            try
            {
                Database.addNewIndexes();
            }
            catch ( InterruptedException ie )
            {
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                // This might not actually be a SQLException underneath, but
                // for now, translate to one until we figure out a better way.
                throw new SQLException( "Could not add indices.", ee );
            }
        }

		Connection connection = null;
		ResultSet results;
        List<String> sql = new ArrayList<>();

        String optionalVacuum = "";

        if (vacuum)
        {
            optionalVacuum = "VACUUM ";
        }

		LOGGER.info("Analyzing data for efficient execution...");

        try
        {
            connection = getConnection();

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

            results = getResults( connection, script );

            while (results.next())
            {
            	sql.add(optionalVacuum + results.getString( "alyze" ));
            }

        }
        finally
        {
            Database.returnConnection(connection);
        }

        sql.add(optionalVacuum + "ANALYZE wres.Observation;");
        sql.add(optionalVacuum + "ANALYZE wres.TimeSeries;");
        sql.add(optionalVacuum + "ANALYZE wres.TimeSeriesSource;");
        sql.add(optionalVacuum + "ANALYZE wres.ProjectSource;");
        sql.add(optionalVacuum + "ANALYZE wres.Source;");
        sql.add(optionalVacuum + "ANALYZE wres.Ensemble;");

        List<Future<?>> queries = new ArrayList<>();

        for (String statement : sql)
        {
            SQLExecutor query = new SQLExecutor( statement );
            query.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );
            queries.add( Database.execute( query ) );
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
                LOGGER.error("A data optimization statement could not be completed.", e);
            }
            catch (InterruptedException e)
			{
				LOGGER.error("A data optimization statement could not be completed.", e);
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

	public static boolean removeOrphanedData() throws SQLException
    {
        try
        {
            if (!Database.thereAreOrphanedValues())
            {
                return false;
            }

            LOGGER.info("Incomplete data has been detected. Incomplete data "
                        + "will now be removed to ensure that all data operated "
                        + "upon is valid.");

            List<String> partitionTables = new ArrayList<>();
            ScriptBuilder script;

            // First, get list of all partition tables to gather
            try
            {
                script = new ScriptBuilder();
                script.addLine("SELECT N.nspname || '.' || C.relname AS table_name" );
                script.addLine( "FROM pg_catalog.pg_class C" );
                script.addLine( "INNER JOIN pg_catalog.pg_namespace N" );
                script.addTab().addLine( "ON N.oid = C.relnamespace" );
                script.addLine( "WHERE relchecks > 0" );
                script.addTab().addLine( "AND N.nspname = 'partitions'" );
                script.addTab().addLine( "AND C.relname LIKE 'timeseriesvalue_lead%'" );
                script.addTab().addLine( "AND relkind = 'r';" );

                script.consume( tableRow -> partitionTables.add(tableRow.getString( "table_name" )) );
            }
            catch ( SQLException databaseError )
            {
                throw new SQLException(
                        "A list of partition tables to evaluate "
                        + "could not be loaded.",
                        databaseError );
            }

            // Next obtain locks for individual tables
            // Exclusive mode is used so that unrelated processes may read, but not modify

            script = new ScriptBuilder(  );

            script.addLine( "LOCK TABLE wres.Source IN EXCLUSIVE MODE;" );
            script.addLine( "LOCK TABLE wres.ProjectSource IN EXCLUSIVE MODE;");
            script.addLine( "LOCK TABLE wres.Project IN EXCLUSIVE MODE;");
            script.addLine( "LOCK TABLE wres.TimeSeriesSource IN EXCLUSIVE MODE;");
            script.addLine( "LOCK TABLE wres.TimeSeries IN EXCLUSIVE MODE;");
            script.addLine( "LOCK TABLE wres.Observation IN EXCLUSIVE MODE;");

            for (String partition : partitionTables)
            {
                script.addLine( "LOCK TABLE ", partition, " IN EXCLUSIVE MODE;" );
            }

            script.addLine();

            script.addLine("DELETE FROM wres.Source S" );
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource PS");
            script.addTab().addLine("WHERE PS.source_id = S.source_id");
            script.addLine(");");
            script.addLine("DELETE FROM wres.Project P");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource PS");
            script.addTab().addLine("WHERE PS.project_id = P.project_id");
            script.addLine(");");
            script.addLine("DELETE FROM wres.TimeSeriesSource TSS");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource S");
            script.addTab().addLine("WHERE S.source_id = TSS.source_id");
            script.addLine(");");
            script.addLine("DELETE FROM wres.Observation O");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.ProjectSource S");
            script.addTab().addLine("WHERE S.source_id = O.source_id");
            script.addLine(");");
            script.addLine("DELETE FROM wres.TimeSeries TS");
            script.addLine("WHERE NOT EXISTS (");
            script.addTab().addLine("SELECT 1");
            script.addTab().addLine("FROM wres.TimeSeriesSource TSS");
            script.addTab().addLine("INNER JOIN wres.ProjectSource PS");
            script.addTab(  2  ).addLine("ON PS.source_id = TSS.source_id");
            script.addTab().addLine("WHERE TSS.timeseries_id = TS.timeseries_id");
            script.addLine(");");

            for (String partition : partitionTables)
            {
                script.addLine("DELETE FROM ", partition, " FV");
                script.addLine("WHERE NOT EXISTS (");
                script.addTab().addLine("SELECT 1");
                script.addTab().addLine("FROM wres.ProjectSource PS");
                script.addTab().addLine("INNER JOIN wres.TimeSeriesSource TSS");
                script.addTab(  2  ).addLine("ON TSS.source_id = PS.source_id");
                script.addTab().addLine("WHERE TSS.timeseries_id = FV.timeseries_id");
                script.addLine(");");
            }

            // We have to run everything sequentially because the tables need
            // to know about their parents to know what to delete.
            // Async would be neat, but not really possible. MVCC might make it
            // possible though; probably worth exploring since this can take a
            // while
            script.executeInTransaction();

            LOGGER.info("Incomplete data has been removed from the system.");
        }
        catch ( SQLException databaseError )
        {
            throw new SQLException( "Orphaned data could not be removed", databaseError );
        }

        return true;
    }

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

        return scriptBuilder.retrieve( "orphans_exist" );
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

			if (LOGGER.isDebugEnabled() && timer != null)
			{
				timer.cancel();
			}

		}
		catch (SQLException error)
		{
		    if (!statement.isClosed())
            {
                try
                {
                    statement.close();
                }
                catch ( SQLException se )
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "Failed to close statement {}.", statement, se );
                }
            }
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
    private static Timer createScriptTimer(final String query)
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
		builder.append("CREATE SCHEMA partitions AUTHORIZATION wres;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeriesSource;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeriesValue;").append(NEWLINE);
		builder.append("TRUNCATE wres.Observation;").append(NEWLINE);
		builder.append("TRUNCATE wres.Source RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.TimeSeries RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.Variable RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("DELETE FROM wres.VariableFeature VF").append(NEWLINE);
		builder.append("WHERE EXISTS(").append(NEWLINE);
		builder.append("    SELECT 1").append(NEWLINE);
		builder.append("    FROM wres.Feature F").append(NEWLINE);
		builder.append("    WHERE F.feature_id = VF.feature_id").append(NEWLINE);
		builder.append(");").append(NEWLINE);
		builder.append("DELETE FROM wres.Ensemble E").append(NEWLINE);
		builder.append("WHERE E.ensemble_id > 1;").append(NEWLINE);
		builder.append("TRUNCATE wres.Project RESTART IDENTITY CASCADE;").append(NEWLINE);
		builder.append("TRUNCATE wres.ProjectSource RESTART IDENTITY CASCADE;").append(NEWLINE);

		try
        {
			Database.execute(builder.toString());
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
    public static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }

    public static void lockTable(Connection connection, String tableName) throws SQLException
    {
        // Locking a table is nonsensical if the connection is autocommiting;
        // even if the database implementation driver allows it, the lock will
        // only last until the commit, which is immediate
        if (LOGGER.isDebugEnabled() && connection.getAutoCommit())
        {
            LOGGER.debug( "The application is seeking to lock a table with a "
                          + "connection that will automatically commit. Nothing "
                          + "will be achieved by this action." );

            StringJoiner traceJoiner = new StringJoiner( NEWLINE );
            for (StackTraceElement trace : Thread.currentThread().getStackTrace())
            {
                traceJoiner.add( trace.toString() );
            }

            LOGGER.debug( traceJoiner.toString() );
        }


        Statement statement = null;
        final String query = "LOCK TABLE " + tableName + " IN ACCESS EXCLUSIVE MODE;";

        try
        {
            statement = connection.createStatement();
            statement.execute( query );
        }
        catch (SQLException e)
        {
            String message = "The table '" + tableName + "' could not be locked."
                    + NEWLINE + "The query was: '" + query + "'";
            // Decorate SQLException with contextual information
            throw new SQLException( message,e );
        }
        finally
        {
            if ( statement != null )
            {
                try
                {
                    statement.close();
                }
                catch ( SQLException se )
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn( "Failed to close a statement.", se );
                }
            }
        }
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
        Connection connection;
        ResultSet resultSet;

        try
        {
            boolean shouldTryAgain;

            do
            {
                connection = SystemSettings.getRawDatabaseConnection();
                resultSet = Database.getResults( connection, TRY_LOCK_SCRIPT );
                boolean successfullyLocked = false;

                if (resultSet.next())
                {
                    successfullyLocked = Database.getValue( resultSet, RESULT_COLUMN );
                }

                if ( successfullyLocked )
                {
                    Database.setAdvisoryLockConnection( connection );
                    resultSet.close();
                    LOGGER.info( "Successfully acquired database change privileges." );
                    break;
                }
                else
                {

                    backoff = backoff * BACKOFF_MULTIPLIER;

                    shouldTryAgain =  START_TIME_MILLIS + MUTATION_LOCK_WAIT_MS
                                      >= System.currentTimeMillis() + backoff;

                    if ( shouldTryAgain )
                    {
                        LOGGER.info( "Waiting for another WRES process to finish modifying the database..." );
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
            while ( true );
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

        // We instead need to release the connection
        synchronized ( Database.ADVISORY_LOCK )
        {
            try
            {
                if (Database.advisoryLockConnection == null || Database.advisoryLockConnection.isClosed())
                {
                    LOGGER.info("{}The advisory lock was released too early.{}", NEWLINE, NEWLINE);
                }
            }
            catch ( SQLException e )
            {
                // The above statement is a diagnostic measure - it doesn't
                // affect the user, but we do need to log it for our own sakes
                LOGGER.warn( "Could not get lock status.", e );
            }

            try
            {
                Database.setAdvisoryLockConnection( null );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Database privileges could not be "
                                       + "adequately released.", e );
            }
        }

        LOGGER.info( "Successfully released database change privileges." );
    }

    private static void setAdvisoryLockConnection(Connection connection)
            throws SQLException
    {
        synchronized ( Database.ADVISORY_LOCK )
        {
            if (connection == null)
            {
                Database.advisoryLockConnection.close();
            }

            Database.advisoryLockConnection = connection;
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
