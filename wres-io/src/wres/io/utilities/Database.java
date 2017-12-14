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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.TimeSeriesValues;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;

public final class Database {
    
    private Database(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

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
    private static Method getCopyAPI() throws NoSuchMethodException {
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
		LOGGER.debug( "Storing ingest task raw Future as {}", task );
		Database.storedIngestTasks.add(task);
	}

	public static Future<?> ingest(WRESRunnable ingestTask)
	{
		Future<?> result = Database.execute( ingestTask );
		LOGGER.debug( "Storing ingest task WRESRunnable as {}", result );
		Database.storeIngestTask( result );
		return result;
	}

	public static <U> Future<U> ingest(WRESCallable<U> ingestTask)
	{
		Future<U> result = Database.submit( ingestTask );
		LOGGER.debug( "Storing ingest task WRESCallable as {}", result );
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
	    if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "Now completing all issued ingest tasks..." );
        }

        List<IngestResult> result = new ArrayList<>();

        // This will gather all left over timeseries values that haven't
        // been sent to the database yet.
        TimeSeriesValues.complete();

		Future<List<IngestResult>> task;

	    boolean shouldAnalyze = false;

		try
		{
            task = getStoredIngestTask();

            while ( task != null )
            {
                shouldAnalyze = true;
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
                        else if ( LOGGER.isDebugEnabled() )
                        {
                            LOGGER.debug( "A null value was returned in the "
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

		if (shouldAnalyze)
        {
            Database.addNewIndexes();
            Database.refreshStatistics( false );
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

    public static List<Runnable> shutdownWithAbandon( long timeOut,
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
            LOGGER.warn( "Database shutdownWithAbandon interrupted." );
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

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "" );
            LOGGER.trace(query);
            LOGGER.trace("");
        }
		
		try
		{
			connection = getConnection();
			statement = connection.createStatement();
			statement.execute(query);
		}
		catch (SQLException error)
		{
		    LOGGER.error("The following SQL call failed:");
		    LOGGER.error(query);
			LOGGER.error(Strings.getStackTrace(error));
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
				try {
					reader.close();
				}
				catch (IOException e) {
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

		throw new RuntimeException("Database.buildInstance() is not ready for execution.");

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
	public static <T> T getResult(final String query, String label) throws SQLException
	{
		Connection connection = null;
		Statement statement = null;
		ResultSet results = null;
		T result = null;

        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "" );
            LOGGER.trace(query);
            LOGGER.trace("");
        }

        try {
            connection = getConnection();

            statement = connection.createStatement();
            statement.setFetchSize(1);

            results = statement.executeQuery(query);

            if (results.isBeforeFirst()) {
                results.next();
                result = (T) results.getObject(label);
            }
        } catch (SQLException error) {
            LOGGER.error("The following SQL call failed:");
            LOGGER.error(formatQueryForOutput(query));
            throw error;
        } finally {
            if (results != null) {
                results.close();
            }

            if (statement != null) {
                statement.close();
            }

            if (connection != null) {
                returnConnection(connection);
            }
        }

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
	public static <U> U getValue(ResultSet resultSet, String fieldName)
			throws SQLException
	{
		return (U)resultSet.getObject( fieldName);
	}

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
			LOGGER.error("The following query failed:");
			LOGGER.error(query);
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

	public static Map populateMap(final Map map,
								  final String query,
								  final String keyLabel,
								  final String valueLabel)
		throws SQLException
	{
		Connection connection = null;
		ResultSet results = null;

		if (map == null)
		{
			throw new NullPointerException( "The map passed into 'populateMap' was null." );
		}

		if (LOGGER.isTraceEnabled())
		{
			LOGGER.trace("");
			LOGGER.trace(query);
			LOGGER.trace("");
			LOGGER.trace("The key is '{}' and the value is '{}'",
						 keyLabel,
						 valueLabel);
			LOGGER.trace( "" );
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
			LOGGER.error( "The following query failed:" );
			LOGGER.error(formatQueryForOutput( query ));
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
        try {
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
        catch (SQLException e) {
			LOGGER.error(Strings.getStackTrace(e));
        }
        finally {
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
			results = statement.executeQuery(query);
		}
		catch (SQLException error)
		{
			LOGGER.error("The following SQL query failed:");
			LOGGER.error(formatQueryForOutput(query));
			LOGGER.error(Strings.getStackTrace(error));
			throw error;
		}
        return results; 
    }

    /**
     * Removes all user data from the database
     * @throws SQLException Thrown if successful communication with the
     * database could not be established
     */
    public static void clean() throws SQLException {
		StringBuilder builder = new StringBuilder();

		Connection connection = null;
		ResultSet results = null;

		builder.append("SELECT 'DROP TABLE IF EXISTS '||n.nspname||'.'||c.relname||' CASCADE;'").append(NEWLINE);
		builder.append("FROM pg_catalog.pg_class c").append(NEWLINE);
		builder.append("INNER JOIN pg_catalog.pg_namespace n").append(NEWLINE);
		builder.append("    ON N.oid = C.relnamespace").append(NEWLINE);
		builder.append("WHERE relchecks > 0").append(NEWLINE);
		builder.append("    AND nspname = 'wres' OR nspname = 'partitions'").append(NEWLINE);
		builder.append("    AND relkind = 'r';");

		try {
			connection = Database.getConnection();
			results = Database.getResults(connection, builder.toString());

			builder = new StringBuilder();

			while (results.next()) {
				builder.append(results.getString(1)).append(NEWLINE);
			}
		}
		catch (final SQLException e) {
			LOGGER.error(Strings.getStackTrace(e));
			throw e;
		}
		finally
		{
			if (results != null)
			{
				try {
					results.close();
				}
				catch (SQLException e) {
					LOGGER.error(Strings.getStackTrace(e));
				}
			}

			if (connection != null)
			{
				Database.returnConnection(connection);
			}
		}

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
		catch (final SQLException e) {
			LOGGER.error("WRES data could not be removed from the database." + NEWLINE);
			LOGGER.error("");
			LOGGER.error(builder.toString());
			LOGGER.error("");
			LOGGER.error(Strings.getStackTrace(e));
			throw e;
		}
	}

    /**
     * Truncates a query for output if the query is too long
     * @param query The query to format
     * @return The query formatted for friendly display on the screen
     */
	private static String formatQueryForOutput(String query)
	{
		if (query.length() > 1000) {
			query = query.substring(0, 1000) + " (...)";
		}
		return query;
	}

    /**
     * @return A reference to the standard connection pool
     */
    public static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }
}
