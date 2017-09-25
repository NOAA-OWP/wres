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
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.SQLExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
import wres.util.FormattedStopwatch;
import wres.util.ProgressMonitor;
import wres.util.Strings;

public final class Database {
    
    private Database(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    private static final ComboPooledDataSource CONNECTION_POOL = SystemSettings.getConnectionPool();
    private static final ComboPooledDataSource HIGH_PRIORITY_CONNECTION_POOL = SystemSettings.getHighPriorityConnectionPool();

	// A thread executor specifically for SQL calls
	private static ThreadPoolExecutor SQL_TASKS = createService();

    private static final String NEWLINE = System.lineSeparator();

    private static Method copyAPI = null;

    private static Method getCopyAPI() throws NoSuchMethodException {
		if (copyAPI == null)
		{
			copyAPI = PGConnection.class.getMethod("getCopyAPI");
		}
		return copyAPI;
	}

	private static final LinkedBlockingQueue<Future> storedIngestTasks = new LinkedBlockingQueue<>();

	public static Future getStoredIngestTask()
    {
		return storedIngestTasks.poll();
	}

	public static void storeIngestTask(Future task)
	{
		storedIngestTasks.add(task);
	}

	public synchronized static void suspendAllIndices()
	{
		StringBuilder builder = new StringBuilder();

		builder.append("SELECT 	(idx.indrelid::REGCLASS)::text AS table_name,").append(NEWLINE);
		builder.append("		T.relname AS index_name,").append(NEWLINE);
		builder.append("		AM.amname AS index_type,").append(NEWLINE);
		builder.append("		'(' ||").append(NEWLINE);
		builder.append("			ARRAY_TO_STRING(").append(NEWLINE);
		builder.append("				ARRAY(").append(NEWLINE);
		builder.append("					SELECT pg_get_indexdef(idx.indexrelid, k+1, TRUE)").append(NEWLINE);
		builder.append("					FROM generate_subscripts(idx.indkey, 1) AS k").append(NEWLINE);
		builder.append("					ORDER BY k").append(NEWLINE);
		builder.append("				),").append(NEWLINE);
		builder.append("			', ')").append(NEWLINE);
		builder.append("		|| ')' AS column_names").append(NEWLINE);
		builder.append("FROM pg_index AS IDX").append(NEWLINE);
		builder.append("INNER JOIN pg_class AS T").append(NEWLINE);
		builder.append("	ON T.oid = IDX.indexrelid").append(NEWLINE);
		builder.append("INNER JOIN pg_am AS AM").append(NEWLINE);
		builder.append("	ON T.relam = AM.oid").append(NEWLINE);
		builder.append("INNER JOIN pg_namespace AS NS").append(NEWLINE);
		builder.append("	ON T.relnamespace = NS.OID").append(NEWLINE);
		builder.append("WHERE T.relname LIKE '%_idx'").append(NEWLINE);
		builder.append("	AND ns.nspname = 'partitions';");

		Connection connection = null;
		ResultSet results = null;

		try
		{
			connection = getConnection();
			results = getResults(connection, builder.toString());

			while (results.next())
			{
				Database.saveIndex( results.getString( "table_name" ),
									results.getString("index_name"),
									results.getString("column_names"),
									results.getString( "index_type" ) );

				builder = new StringBuilder(  );
				builder.append("DROP INDEX IF EXISTS ")
                       .append(results.getString( "index_name" ))
                       .append(";");
				Database.execute( builder.toString() );
			}
		}
		catch (SQLException error)
		{
		    LOGGER.error("The list of indices to suspend could not be properly loaded.");
            LOGGER.error(NEWLINE + builder.toString() + NEWLINE);
			LOGGER.error(Strings.getStackTrace(error));
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
				returnConnection(connection);
			}
		}
	}

	public synchronized static void restoreAllIndices()
	{

		StringBuilder builder;

		boolean shouldRefresh = false;
        LinkedList<Future<?>> indexTasks = new LinkedList<>();

        Connection connection = null;
        ResultSet indexes = null;
        LOGGER.info("Restoring Indices...");

        FormattedStopwatch watch = new FormattedStopwatch();
        watch.start();

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

                WRESRunnable restore = new SQLExecutor( builder.toString(), false);
                restore.setOnRun(ProgressMonitor.onThreadStartHandler());
                restore.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                indexTasks.add(Database.execute(restore));

                builder = new StringBuilder(  );
                builder.append("DELETE FROM public.IndexQueue").append(NEWLINE);
                builder.append("WHERE indexqueue_id = ")
                       .append(indexes.getInt( "indexqueue_id" ))
                       .append(";");

                restore = new SQLExecutor( builder.toString());
                restore.setOnRun(ProgressMonitor.onThreadStartHandler());
                restore.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                indexTasks.add(Database.execute(restore));
            }
        }
        catch ( SQLException e )
        {
            e.printStackTrace();
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


		Future<?> task;
		while ((task = indexTasks.poll()) != null)
        {
            try {
                task.get();
                shouldRefresh = true;
            }
            catch (InterruptedException | ExecutionException e) {
                LOGGER.error(Strings.getStackTrace(e));
            }
        }

        watch.stop();
		LOGGER.trace("It took {} to restore all indexes in the database.", watch.getFormattedDuration());

		if (shouldRefresh)
        {
            Database.refreshStatistics(false);
        }
	}

	public static void saveIndex(String tableName, String indexName, String indexDefinition)
	{
		saveIndex( tableName, indexName, indexDefinition, "btree" );
	}

	public static void saveIndex(String tableName, String indexName, String indexDefinition, String indexType)
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
		ThreadFactory factory = runnable -> new Thread(runnable, "Database Thread");
		ThreadPoolExecutor executor = new ThreadPoolExecutor(CONNECTION_POOL.getMaxPoolSize(),
                                                             CONNECTION_POOL.getMaxPoolSize(),
                                                             SystemSettings.poolObjectLifespan(),
                                                             TimeUnit.MILLISECONDS,
															 new ArrayBlockingQueue<>(CONNECTION_POOL.getMaxPoolSize() * 5),
															 factory
		);

		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;
	}

	public static void completeAllIngestTasks()
	{
		LOGGER.trace("Now completing all issued ingest tasks...");
		Future task;
		try {
			while (storedIngestTasks.peek() != null)
            {
				ProgressMonitor.increment();
				task = getStoredIngestTask();
				if (!task.isDone()) {
					try {
						task.get();
					}
					catch (ExecutionException e) {
						LOGGER.error(Strings.getStackTrace(e));
					}
				}
				ProgressMonitor.completeStep();
			}
		}
		catch (InterruptedException e) {
		    LOGGER.error("Ingest task completion was interrupted.");
			LOGGER.error(Strings.getStackTrace(e));
		}
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
            CONNECTION_POOL.close();
		}
	}

	public static Connection getConnection() throws SQLException
	{
		return CONNECTION_POOL.getConnection();
	}

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
	        // The implementation of the C3P0 Connection option returns the connection to the pool when "close"d
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

	public static void returnHighPriorityConnection(Connection connection)
    {
        if (connection != null)
        {
            try
            {
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
	
	public static void copy(final String table_definition, final String values, String delimiter) throws CopyException
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
			LOGGER.error("Data could not be copied to the database:");
			LOGGER.error(Strings.truncate(values));
			LOGGER.error("");

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

	public static <U> Collection<U> populateCollection(final Collection<U> collection,
                                                       final Class<U> collectionDataType,
                                                       final String query,
                                                       final String fieldLabel) throws SQLException
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
        }

		try
		{
            connection = Database.getConnection();
            results = Database.getResults(connection, query);

            while (results.next())
            {
            	if (results.getObject(fieldLabel) != null)
            	{
					collection.add(results.getObject(fieldLabel, collectionDataType));
				}
            }
		}
		catch (SQLException error)
		{
			LOGGER.error("The following query failed:");
			LOGGER.error(formatQueryForOutput(query));
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

	public static <K,V> Map<K, V> populateMap(Map<K, V> map,
                                              String script,
                                              String keyName,
                                              String valueName) throws SQLException
    {
        if (LOGGER.isTraceEnabled())
        {
            LOGGER.trace( "" );
            LOGGER.trace(script);
            LOGGER.trace("");
        }
        Connection connection = null;
        ResultSet results = null;

        try
        {
            connection = Database.getConnection();
            results = Database.getResults(connection, script);

            while (results.next())
            {
            	if (results.getObject(keyName) != null && results.getObject(valueName) != null) {
					map.put((K) results.getObject(keyName), (V) results.getObject(valueName));
				}
            }
        }
        catch (ClassCastException e)
        {
            LOGGER.error("The results from the SQL script could not be adequetely cast to the map.");
            LOGGER.error(Strings.getStackTrace(e));
            throw e;
        }
        catch (SQLException e) {
        	LOGGER.error("A map could not be generated from the database:");
        	LOGGER.error(formatQueryForOutput(script));
            LOGGER.error(Strings.getStackTrace(e));
            throw e;
        }
        finally {
            if (results != null)
            {
                try
                {
                    results.close();
                }
                catch (SQLException e) {
                    LOGGER.error("Could not close result set.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }

        return map;
    }

	public static void refreshStatistics(boolean vacuum)
	{
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
        // statement is purposely left open so that the returned ResultSet is
        // not closed. We count on c3p0 to magically take care of closing any
        // open resources when it should?
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
		builder.append("TRUNCATE wres.ForecastEnsemble RESTART IDENTITY CASCADE;").append(NEWLINE);
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

	private static String formatQueryForOutput(String query)
	{
		if (query.length() > 1000) {
			query = query.substring(0, 1000) + " (...)";
		}
		return query;
	}

    public static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }
}
