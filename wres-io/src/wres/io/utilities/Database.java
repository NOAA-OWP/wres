package wres.io.utilities;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.config.SystemSettings;
import wres.io.grouping.DualString;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.*;

public final class Database {
    
    private Database(){}

    private static final ConcurrentHashMap<String, Map<String, DualString>> SAVED_INDEXES = new ConcurrentHashMap<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    private static final ComboPooledDataSource CONNECTION_POOL = SystemSettings.getConnectionPool();

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

	public static Future getStoredIngestTask() throws InterruptedException {
		return storedIngestTasks.poll();
	}

	public static void storeIngestTask(Future task)
	{
		storedIngestTasks.add(task);
	}

	// A thread executor specifically for SQL calls
	private static ExecutorService sqlTasks = createService();

	public static void kill()
	{
		shutdown();
		CONNECTION_POOL.close();
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
		builder.append("	AND ns.nspname = ANY('{partitions, wres}');");

		Connection connection = null;
		ResultSet results = null;
		Map<String, Map<String, DualString>> foundIndexes = new TreeMap<>();

		try
		{
			connection = getConnection();
			results = getResults(connection, builder.toString());

			while (results.next())
			{
				foundIndexes.putIfAbsent(results.getString("table_name"), new TreeMap<String, DualString>());
				DualString value = new DualString(results.getString("column_names"), results.getString("index_type"));
				foundIndexes.get(results.getString(("table_name")))
							.put(results.getString("index_name"), value);
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

		for (String tableName : foundIndexes.keySet())
		{
			builder = new StringBuilder();
			builder.append("DROP INDEX IF EXISTS ");
			int indexCount = 0;

			for (String indexName : foundIndexes.get(tableName).keySet())
			{
				if (indexCount > 0)
				{
					builder.append(", ");
				}

				builder.append(indexName);
			}

			builder.append(";");

			try {
				execute(builder.toString());
				SAVED_INDEXES.putIfAbsent(tableName, new TreeMap<>());

				for (Map.Entry<String, DualString> indexDefinition : foundIndexes.get(tableName).entrySet())
				{
					SAVED_INDEXES.get(tableName)
								 .put(indexDefinition.getKey(), indexDefinition.getValue());
				}
			}
			catch (SQLException e) {
			    LOGGER.error("The indexes for %s could not be dropped.", tableName);
                LOGGER.error(NEWLINE + builder.toString() + NEWLINE);
				LOGGER.error(Strings.getStackTrace(e));
			}
		}
	}

	public synchronized static void restoreAllIndices()
	{
		StringBuilder builder;

		Set<String> updatedTables = new HashSet<>();

		for (String tableName : SAVED_INDEXES.keySet())
		{
			Object[] indexNames = SAVED_INDEXES.get(tableName).keySet().toArray();

			for (int nameIndex = 0; nameIndex < indexNames.length; ++nameIndex)
			{
				String indexName = (String)indexNames[nameIndex];
                DualString definition = SAVED_INDEXES.get(tableName).get(indexName);
				builder = new StringBuilder();
				builder.append("CREATE INDEX IF NOT EXISTS ").append(indexName).append(NEWLINE);
				builder.append("	ON ").append(tableName).append(NEWLINE);
				builder.append("	USING ").append(definition.getSecond()).append(NEWLINE);
				builder.append("	").append(definition.getFirst()).append(";");

				try {
					LOGGER.trace("Restoring the {} index on {}...", indexName, tableName);
					execute(builder.toString());
					SAVED_INDEXES.get(tableName).remove(indexName);
					updatedTables.add(tableName);
					LOGGER.trace("The {} index on {} has been restored.", indexName, tableName);
				}
				catch (SQLException e) {
				    LOGGER.error("The {} index on {} could not be restored.", indexName, tableName);
                    LOGGER.error(NEWLINE + builder.toString() + NEWLINE);
					LOGGER.error(Strings.getStackTrace(e));
				}
			}
		}

		SAVED_INDEXES.clear();

		if (updatedTables.size() > 0)
		{
			builder = new StringBuilder();

			for (String tableName : updatedTables)
			{
				builder.append("ANALYZE " + tableName + ";").append(NEWLINE);
			}

			try {
				LOGGER.trace("Statistics for indexed tables are being refreshed...");
				execute(builder.toString());
				LOGGER.trace("The statistics have been refreshed.");
			}
			catch (SQLException e) {
			    LOGGER.error("Statistics for restored indices could not be refreshed.");
                LOGGER.error(NEWLINE + builder.toString() + NEWLINE);
				LOGGER.error(Strings.getStackTrace(e));
			}
		}
	}

	public static void saveIndex(String tableName, String indexName, String indexDefinition)
	{
		if (!indexDefinition.startsWith("("))
		{
			indexDefinition = "(" + indexDefinition;
		}

		if (!indexDefinition.endsWith(")"))
		{
			indexDefinition += ")";
		}

		SAVED_INDEXES.putIfAbsent(tableName, new TreeMap<>());
		SAVED_INDEXES.get(tableName)
					 .put(indexName, new DualString(indexDefinition, "btree"));
	}

	public static void saveIndex(String tableName, String indexName, String indexDefinition, String indexType)
    {
        SAVED_INDEXES.putIfAbsent(tableName, new TreeMap<>());
        SAVED_INDEXES.get(tableName)
                     .put(indexName, new DualString(indexDefinition, indexType));
    }

	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static ExecutorService createService()
	{
		if (sqlTasks != null)
		{
			sqlTasks.shutdown();
			while (!sqlTasks.isTerminated());
		}
		ThreadPoolExecutor executor = new ThreadPoolExecutor(CONNECTION_POOL.getMaxPoolSize(),
                                                             CONNECTION_POOL.getMaxPoolSize(),
                                                             SystemSettings.poolObjectLifespan(),
                                                             TimeUnit.MILLISECONDS,
                                                             //new LinkedBlockingQueue<>(1200)
															 new ArrayBlockingQueue<>(1200)
		);

		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;// Executors.newFixedThreadPool(CONNECTION_POOL.getMaxPoolSize());
	}

	public static void completeAllIngestTasks()
	{
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
						e.printStackTrace();
					}
				}
				ProgressMonitor.completeStep();
			}
		}
		catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 */
	public static Future<?> execute(Runnable task)
	{
		if (sqlTasks == null || sqlTasks.isShutdown())
		{
			sqlTasks = createService();
		}

		return sqlTasks.submit(task);
	}

	public static <V> Future<V> submit(Callable<V> task)
	{
		if (sqlTasks == null || sqlTasks.isShutdown())
		{
			sqlTasks = createService();
		}
		return sqlTasks.submit(task);
	}
	
	/**
	 * Waits until all passed in jobs have executed.
	 */
	public static void shutdown()
	{
		if (!sqlTasks.isShutdown())
		{
			sqlTasks.shutdown();
			while (!sqlTasks.isTerminated());
            CONNECTION_POOL.close();
		}
	}

	public static Connection getConnection() throws SQLException
	{
		return CONNECTION_POOL.getConnection();
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
	
	public static void execute(final String query) throws SQLException
	{	
		Connection connection = null;
		Statement statement = null;
		
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
		if (!SystemSettings.getDatabaseType().equalsIgnoreCase("postgresql"))
		{
            try {
                translateCopyToInsert(table_definition, values, delimiter);
            }
            catch (SQLException e) {
                LOGGER.error("Translating the copy operation to an insert operation failed.");
                throw new CopyException("Translating the copy operation to an insert operation failed.", e);
            }
            return;
		}
		
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
            e.printStackTrace();
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
		Connection connection = null;

		throw new RuntimeException("Database.buildInstance() is not ready for execution.");

		/*try {
			connection = Database.getConnection();
			liquibase.database.Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
			// TODO: MUEY BAD; there needs to be a better solution than accessing the file via a hardcoded path. Maybe a system setting?
			Liquibase liquibase = new Liquibase("nonsrc/database/db.changelog-master.xml", new ClassLoaderResourceAccessor(), database);
			liquibase.update(new Contexts(), new LabelExpression());
			database.close();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		catch (DatabaseException e) {
			e.printStackTrace();
		}
		catch (LiquibaseException e) {
			e.printStackTrace();
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
	 * Take information about information that was supposed to be copied into a Database, convert to insert statements, and execute.
	 * The only database that supports this copy operation is PostgreSQL, so this will need to be implemented for any other database.
	 * 
	 * @param table_definition
	 * @param values
	 * @param delimiter
	 * @return
	 * @throws SQLException
	 */
	private static boolean translateCopyToInsert(final String table_definition, final String values, String delimiter) throws SQLException
	{
		boolean success = false;
		
		// This script will need to be put together from the table definition and the values picked apart by the delimiter
		String script = "";
		
		execute(script);
		
		return success;
	}

	@SuppressWarnings("unchecked")
	public static <T> T getResult(final String query, String label) throws SQLException
	{
		Connection connection = null;
		Statement statement = null;
		ResultSet results = null;
		T result = null;

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
            System.err.println("The following SQL call failed:");
            if (query.length() > 1000) {
                System.err.println(query.substring(0, 1000));
            } else {
                System.err.println(query);
            }
            System.err.println();

            error.printStackTrace();
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

	public static void refreshStatistics()
	{
		Connection connection;
		ResultSet results;

        try {
            connection = getConnection();

            StringBuilder script = new StringBuilder();

            script.append("SELECT 'ANALYZE '||n.nspname ||'.'|| c.relname||';' AS alyze,").append(NEWLINE);
            script.append("     'REINDEX TABLE '||n.nspname ||'.'|| c.relname||';' AS reidx").append(NEWLINE);
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
				script.append(results.getString("reidx")).append(NEWLINE);
                script.append(results.getString("alyze")).append(NEWLINE);
            }

            script.append("REINDEX TABLE wres.Forecast;").append(NEWLINE);
			script.append("ANALYZE wres.Forecast;").append(NEWLINE);
			script.append("REINDEX TABLE wres.Observation;").append(NEWLINE);
            script.append("ANALYZE wres.Observation;").append(NEWLINE);

            LOGGER.info("Now refreshing the statistics within the database.");
            Database.execute(script.toString());

        }
        catch (SQLException e) {
            e.printStackTrace();
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
        ResultSet results = null;
        Statement statement = connection.createStatement();
        // statement is purposely left open so that the returned ResultSet is
        // not closed. We count on c3p0 to magically take care of closing any
        // open resources when it should?
		statement.setFetchSize(SystemSettings.fetchSize());
		results = statement.executeQuery(query);
        return results; 
    }

    public static ComboPooledDataSource getPool()
    {
        return Database.CONNECTION_POOL;
    }
}
