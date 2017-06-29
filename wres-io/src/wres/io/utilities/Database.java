package wres.io.utilities;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.SystemSettings;

public final class Database {
    
    private Database(){}

    private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

    private static final ComboPooledDataSource CONNECTION_POOL = SystemSettings.getConnectionPool();
    
    private static final String NEWLINE = System.lineSeparator();

	// A thread executor specifically for SQL calls
	private static ExecutorService sqlTasks = createService();

	public static void kill()
	{
		shutdown();
		CONNECTION_POOL.close();
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
                                                             new LinkedBlockingQueue<>(1200)
		);

		executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
		return executor;// Executors.newFixedThreadPool(CONNECTION_POOL.getMaxPoolSize());
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
		Connection connection = null;
		short attemptCount = 0;
		SQLException exception = null;

		while (attemptCount < 10) {
			try {
				connection = CONNECTION_POOL.getConnection();
				break;
			} catch (SQLException error) {
				Debug.error(LOGGER, System.lineSeparator() + "A connection to the database could not be created" + System.lineSeparator());
				exception = new SQLException(error);
			}
			attemptCount++;
		}

		if (connection == null)
		{
		    if (exception == null)
            {
                exception = new SQLException("A connection could not be retrieved, but no errors were encountered.");
            }

            assert exception != null;
            throw exception;
		}

		return connection;
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
                Debug.error(LOGGER, "A connection could not be returned to the connection pool properly." + System.lineSeparator());
                Debug.error(LOGGER, error.toString());
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
		    Debug.error(LOGGER, "The following SQL call failed:");
		    Debug.error(LOGGER, query);
			Debug.error(LOGGER, error.toString());
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
	
	public static void copy(final String table_definition, final String values, String delimiter) throws Exception
	{
		if (!SystemSettings.getDatabaseType().equalsIgnoreCase("postgresql"))
		{
			translateCopyToInsert(table_definition, values, delimiter);
			return;
		}
		
		Connection connection = null;
		PushbackReader reader = null;
		
		try
		{
			connection = getConnection();
			C3P0ProxyConnection proxy = (C3P0ProxyConnection)connection;
			Method get_copy_api = PGConnection.class.getMethod("getCopyAPI");//, new Class[]{});
			Object[] arg = new Object[]{};
			CopyManager manager = (CopyManager)proxy.rawConnectionOperation(get_copy_api, 
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
		catch (SQLException | IOException error)
		{
			System.err.println("Data could not be copied to the database:" + System.lineSeparator());
			if (values.length() > 1000)
			{
				System.err.println(values.substring(0, 1000) + "...");
			}
			else
			{
				System.err.println(values);
			}
			System.err.println();
			
			throw error;
		}
		finally
		{
			if (reader != null)
			{
				reader.close();
			}

			if (connection != null)
			{
				returnConnection(connection);
			}
		}

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

            System.out.println("The following queries will be executed:");

            while (results.next())
            {
                System.out.println(results.getString("alyze"));
                script.append(results.getString("alyze")).append(NEWLINE);
                System.out.println(results.getString("reidx"));
                script.append(results.getString("reidx")).append(NEWLINE);
            }

            System.out.println("ANALYZE wres.Forecast;");
            script.append("ANALYZE wres.Forecast;").append(NEWLINE);
            System.out.println("REINDEX TABLE wres.Forecast;");
            script.append("REINDEX TABLE wres.Forecast;").append(NEWLINE);
            System.out.println("ANALYZE wres.Observation;");
            script.append("ANALYZE wres.Observation;").append(NEWLINE);
            System.out.println("REINDEX TABLE wres.Observation;");
            script.append("REINDEX TABLE wres.Observation;");

            Database.execute(script.toString());

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Creates set of results from the given query through the given connection
     * @param connection The connection used to connect to the database
     * @param query The text for the query to call
     * @return The results of the query
     * @throws SQLException Any issue caused by running the query in the database
     */
    public static ResultSet getResults(final Connection connection, String query) throws SQLException
    {
        Statement statement = connection.createStatement();
        statement.setFetchSize(SystemSettings.fetchSize());

        return statement.executeQuery(query);
    }
}
