package util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import config.SystemConfig;

public class Database {

    private static ComboPooledDataSource pool = SystemConfig.instance().get_connection_pool();

	// A thread executor specifically for SQL calls
	private static ExecutorService sqlTasks = createService();
	
	/**
	 * Creates a new thread executor
	 * @return A new thread executor that may run the maximum number of configured threads
	 */
	private static final ExecutorService createService()
	{
		if (sqlTasks != null)
		{
			sqlTasks.shutdown();
		}
		return Executors.newFixedThreadPool(SystemConfig.instance().get_maximum_thread_count());
	}
	
	/**
	 * Submits the passed in runnable task for execution
	 * @param task The thread whose task to execute
	 * @return An object containing an empty value generated at the end of thread execution
	 */
	public static void execute(Runnable task)
	{
		if (sqlTasks == null || sqlTasks.isShutdown())
		{
			sqlTasks = createService();
		}

		sqlTasks.execute(task);
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
			close();
		}
	}

	public static void close() {
		pool.close();
	}
	
	public static Connection getConnection() throws SQLException
	{
		Connection connection = null;
		
		try {
			connection = pool.getConnection();
		} catch (SQLException error) {
			System.err.println();
			System.err.println("A connection to the database could not be created");
			System.err.println();
			throw error;
		}
		return connection;
	}
	
	public static void returnConnection(Connection connection) throws SQLException
	{
		connection.close();
	}
	
	public static boolean execute(final String query) throws SQLException
	{	
		Connection connection = null;
		Statement statement = null;
		boolean success = false;
		
		try
		{
			connection = getConnection();
			statement = connection.createStatement();
			success = statement.execute(query);
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			
			System.err.println("The following SQL call failed:");
			if (query.length() > 1000)
			{
				System.err.println(query.substring(0, 1000));
			}
			else
			{
				System.err.println(query);
			}
			System.err.println();
			
			error.printStackTrace();
			throw error;
		}
		finally
		{
			
			if (connection != null)
			{
				returnConnection(connection);
			}
		}
		
		return success;
	}
	
	public static boolean copy(final String table_definition, final String values, String delimiter) throws Exception
	{
		if (!SystemConfig.instance().get_database_type().equalsIgnoreCase("postgresql"))
		{
			return translateCopyToInsert(table_definition, values, delimiter);
		}
		
		Connection connection = null;
		boolean success = false;
		
		
		try
		{
			connection = getConnection();
			C3P0ProxyConnection proxy = (C3P0ProxyConnection)connection;
			Method get_copy_api = PGConnection.class.getMethod("getCopyAPI", new Class[]{});
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

			PushbackReader reader = new PushbackReader(new StringReader(""), values.length() + 1000);
			reader.unread(values.toCharArray());
			manager.copyIn(copy_definition, reader);

			success = true;
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
			
			if (connection != null)
			{
				returnConnection(connection);
			}
		}
		
		return success;
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
	public static <T> T get_result(final String query, String label) throws SQLException
	{
		ResultSet results = null;
		Connection connection = null;
		Statement statement = null;
		T result = null;
		
		try
		{
			connection = getConnection();
			statement = connection.createStatement();
			statement.setFetchSize(1);
			results = statement.executeQuery(query);
			
			if (results.isBeforeFirst())
			{
				results.next();
				result = (T) results.getObject(label);
			}
		}
		catch (SQLException error)
		{			
			System.err.println("The following SQL call failed:");
			if (query.length() > 1000)
			{
				System.err.println(query.substring(0, 1000));
			}
			else
			{
				System.err.println(query);
			}
			System.err.println();
			
			error.printStackTrace();
			throw error;
		}
		finally
		{			
			if (connection != null)
			{
				returnConnection(connection);
			}
		}
		
		return result;
	}
}
