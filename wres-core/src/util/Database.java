package util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import config.SystemConfig;

public class Database {

    private static ComboPooledDataSource pool = SystemConfig.getConnectionPool();

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
		return Executors.newFixedThreadPool(SystemConfig.maximumThreadCount());
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
	
	/**
	 * Returns the connection to the connection pool.
	 * @param connection The connection to return
	 * @throws SQLException
	 */
	public static void returnConnection(Connection connection) throws SQLException {
	    if (connection != null) {
	        // The implementation of the C3P0 Connection option returns the connection to the pool when "close"d
	        connection.close();
	    }
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
		if (!SystemConfig.getDatabaseType().equalsIgnoreCase("postgresql"))
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
	/**
	 * Returns the value in the labeled column in the first row of the result from the query
	 * @param query The statement to execute in the database
	 * @param label The name of the column to pull the value from
	 * @return The resulting value. Null if nothing was returned
	 * @throws SQLException Thrown if the function failed to interact with the database
	 */
	public static <T> T getResult(final String query, String label) throws SQLException
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
			if (statement != null)
			{
				statement.close();
			}
			if (connection != null)
			{
				returnConnection(connection);
			}
		}
		
		return result;
	}
	
	/**
	 * Retrieves an array from the database
	 * @param query The query to execute to return the array
	 * @param label The name of the column containing the array
	 * @return the requested array
	 * @throws SQLException Thrown if the an error occured while interacting with the database
	 */
	public static <T> T[] getArray(final String query, String label) throws SQLException
	{	    
        ResultSet results = null;
        Connection connection = null;
        Statement statement = null;
        T[] result = null;
        
	    try
        {
            connection = getConnection();
            statement = connection.createStatement();
            statement.setFetchSize(1);
            results = statement.executeQuery(query);
            
            if (results.isBeforeFirst())
            {
                results.next();
                result = (T[])results.getArray(label).getArray();
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
            if (statement != null)
            {
                statement.close();
            }
            if (connection != null)
            {
                returnConnection(connection);
            }
        }
        
        return result;
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
        statement.setFetchSize(SystemConfig.fetchSize());
        return statement.executeQuery(query);
    }
}
