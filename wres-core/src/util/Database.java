package util;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import config.SystemConfig;

public class Database {

    private static ComboPooledDataSource pool = SystemConfig.instance().get_connection_pool();
	private static boolean close_pool = false;
	public static void close()
	{
		pool.close();
	}
	
	public static Connection get_connection() throws SQLException
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
	
	public static void return_connection(Connection conn) throws SQLException
	{
		conn.close();
	}
	
	public static boolean execute(final String query) throws SQLException
	{	
		Connection connection = null;
		Statement statement = null;
		boolean success = false;
		
		try
		{
			connection = get_connection();
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
				return_connection(connection);
			}
		}
		
		return success;
	}
	
	public static boolean copy(final String table_definition, final String values, String delimiter) throws Exception
	{
		if (!SystemConfig.instance().get_database_type().equalsIgnoreCase("postgresql"))
		{
			return translate_copy_to_insert(table_definition, values, delimiter);
		}
		
		Connection connection = null;
		boolean success = false;
		
		
		try
		{
			connection = get_connection();
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
			System.err.println(values);
			System.err.println();
			
			throw error;
		}
		finally
		{
			
			if (connection != null)
			{
				return_connection(connection);
			}
		}
		
		return success;
	}
	
	private static boolean translate_copy_to_insert(final String table_definition, final String values, String delimiter) throws SQLException
	{
		boolean success = false;

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
			connection = get_connection();
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
				return_connection(connection);
			}
		}
		
		return result;
	}
	
	public static void execute_queries(final String[] queries) throws SQLException
	{
		Connection connection = null;
		String current_query = "";
		Statement statement = null;
		
		try
		{
			connection = get_connection();
			
			for (String query : queries)
			{
				current_query = query;

				statement = connection.createStatement();
				statement.execute(current_query);
			}
		}
		catch (SQLException error)
		{			
			System.err.println("The following SQL call failed:");
			if (current_query.length() > 1000)
			{
				System.err.println(current_query.substring(0, 1000));
			}
			else
			{
				System.err.println(current_query);
			}
			System.err.println();
			
			error.printStackTrace();
			throw error;
		}
		finally
		{
			
			if (connection != null)
			{
				return_connection(connection);
			}
		}
	}
}
