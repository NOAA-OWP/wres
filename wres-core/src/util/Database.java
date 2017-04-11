package util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import config.SystemConfig;

public class Database extends ObjectPool<Connection> {
		
	private static Database connection_pool = null;
	
	private static synchronized Database pool()
	{
		if (connection_pool == null)
		{
			connection_pool = new Database();
		}
		
		return connection_pool;
	}
	
	public static Connection get_connection()
	{
		return pool().check_out();
	}
	
	public static void return_connection(Connection conn)
	{
		pool().checkIn(conn);
	}
	
	public static boolean execute(final String query) throws SQLException
	{	
		Connection connection = null;
		Statement statement = null;
		boolean success = false;
		
		try
		{
			connection = pool().check_out();
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
				connection_pool.checkIn(connection);
			}
		}
		
		return success;
	}
	
	@Override
	public synchronized void checkIn(Connection connection) {
		try {
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		super.checkIn(connection);
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
			connection = pool().check_out();
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
				connection_pool.checkIn(connection);
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
			connection = pool().check_out();
			
			for (String query : queries)
			{
				current_query = query;

				statement = connection.createStatement();
				statement.execute(current_query);
			}
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			
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
				connection_pool.checkIn(connection);
			}
		}
	}
	
	@Override
	public boolean validate(Connection o) {
		boolean valid = false;
		
		try
		{
			valid = !o.isClosed();
		}
		catch (SQLException error)
		{
			error.printStackTrace();
		}
		
		return valid;
	}

	@Override
	public void expire(Connection o) {
		try
		{
			o.commit();
			o.close();
		}
		catch (SQLException error)
		{
			error.printStackTrace();
		}
		
	}

	@Override
	protected Connection create() {
		Connection connection = null;
		try
		{
			connection = SystemConfig.get_database_connection();
		}
		catch (SQLException error)
		{
			error.printStackTrace();
		}
		
		return connection;
	}

}
