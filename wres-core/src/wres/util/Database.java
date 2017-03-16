package wres.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.postgresql.Driver;

public class Database extends ObjectPool<Connection> {

	// A link to the database in use
	// TODO: Bake the url into a configuration file	
	private static final String DATABASE_TYPE = "postgresql";
	private static final String DATABASE_PORT = "5432";

	// This is the EDS connection string
	//private static final String DATABASE_URL = "***REMOVED***eds-dev1.***REMOVED***.***REMOVED***";
	//private static final String DATABASE_USERNAME = "christopher.tubbs";
	//private static final String DATABASE_PASSWORD = "changeme";
	
	//IOEP Database Name
	//public static final String DATABASE_NAME = "WRESDBTEST";
	
	//EDS Database Name
	//private static final String DATABASE_NAME = "wres";
	
	public static final String DATABASE_URL = "localhost";
	
	//local DB Name
	public static final String DATABASE_NAME = "ctubbs";
	public static final String DATABASE_USERNAME = "ctubbs";
	// The password used to access the database
	// TODO: Bake the password into a configuration file
	public static final String DATABASE_PASSWORD = "";
	
	// This is the IOEP connection string.
	// NOTE: You must be running directly from the IOEP vm to use this
	//public static final String DATABASE_USERNAME = "pguser";
	//public static final String DATABASE_PASSWORD = "pass";
		
	private static Database connection_pool = null;
	
	private static synchronized Database pool()
	{
		if (connection_pool == null)
		{
			connection_pool = new Database();
		}
		
		return connection_pool;
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
			System.err.println(query);
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
	
	public static ResultSet execute_for_result(final String query) throws SQLException
	{
		ResultSet results = null;		
		Connection connection = null;
		Statement statement = null;
		
		try
		{
			connection = pool().check_out();
			statement = connection.createStatement();
			results = statement.executeQuery(query);
			
			if (results.isBeforeFirst())
			{
				results.next();
			}
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			
			System.err.println("The following SQL call failed:");
			System.err.println(query);
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
		
		return results;
	}
	
	public static void execute_queries(final String[] queries) throws SQLException
	{
		Connection connection = null;
		String current_query = "";
		Statement statement = null;
		
		try
		{
			connection = pool().check_out();
			connection.setAutoCommit(false);
			
			for (String query : queries)
			{
				current_query = query;

				statement = connection.createStatement();
				statement.execute(current_query);
			}
			
			connection.commit();
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			
			System.err.println("The following SQL call failed:");
			System.err.println(current_query);
			System.err.println();
			
			error.printStackTrace();
			throw error;
		}
		finally
		{
			
			if (connection != null)
			{
				connection.setAutoCommit(true);
				connection_pool.checkIn(connection);
			}
		}
	}
	
	public static Map<String, ResultSet> execute_for_results(final Map<String, String> queries) throws SQLException
	{
		Map<String, ResultSet> results = new TreeMap<String, ResultSet>();
		Connection connection = null;
		String current_query = "";
		Statement query = null;

		try
		{
			connection = pool().check_out();
			connection.setAutoCommit(false);
			
			for (String query_name : queries.keySet())
			{
				current_query = queries.get(query_name);
				query = connection.createStatement();
				ResultSet result = query.executeQuery(current_query);
				
				if (result.isBeforeFirst())
				{
					result.next();
				}
				
				results.put(query_name, result);
			}
			
			connection.commit();
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			
			System.err.println("The following SQL call failed:");
			System.err.println(current_query);
			System.err.println();
			
			error.printStackTrace();
			throw error;
		}
		finally
		{
			
			if (connection != null)
			{
				connection.setAutoCommit(true);
				connection_pool.checkIn(connection);
			}
		}
		
		return results;
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
			String url = "jdbc:";
			url += DATABASE_TYPE;
			url += "://";
			url += DATABASE_URL;
			url += ":";
			url += DATABASE_PORT;
			url += "/";
			url += DATABASE_NAME;
			
			Properties props = new Properties();
			props.setProperty("user", DATABASE_USERNAME);
			props.setProperty("password",DATABASE_PASSWORD);
			Driver driver = new Driver();
			connection = driver.connect(url, props);
			//System.err.println("Connection created...");
		}
		catch (SQLException error)
		{
			error.printStackTrace();
		}
		
		return connection;
	}

}
