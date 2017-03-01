package wres.util;

import java.lang.reflect.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.postgresql.Driver;

public final class Utilities {
	// Dictates the number of threads that may be run to execute queries asynchronously
	private static int THREAD_COUNT = 30;
	
	// Executor used to manage threads used to execute database queries
	private static ExecutorService query_executor = Executors.newFixedThreadPool(THREAD_COUNT);
	
	// Contains a queue of queries that may be fired off in batches
	private static Queue<String> query_queue =  new ConcurrentLinkedQueue<String>();
	
	// A link to the database in use
	// TODO: Bake the url into a configuration file
	public static String DATABASE_URL = "jdbc:postgresql://localhost:5432/WRESDBTEST";
	
	// The name of the user to use when accessing the database
	// TODO: Bake the username into a configuration file
	public static String DATABASE_USERNAME = "pguser";
	
	// The password used to access the database
	// TODO: Bake the password into a configuration file
	public static String DATABASE_PASSWORD = "pass";
	
	public static void add_query(String query)
	{
		query_queue.add(query);
	}
	
	public static void execute_queries()
	{
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		String query = null;
		while((query = query_queue.poll()) != null)
		{
			executor.execute(new Runnable() {
				private String inner_query = "";
				public void run()
				{
					Connection connection = null;
					try
					{
						connection = create_eds_connection();
						Statement statement = connection.createStatement();
						statement.execute(inner_query);
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					finally
					{
						if (connection != null)
						{
							try {
								connection.close();
							} catch (SQLException e) {
								System.err.println("The connection could not be closed.");
								e.printStackTrace();
							}
						}
					}
				}
				
				private Runnable init(String inner_query)
				{
					this.inner_query = inner_query;
					return this;
				}
			}.init(query));
		}
		executor.shutdown();
		while (!executor.isTerminated())
		{
		}
	}
	
	@SuppressWarnings("unchecked")
	public static <T> T[] removeIndexFromArray(T[] array, Class<T> arrayType, int index)
	{
		if (index >= array.length)
		{
			String error = "Cannot remove index %d from an array of length %d.";
			error = String.format(error, index, array.length);
			throw new IndexOutOfBoundsException(error);
		}
		
		T[] copy = (T[])Array.newInstance(arrayType, array.length - 1);
		
		for (int i = 0; i < array.length; i++)
		{
			if (i != index)
			{
				if (i < index)
				{
					copy[i] = array[i];
				}
				else
				{
					copy[i - 1] = array[i];
				}
			}
		}
		
		return (T[])copy;
	}
	
	public static Connection create_eds_connection() throws SQLException
	{
		Connection connection = null;
		Properties props = new Properties();
		props.setProperty("user", DATABASE_USERNAME);
		props.setProperty("password", DATABASE_PASSWORD);
		int attempt_count = 0;
		while (connection == null)
		{
			try {
				attempt_count++;
				Driver driver = new Driver();
				connection = driver.connect(DATABASE_URL, props);
				if (attempt_count > 20)
				{
					System.out.println(String.format("Connection granted after %d attempts.", attempt_count));
				}
			}
			catch (Exception error)
			{
				try {
					Thread.sleep(800);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return connection;
	}
	
	public static Connection create_connection() throws SQLException
	{
		// Use for local connections; this is only an example. This will create a connection to my local 'ctubbs' database
		String url="jdbc:postgresql://localhost:5432/ctubbs";
		Properties props = new Properties();
		props.setProperty("user", "ctubbs");
		props.setProperty("password", "");
		Driver driver = new Driver();

		return driver.connect(url, props);
	}
	
	public static int milliseconds_to_seconds(int millliseconds)
	{
		return (int)(millliseconds / 1000) % 60;
	}
	
	public static int milliseconds_to_minutes(int milliseconds)
	{
		return (int) ((milliseconds / (1000*60)) % 60);
	}
	
	public static int milliseconds_to_hours(int milliseconds)
	{
		return (int) ((milliseconds / (1000*60*60)) % 24);
	}
	
	public static String convert_date_to_string(Date date)
	{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSSZ");
		
		return formatter.format(date);
	}
	
	public static String convert_date_to_string(Date date, String time_difference)
	{
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS" + time_difference);
		
		return formatter.format(date);
	}
	
	public static String convert_date_to_string(Calendar cal)
	{
		int offset = cal.getTimeZone().getRawOffset();
		offset = milliseconds_to_hours(offset);
		DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS" + String.valueOf(offset));
		
		return formatter.format(cal.getTime());
	}

	public static String convert_date_to_string(OffsetDateTime datetime)
	{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		return datetime.format(formatter);
	}
		
	public static boolean is_leap_year(int year)
	{
		return (year % 400) == 0 || ( (year % 4) == 0 && (year % 100) != 0);
	}
	
	public static void execute_eds_query(String query) throws SQLException
	{
		Connection connection = create_eds_connection();
		Statement statement = connection.createStatement();
		statement.execute(query);
		connection.close();
	}
	
	public static ResultSet get_results(String query) throws SQLException
	{
		Connection connection = create_eds_connection();
		Statement statement = connection.createStatement();
		ResultSet results = statement.executeQuery(query);
		results.next();
		return results;
	}
	
	public static void execute_eds_query_async(String query)
	{
		query_executor.execute(new Runnable() {
			private String inner_query = "";
			public void run()
			{
				Properties props = new Properties();
				props.setProperty("user", DATABASE_USERNAME);
				props.setProperty("password", DATABASE_PASSWORD);
				Driver driver = new Driver();
				Connection connection = null;
				try
				{
					connection = driver.connect(DATABASE_URL, props);
					Statement statement = connection.createStatement();
					statement.execute(inner_query);
				}
				catch (SQLException sql)
				{
					execute_eds_query_async(inner_query);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				finally
				{
					if (connection != null)
					{
						try {
							connection.close();
						} catch (SQLException e) {
							System.err.println("The connection could not be closed.");
							e.printStackTrace();
						}
					}
				}
			}
			
			private Runnable init(String inner_query)
			{
				this.inner_query = inner_query;
				return this;
			}
		}.init(query));
	}
	
	public static boolean execute_query(String query) throws SQLException
	{
		boolean success = false;
		Connection connection = null;
		
		try
		{
			connection = create_connection();
			connection.setAutoCommit(false);
			Statement statement = connection.createStatement();			
			success = statement.execute(query);
			connection.commit();
		}
		catch (SQLException error)
		{
			if (connection != null)
			{
				connection.rollback();
			}
			System.err.println("The following query could not be executed:");
			System.err.println();
			System.err.println(query);
			throw error;
		}
		finally
		{
			if (connection != null)
			{
				connection.close();
			}
		}
		
		return success;
	}
}
