package wres.io.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import wres.util.Internal;

import javax.xml.stream.XMLStreamReader;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

/**
 * Contains access to configured settings and objects for accessing the database
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
final class DatabaseSettings {

	// A mapping of database names to the name of the class for the 
	private static final Map<String, String> DRIVER_MAPPING = createDriverMapping();

	private String url = "localhost";
	private String username = "wres";
	private String password = "wres";
	private String port = "5432";
	private String databaseName = "wres";
	private String databaseType = "postgresql";
	private String connection_string = null;
	private int max_pool_size = 10;
	private int max_idle_time = 30;

	/**
	 * Creates the mapping between the names of databases to the name of the classes that may connect to them
	 * @return Map of database names to class names
	 */
	private static Map<String, String> createDriverMapping()
	{
		TreeMap<String, String> mapping = new TreeMap<>();
		mapping.put("mysql", "com.mysql.jdbc.Driver");
		mapping.put("postgresql", "org.postgresql.Driver");
		return mapping;
	}

	/**
	 * Parses the settings for the database from an XMLReader
	 * @param reader The reader containing XML data
	 */
	DatabaseSettings(XMLStreamReader reader)
	{
		try {
			while (reader.hasNext())
			{
				if (reader.isStartElement() && reader.getLocalName().equalsIgnoreCase("database"))
				{
					reader.next();
				}
				else if (reader.isEndElement() && reader.getLocalName().equalsIgnoreCase("database"))
				{
					break;
				}
				else
				{
					parseElement(reader);
					reader.next();
				}
			}
			testConnection();
		} catch (Exception e) {
		    throw new ExceptionInInitializerError(e);
		}
	}

	/**
	 * For when there is something wrong with the file with database settings.
	 */
	DatabaseSettings()
	{
	}

	private void testConnection() throws Exception {
        Connection connection = null;

        try {
            Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
            connection = DriverManager.getConnection(this.getConnectionString(), this.username, this.password);
            Statement test = connection.createStatement();
            test.execute("SELECT 1;");
        }
        catch (SQLException sqlError)
        {
            String message = "The database could not be reached for connection verification." + System.lineSeparator() + System.lineSeparator();
            message += sqlError.getMessage() + System.lineSeparator() + System.lineSeparator();
            message += "Please ensure that you have:" + System.lineSeparator();
            message += "1) The correct URL to your database" + System.lineSeparator();
            message += "2) The correct username for your database" + System.lineSeparator();
            message += "3) The correct password for your user in the database" + System.lineSeparator();
            message += "4) An active connection to a network that may reach the requested database server" + System.lineSeparator() + System.lineSeparator();
            message += "The application will now exit.";
            throw new SQLException(message);
        }
        catch (ClassNotFoundException classError)
        {
            String message = "The specified database type of '" +
                    this.getDatabaseType() +
                    "' is not valid and a connection could not be created. Shutting down...";
            throw new IllegalArgumentException(message, classError);
        }
        finally {
            if (connection != null)
            {
                connection.close();
            }
        }
    }

	
	public ComboPooledDataSource createDatasource()
	{
		ComboPooledDataSource datasource = new ComboPooledDataSource();
		
		try {
			datasource.setDriverClass(DRIVER_MAPPING.get(getDatabaseType()));
			datasource.setJdbcUrl(getConnectionString());
			datasource.setUser(username);
			datasource.setPassword(password);
			datasource.setAutoCommitOnClose(true);
			datasource.setMaxIdleTime(max_idle_time);
			datasource.setMaxPoolSize(max_pool_size);
			datasource.setInitialPoolSize(max_pool_size);
			datasource.setPreferredTestQuery("SELECT 1");
			datasource.setTestConnectionOnCheckout(false);
		} 
		catch (PropertyVetoException e) {
			e.printStackTrace();
		}

        return datasource;
	}

	public ComboPooledDataSource createHighPriorityDataSource()
	{
		ComboPooledDataSource highPrioritySource = new ComboPooledDataSource();

		try
		{
			highPrioritySource.setDriverClass(DRIVER_MAPPING.get(getDatabaseType()));
			highPrioritySource.setJdbcUrl(getConnectionString());
			highPrioritySource.setUser(username);
			highPrioritySource.setPassword(password);
			highPrioritySource.setAutoCommitOnClose(true);
			highPrioritySource.setMaxIdleTime(10);
			highPrioritySource.setMaxPoolSize(5);
			highPrioritySource.setPreferredTestQuery("SELECT 1");
			highPrioritySource.setTestConnectionOnCheckout(false);
		}
		catch (PropertyVetoException e)
		{
			e.printStackTrace();
		}

		return highPrioritySource;
	}
	
	/**
	 * Sets the URL of the database to connect to
	 * @param url The address of the database to connect to
	 */
    private void setUrl (String url)
	{
		this.url = url;
	}
	
	/**
	 * Sets the username used to connect to the database
	 * @param username The username used to connect to the database
	 */
    private void setUsername (String username)
	{
		this.username = username;
	}
	
	/**
	 * Sets the password used to connect to the database
	 * @param password The password used to connect to the database
	 */
    private void setPassword (String password)
	{
		this.password = password;
	}
	
	/**
	 * Sets the identifier for the port used to connect to the database
	 * @param port
	 */
    private void setPort (String port)
	{
		this.port = port;
	}
	
	/**
	 * Sets the name of the database to connect to
	 * @param database_name The name of the database to access
	 */
    private void setDatabaseName (String database_name)
	{
		this.databaseName = database_name;
	}
	
	/**
	 * Sets the name of the type of database in use (such as 'mysql', 'postgresql', etc)
	 * @param database_type The name of the database to connect to
	 */
    private void setDatabaseType (String database_type)
	{
		this.databaseType = database_type;
	}
	
	/**
	 * Creates the connection string used to access the database
	 * @return The connection string used to connect to the database of interest
	 */
    private String getConnectionString ()
	{
		if (connection_string == null)
		{
			connection_string = "jdbc:";
			
			connection_string += databaseType;
			connection_string += "://";
			connection_string += url;
			if (port != null)
			{
				connection_string += ":";
				connection_string += port;
			}
			connection_string += "/";
			connection_string += databaseName;
		}
		
		return connection_string;
	}
	
	/**
	 * Parses out settings from the passed in XML 
	 * @param reader The XML reader containing XML data describing the database settings
	 * @throws Exception Any exception occurred when reading from the XML
	 */
	private void parseElement(XMLStreamReader reader) throws Exception
	{
		if (reader.isStartElement())
		{
			String tag_name = reader.getLocalName();
			reader.next();
			if (reader.isCharacters())
			{
				int begin_index = reader.getTextStart();
				int end_index = reader.getTextLength();
				String value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
				
				switch(tag_name)
				{
				case "database_type":
					setDatabaseType(value);
					break;
				case "port":
					setPort(value);
					break;
				case "name":
					setDatabaseName(value);
					break;
				case "password":
					setPassword(value);
					break;
				case "url":
					setUrl(value);
					break;
				case "username":
					setUsername(value);
					break;
				case "max_pool_size":
					max_pool_size = Integer.parseInt(value);
					break;
				case "max_idle_time":
					max_idle_time = Integer.parseInt(value);
					break;
				default:
					System.err.println("Tag of type: '" + tag_name + "' is not valid for database configuration.");
				}
			}
		}
	}
	
	public String getDatabaseType()
	{
		return this.databaseType;
	}

	public String getUsername()
	{
		return this.username;
	}
	
	@Override
    public String toString()
	{
		String string_rep = "Database Configuration:";
		
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "URL:\t\t";
		string_rep += String.valueOf(url);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Username:\t";
		string_rep += String.valueOf(username);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Password:\t";
		string_rep += String.valueOf(password);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Database Name:\t";
		string_rep += String.valueOf(databaseName);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Port:\t\t";
		string_rep += String.valueOf(port);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Database Type:\t";
		string_rep += String.valueOf(databaseType);
		
		return string_rep;
	}

}
