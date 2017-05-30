package wres.io.config;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamReader;

import com.mchange.v2.c3p0.ComboPooledDataSource;

/**
 * Contains access to configured settings and objects for accessing the database
 * @author Christopher Tubbs
 */
public final class Database {

	// A mapping of database names to the name of the class for the 
	private static final Map<String, String> driver_mapping = create_driver_mapping();
	
	/**
	 * Creates the mapping between the names of databases to the name of the classes that may connect to them
	 * @return Map of database names to class names
	 */
	private static Map<String, String> create_driver_mapping()
	{
		TreeMap<String, String> mapping = new TreeMap<>();
		mapping.put("mysql", "com.mysql.jdbc.Driver");
		mapping.put("postgresql", "org.postgresql.Driver");
		return mapping;
	}
	
	/**
	 * Default Constructor
	 */
	public Database() {}
	
	/**
	 * Parses the settings for the database from an XMLReader
	 * @param reader The reader containing XML data
	 */
	public Database(XMLStreamReader reader)
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
					parse_element(reader);
					reader.next();
				}
				
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public ComboPooledDataSource createDatasource()
	{
		ComboPooledDataSource datasource = new ComboPooledDataSource();
		
		try {
			datasource.setDriverClass(driver_mapping.get(getDatabaseType()));
			datasource.setJdbcUrl(get_connection_string());
			datasource.setUser(username);
			datasource.setPassword(password);
			datasource.setAutoCommitOnClose(true);
			datasource.setMaxIdleTime(max_idle_time);
			datasource.setMaxPoolSize(max_pool_size);
			datasource.setInitialPoolSize(max_pool_size);
		} 
		catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		
		return datasource;
	}
	
	/**
	 * Sets the URL of the database to connect to
	 * @param url The address of the database to connect to
	 */
	public void set_url(String url)
	{
		this.url = url;
	}
	
	/**
	 * Sets the username used to connect to the database
	 * @param username The username used to connect to the database
	 */
	public void set_username(String username)
	{
		this.username = username;
	}
	
	/**
	 * Sets the password used to connect to the database
	 * @param password The password used to connect to the database
	 */
	public void set_password(String password)
	{
		this.password = password;
	}
	
	/**
	 * Sets the identifier for the port used to connect to the database
	 * @param port
	 */
	public void set_port(String port)
	{
		this.port = port;
	}
	
	/**
	 * Sets the name of the database to connect to
	 * @param database_name The name of the database to access
	 */
	public void set_database_name(String database_name)
	{
		this.database_name = database_name;
	}
	
	/**
	 * Sets the name of the type of database in use (such as 'mysql', 'postgresql', etc)
	 * @param database_type The name of the database to connect to
	 */
	public void set_database_type(String database_type)
	{
		this.database_type = database_type;
	}
	
	/**
	 * Creates the connection string used to access the database
	 * @return The connection string used to connect to the database of interest
	 */
	public String get_connection_string()
	{
		if (connection_string == null)
		{
			connection_string = "jdbc:";
			
			connection_string += database_type;
			connection_string += "://";
			connection_string += url;
			if (port != null)
			{
				connection_string += ":";
				connection_string += port;
			}
			connection_string += "/";
			connection_string += database_name;		
		}
		
		return connection_string;
	}
	
	/**
	 * Creates a Connection object used to access the configured database
	 * @return An open connection to the database
	 */
	public Connection create_connection()
	{		
		Connection connection = null;
		
		try {
			Class.forName(driver_mapping.get(this.database_type)).newInstance();
			
			Properties connection_properties = new Properties();
			connection_properties.setProperty("user", this.username);
			connection_properties.setProperty("password", this.password);
			
			connection = DriverManager.getConnection(this.get_connection_string(), connection_properties);
			connection.setAutoCommit(false);
			
		} catch (Exception error) {
			error.printStackTrace();
		}
		return connection;
	}
	
	/**
	 * Parses out settings from the passed in XML 
	 * @param reader The XML reader containing XML data describing the database settings
	 * @throws Exception Any exception occurred when reading from the XML
	 */
	private void parse_element(XMLStreamReader reader) throws Exception
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
					set_database_type(value);
					break;
				case "port":
					set_port(value);
					break;
				case "name":
					set_database_name(value);
					break;
				case "password":
					set_password(value);
					break;
				case "url":
					set_url(value);
					break;
				case "username":
					set_username(value);
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
		return this.database_type;
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
		string_rep += String.valueOf(database_name);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Port:\t\t";
		string_rep += String.valueOf(port);
		string_rep += System.lineSeparator();
		string_rep += "\t";
		string_rep += "Database Type:\t";
		string_rep += String.valueOf(database_type);
		
		return string_rep;
	}

	private String url = null;
	private String username = null;
	private String password = "";
	private String port = null;
	private String database_name = null;
	private String database_type = null;
	private String connection_string = null;
	private int max_pool_size = 10;
	private int max_idle_time = 30;
}
