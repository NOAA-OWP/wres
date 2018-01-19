package wres.io.config;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Strings;
import wres.util.XML;

import javax.xml.stream.XMLStreamException;
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
final class DatabaseSettings
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseSettings.class);

	// A mapping of database names to the name of the class for the 
	private static final Map<String, String> DRIVER_MAPPING = createDriverMapping();

	private String url = "localhost";
	private String username = "wres";
	private String password;
	private String port = "5432";
	private String databaseName = "wres";
	private String databaseType = "postgresql";
    private int maxPoolSize = 10;
    private int maxIdleTime = 30;

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
    DatabaseSettings( XMLStreamReader reader )
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
			this.applySystemPropertyOverrides();
			testConnection();
        }
        catch ( XMLStreamException | SQLException e )
        {
            throw new ExceptionInInitializerError( e );
        }
	}

	/**
	 * For when there is something wrong with the file with database settings.
	 */
	DatabaseSettings()
	{
		this.applySystemPropertyOverrides();
	}

	private void testConnection() throws SQLException
    {
        Connection connection = null;
        Statement test = null;

        try {
            Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
            connection = DriverManager.getConnection(this.getConnectionString(), this.username, this.password);
            test = connection.createStatement();
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
            throw new SQLException(message, sqlError);
        }
        catch (ClassNotFoundException classError)
        {
            String message = "The specified database type of '" +
                    this.getDatabaseType() +
                    "' is not valid and a connection could not be created. Shutting down...";
            throw new SQLException( message, classError);
        }
        finally
		{
			if (test != null)
			{
				test.close();
			}

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
            datasource.setMaxIdleTime( maxIdleTime );
            datasource.setMaxPoolSize( maxPoolSize );
            datasource.setInitialPoolSize( maxPoolSize );
			datasource.setPreferredTestQuery("SELECT 1");
			datasource.setTestConnectionOnCheckout(false);
		} 
		catch (PropertyVetoException e) {
			LOGGER.error(Strings.getStackTrace(e));
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
			LOGGER.error(Strings.getStackTrace(e));
		}

		return highPrioritySource;
	}

    private String getUrl()
    {
        return this.url;
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

    private String getPort()
    {
        return this.port;
    }

	/**
	 * Sets the identifier for the port used to connect to the database
	 * @param port
	 */
    private void setPort (String port)
	{
		this.port = port;
	}

    private String getDatabaseName()
    {
        return this.databaseName;
    }

	/**
	 * Sets the name of the database to connect to
	 * @param databaseName The name of the database to access
	 */
    private void setDatabaseName (String databaseName)
	{
		this.databaseName = databaseName;
	}

	/**
	 * Sets the name of the type of database in use (such as 'mysql', 'postgresql', etc)
	 * @param databaseType The name of the database to connect to
	 */
    private void setDatabaseType (String databaseType)
	{
		this.databaseType = databaseType;
	}

	/**
	 * Creates the connection string used to access the database
	 * @return The connection string used to connect to the database of interest
	 */
    private String getConnectionString ()
    {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append( "jdbc:" );
        connectionString.append( this.getDatabaseType() );
        connectionString.append( "://" );
        connectionString.append( this.getUrl() );

        if ( this.getPort() != null )
        {
            connectionString.append( ":" );
            connectionString.append( this.getPort() );
        }

        connectionString.append( "/" );
        connectionString.append( this.getDatabaseName() );

        return connectionString.toString();
    }

	/**
	 * Parses out settings from the passed in XML 
	 * @param reader The XML reader containing XML data describing the database settings
	 * @throws Exception Any exception occurred when reading from the XML
	 */
    private void parseElement( XMLStreamReader reader )
            throws XMLStreamException
	{
		if (reader.isStartElement())
		{
			String tagName = reader.getLocalName();
			reader.next();
			if (reader.isCharacters())
			{
				int beginIndex = reader.getTextStart();
				int endIndex = reader.getTextLength();
				String value = new String(reader.getTextCharacters(), beginIndex, endIndex).trim();
				
				switch(tagName)
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
                    maxPoolSize = Integer.parseInt( value);
					break;
				case "max_idle_time":
                    maxIdleTime = Integer.parseInt( value);
					break;
				default:
					LOGGER.error("Tag of type: '{}' is not valid for database configuration.",
                                 tagName);
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
		String stringRep = "Database Configuration:";
		
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "URL:\t\t";
		stringRep += String.valueOf(url);
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "Username:\t";
		stringRep += String.valueOf(username);
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "Password:\t";
		stringRep += "(REDACTED)";
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "Database Name:\t";
		stringRep += String.valueOf(databaseName);
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "Port:\t\t";
		stringRep += String.valueOf(port);
		stringRep += System.lineSeparator();
		stringRep += "\t";
		stringRep += "Database Type:\t";
		stringRep += String.valueOf(databaseType);
		
		return stringRep;
	}

	private void applySystemPropertyOverrides()
	{
		String usernameOverride = System.getProperty( "wres.username" );
		if ( usernameOverride != null )
		{
			this.username = usernameOverride;
		}

		String passwordOverride = System.getProperty( "wres.password" );
		if ( passwordOverride != null )
		{
			this.password = passwordOverride;
		}

		String databaseNameOverride = System.getProperty( "wres.databaseName" );
		if ( databaseNameOverride != null )
		{
			this.databaseName = databaseNameOverride;
		}

		String urlOverride = System.getProperty( "wres.url" );
		if ( urlOverride != null )
		{
			this.url = urlOverride;
		}
	}
}
