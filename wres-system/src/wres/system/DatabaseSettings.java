package wres.system;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.Strings;

/**
 * Contains access to configured settings and objects for accessing the database
 * @author Christopher Tubbs
 */
final class DatabaseSettings
{
	private static final Logger LOGGER =
			LoggerFactory.getLogger( DatabaseSettings.class );

	// A mapping of database names to the name of the class for the 
	private static final Map<String, String> DRIVER_MAPPING =
			createDriverMapping();

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
		mapping.put( "mysql", "com.mysql.jdbc.Driver" );
		mapping.put( "postgresql", "org.postgresql.Driver" );
		return mapping;
	}

	/**
	 * Parses the settings for the database from an XMLReader
	 * @param reader The reader containing XML data
	 */
	DatabaseSettings( XMLStreamReader reader )
	{
		try
		{
			while ( reader.hasNext() )
			{
				if ( reader.isStartElement() && reader.getLocalName()
													  .equalsIgnoreCase(
															  "database" ) )
				{
					reader.next();
				}
				else if ( reader.isEndElement() && reader.getLocalName()
														 .equalsIgnoreCase(
																 "database" ) )
				{
					break;
				}
				else
				{
					parseElement( reader );
					reader.next();
				}
			}
			this.applySystemPropertyOverrides();
			this.buildInstance();
			testConnection();
			cleanPriorRuns();
		}
		catch ( XMLStreamException | SQLException | IOException e )
		{
			throw new ExceptionInInitializerError( e );
		}
	}

	private void cleanPriorRuns() throws SQLException
	{
		final String NEWLINE = System.lineSeparator();

		String script = "";
		script += "SELECT pg_cancel_backend(PT.pid)" + NEWLINE;
		script += "FROM pg_locks L" + NEWLINE;
		script += "INNER JOIN pg_stat_all_tables T" + NEWLINE;
		script += "    ON L.relation = t.relid" + NEWLINE;
		script += "INNER JOIN pg_stat_activity PT" + NEWLINE;
		script += "    ON L.pid = PT.pid" + NEWLINE;
		script += "WHERE T.schemaname <> 'pg_toast'::name" + NEWLINE;
		script += "    AND t.schemaname < 'pg_catalog'::name" + NEWLINE;
		script += "    AND usename = '" + this.getUsername() + "'" + NEWLINE;
		script += "    AND datname = '" + this.getDatabaseName() + "'" + NEWLINE;
		script += "GROUP BY PT.pid;";

		try
		{
			Class.forName( DRIVER_MAPPING.get( getDatabaseType() ) );
		}
		catch ( ClassNotFoundException e )
		{
			throw new SQLException( "The database driver could not be found.", e );
		}

		try (Connection connection = DriverManager.getConnection(
		        this.getConnectionString(),
                this.username,
                this.password );
             Statement clean = connection.createStatement()
        )
        {
            clean.execute( script );
            if (clean.getResultSet().isBeforeFirst())
            {
                LOGGER.debug( "Lock(s) from previous runs of this applications "
                              + "have been released." );
            }
		}
	}

    /**
	 * For when there is something wrong with the file with database settings.
	 */
	DatabaseSettings()
	{
		this.applySystemPropertyOverrides();
	}

	private boolean urlIsValid() throws IOException
	{
		if (this.url.equalsIgnoreCase( "localhost" ))
		{
			return true;
		}

        try (Socket socket = new Socket())
        {
            socket.connect( new InetSocketAddress( this.url, Integer.parseInt(this.port) ), 2000 );
            return true;
        }
        catch (IOException ioe)
        {
            LOGGER.error( "The intended URL ({}) is not accessible due to {}", this.url, ioe.toString() );
            return false;
        }
	}

	private void testConnection() throws SQLException, IOException
	{
        Connection connection = null;
        Statement test = null;

        boolean validURL = this.urlIsValid();

        if (!validURL)
		{
			throw new IOException( "The given database URL ('" + this.url + "') is not accessible." );
		}

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

    Connection getRawConnection() throws SQLException
	{
		try
		{
			Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
		}
		catch ( ClassNotFoundException e )
		{
			throw new SQLException( "The driver that will call the database "
									+ "could not be found.", e );
		}
		return DriverManager.getConnection(this.getConnectionString(), this.username, this.password);
	}

	ComboPooledDataSource createDatasource()
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
		catch (PropertyVetoException e)
        {
			LOGGER.error(Strings.getStackTrace(e));
		}

        return datasource;
	}

	ComboPooledDataSource createHighPriorityDataSource()
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
	 * @throws XMLStreamException Any exception occurred when reading from the XML
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

		// Intended order of passphrase precedence:
        // 1) -Dwres.password (but perhaps we should remove this)
        // 2) .pgpass file
        // 3) wresconfig.xml

        // Even though pgpass is not a "system property override" it seems
        // necessary for the logic to be here due to the above order of
        // precedence, combined with the dependency of having the user name,
        // host name, database name to search the pgpass file with.

		String passwordOverride = System.getProperty( "wres.password" );

		if ( passwordOverride != null )
		{
			this.password = passwordOverride;
		}
		else if ( PgPassReader.pgPassExistsAndReadable() )
        {
            String pgPass = null;

            try
            {
                pgPass = PgPassReader.getPassphrase( this.url,
                                                     5432,
                                                     this.databaseName,
                                                     this.username );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to read pgpass file.", ioe );
            }

            if ( pgPass != null )
            {
                this.password = pgPass;
            }
            else
            {
                LOGGER.warn( "Could not find password for {}:{}:{}:{} in pgpass file.",
                             this.url, 5432, this.databaseName, this.username );
            }
        }
	}

    /**
     * Executes the configured Liquibase scripts to keep the database up to date
     */
    private void buildInstance() throws SQLException, IOException
    {
        // Make sure that the database exists if the user has the authority to add one
        this.addDatabase();

        // If this machine already holds a lock on the database, liquibase will
        // prevent this system from proceeding even though it already holds the lock.
        // Thus, we need to make sure there aren't any prior locks assigned to it.
        this.removePriorLocks();

        Database database = null;
        Liquibase liquibase;

        try (Connection connection = DriverManager.getConnection(this.getConnectionString(), this.username, this.password);  )
        {
            database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation( new JdbcConnection( connection));
            URL changelogURL = this.getClass().getClassLoader().getResource( "database/db.changelog-master.xml" );

            Objects.requireNonNull(changelogURL, "The definition for the WRES data model could not be found.");
            liquibase = new Liquibase(
                    "database/db.changelog-master.xml",
                    new ClassLoaderResourceAccessor(),
                    database
            );

            // Liquibase sends a lot of information to its own internal logging system that spits everything out to
            // stdout at the 'info' level. Changing it to 'severe' (i.e. error) to prevent all of the diagnostic
            // messaging.
            liquibase.getLog().setLogLevel( "severe" );

            Contexts contexts = new Contexts(  );
            LabelExpression expression = new LabelExpression(  );
            liquibase.update( contexts, expression );
        }
        catch (SQLException | LiquibaseException e)
        {
            throw new SQLException( "The WRES could not be properly initialized.", e );
        }
        finally
        {
            if (database != null)
            {
                try
                {
                    database.close();
                }
                catch ( DatabaseException e )
                {
                    LOGGER.debug( "The liquibase database class used to update the database could not be closed." );
                }
            }
        }
    }

    private void addDatabase() throws SQLException
    {
        try
        {
            Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
        }
        catch ( ClassNotFoundException e )
        {
            throw new SQLException( "The driver that will call the database "
                                    + "could not be found.", e );
        }

        StringBuilder directConnectionString = new StringBuilder();
        directConnectionString.append( "jdbc:" );
        directConnectionString.append( this.getDatabaseType() );
        directConnectionString.append( "://" );
        directConnectionString.append( this.getUrl() );

        if ( this.getPort() != null )
        {
            directConnectionString.append( ":" );
            directConnectionString.append( this.getPort() );
        }

        directConnectionString.append( "/" );

        // TODO: If we're in a postgresql instance, the default db is postgres. We'll need to add other
        // defaults for other types
        if (this.getDatabaseType().equalsIgnoreCase( "postgresql" ))
        {
            directConnectionString.append( "postgres" );
        }

        try (Connection connection = DriverManager.getConnection(directConnectionString.toString(), this.username, this.password))
        {
            boolean databaseExists = false;
            boolean canAddDatabase = false;

            Statement statement = connection.createStatement();

            ResultSet results = statement.executeQuery( "SELECT * FROM pg_database;" );

            while (results.next())
            {
                String name = results.getString( 1 );
                if (name.equalsIgnoreCase( this.getDatabaseName() ))
                {
                    databaseExists = true;
                    break;
                }
            }

            if (!databaseExists)
            {
                statement.close();
                statement = connection.createStatement();

                // TODO: If we support another database, we'll need to modify this to handle the others as well
                results = statement.executeQuery( "SELECT rolcreatedb FROM pg_roles WHERE rolname = CURRENT_USER;" );
                while (results.next())
                {
                    canAddDatabase = results.getBoolean( 1 );
                }

                statement.close();

                if (!canAddDatabase)
                {
                    throw new SQLException( "The database '" + this.getDatabaseName() + "' does not exist on '" +
                                            this.url +
                                            "' and you do not have permission to create it. Please contact an "
                                            + "administrator to add it."  );
                }

                statement = connection.createStatement();
                statement.execute( "CREATE DATABASE " + this.getDatabaseName() + ";" );
                statement.close();
            }
        }
    }

    private void removePriorLocks() throws SQLException, IOException
    {
        try (Connection connection = this.getRawConnection())
        {
            // Determine whether or not the changeloglock exists in the database
            String script = "SELECT EXISTS (" + System.lineSeparator();
            script += "    SELECT 1" + System.lineSeparator();
            script += "    FROM information_schema.tables" + System.lineSeparator();
            script += "    WHERE table_catalog = '" + this.getDatabaseName() + "'" + System.lineSeparator();
            script += "        AND table_name = 'databasechangeloglock'" + System.lineSeparator();
            script += ");";

            Statement statement = connection.createStatement();

            ResultSet result = statement.executeQuery( script );

            result.next();

            // Get the result from the database to determine whether or not it exists
            boolean changeLogLockExists = result.getBoolean( 1 );

            // If the change log lock table exists
            if ( changeLogLockExists )
            {
                // Collect the address to every interface for the system. Liquibase keeps track of
                // lock ownership by the address of different lock interfaces
                ArrayList<String> addresses = new ArrayList<>();

                try
                {
                    Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
                    for ( NetworkInterface networkInterface : Collections.list( interfaces ) )
                    {
                        for ( InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses() )
                        {
                            String address = interfaceAddress.getAddress().getHostAddress();
                            addresses.add( address + " (" + address + ")" );
                        }
                    }
                }
                catch ( SocketException e )
                {
                    throw new IOException("Could not determine if this system already holds "
                                          + "liquibase locks due to I/O miscommunication.", e);
                }

                // If at least one address was determined...
                if ( !addresses.isEmpty() )
                {
                    // Forcibly remove any prior lock for this system.  Since we know that liquibase
                    // stores ownership via address, we want to delete any locks that are associated
                    // with this system, since any possible lock for this system will prevent this
                    // system (the one that supposedly already owns the lock) from doing its work.
                    StringJoiner builder = new StringJoiner( ",",
                                                             "DELETE FROM databasechangeloglock WHERE lockedby = ANY('{",
                                                             "}');" );
                    addresses.forEach( builder::add );
                    statement = connection.createStatement();
                    statement.execute( builder.toString() );
                    statement.close();
                }
            }
        }
    }
}
