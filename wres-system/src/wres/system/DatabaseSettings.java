package wres.system;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.builder.ToStringBuilder;
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

	private static final Map<String, Map<String, String>> DRIVER_PROPERTIES =
            createDriverProperties();

	private String url = "localhost";
	private String username = "wres";
	private String password;
	private String port = "5432";
	private String databaseName = "wres";
	private String databaseType = "postgresql";
	private String certificateFileToTrust;
	private int maxPoolSize = 10;
	private int maxIdleTime = 30;

	private Properties connectionProperties;

	private boolean useSSL = false;
	private boolean validateSSL = true;

	// The query timeout needs to be in seconds and we're setting the default for 5 hours (arbitrarily large)
	private int queryTimeout = 60 * 60 * 5;

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

	private static Map<String, Map<String, String>> createDriverProperties()
    {
        Map<String, Map<String, String>> mapping = new TreeMap<>();

        Map<String, String> postgresqlProperties = new TreeMap<>();
        postgresqlProperties.put( "ssl", "ssl" );
        postgresqlProperties.put("validate", "sslfactory");
        postgresqlProperties.put("nonValidateAnswer", "org.postgresql.ssl.NonValidatingFactory");
        // If postgresql had a custom "yes" property for validating ssl, that would go here
        postgresqlProperties.put( "validateAnswer", "wres.system.PgSSLSocketFactory" );
        mapping.put("postgresql", postgresqlProperties);

        Map<String, String> mysqlProperties = new TreeMap<>();
        mysqlProperties.put("ssl", "useSSL");
        mysqlProperties.put("validate", "verifyServerCertificate");
        mysqlProperties.put("nonValidateAnswer", "false");
        // If mysql had a custom "yes" property for validating ssl, that would go here

        mapping.put("mysql", mysqlProperties);

        return mapping;
    }

	/**
	 * Parses the settings for the database from an XMLReader
	 * @param reader The reader containing XML data
	 */
	DatabaseSettings( XMLStreamReader reader )
	{
        LOGGER.debug( "Default db settings before applying settings xml and before applying system property overrides: {}",
                      this );

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

            LOGGER.debug( "Db settings after applying settings xml and before applying system property overrides: {}",
                          this );

			this.applySystemPropertyOverrides();

			LOGGER.debug( "Db settings after applying system property overrides: {}",
                          this );

            DatabaseSchema schema = new DatabaseSchema(this.getDatabaseName());

            String rootDatabaseName = null;

            // TODO: If we're in a postgresql instance, the default db is postgres. We'll need to add other
            // defaults for other types
            if (this.getDatabaseType().equalsIgnoreCase( "postgresql" ))
            {
                rootDatabaseName = "postgres";
            }

            try (Connection connection = this.getRawConnection( this.getConnectionString( rootDatabaseName ) ))
            {
                schema.createDatabase( connection );
            }

            try (Connection connection = this.getRawConnection( this.getConnectionString( this.getDatabaseName() ) ))
            {
                schema.applySchema( connection );
            }

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
		script += "SELECT pg_terminate_backend(PT.pid)" + NEWLINE;
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

		try (Connection connection = this.getRawConnection( null );
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

	private boolean urlIsValid()
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
            LOGGER.warn( "The intended URL ({}) is not accessible due to",
                         this.url, ioe );
            return false;
        }
	}

	private void testConnection() throws SQLException, IOException
	{
        boolean validURL = this.urlIsValid();

        if (!validURL)
		{
			throw new IOException( "The given database URL ('" + this.url + "') is not accessible." );
		}

        try
        {
            Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
        }
        catch (ClassNotFoundException classError)
        {
            String message = "The specified database type of '" +
                             this.getDatabaseType() +
                             "' is not valid and a connection could not be created. Shutting down...";
            throw new SQLException( message, classError);
        }

        try ( Connection connection = this.getRawConnection( null );
              Statement test = connection.createStatement() )
        {
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
    }

    private synchronized Properties getConnectionProperties()
    {
        if (this.connectionProperties == null)
        {
            connectionProperties = new Properties();

            Map<String, String> driverProperties = DatabaseSettings.DRIVER_PROPERTIES.get( this.getDatabaseType() );

            if ( driverProperties == null )
            {
                LOGGER.debug( "No custom properties will be applied to database connections." );
                return connectionProperties;
            }

            if ( this.shouldUseSSL() )
            {
                if ( driverProperties.containsKey( "ssl" ) )
                {
                    String sslName = driverProperties.get( "ssl" );
                    connectionProperties.setProperty( sslName, String.valueOf( this.useSSL ) );

                    String validateName = driverProperties.getOrDefault( "validate", "" );

                    if ( this.shouldValidateSSL() )
                    {
                        if ( !validateName.isEmpty() && driverProperties.containsKey( "validateAnswer" ) )
                        {
                            connectionProperties.setProperty( validateName, driverProperties.get( "validateAnswer" ) );

                            // Hate to do this, but an additional property is
                            // needed for postgres. If mariadb or h2 need the
                            // same, then we can genericize at that moment.
                            // It looks like mariadb allows something like this
                            // with the serverSslCert property.
                            if ( this.getDatabaseType().equals( "postgresql" ) )
                            {
                                connectionProperties.setProperty( "sslfactoryarg",
                                                                  this.certificateFileToTrust );
                            }
                        }
                        else
                        {
                            LOGGER.debug( "The system was set to validate SSL, but {} either doesn't "
                                          + "support validation toggling or there isn't a non-default "
                                          + "property for its activation.", this.getDatabaseType() );
                        }
                    }
                    else
                    {
                        if ( !validateName.isEmpty() && driverProperties.containsKey( "nonValidateAnswer" ) )
                        {
                            connectionProperties.setProperty( validateName, driverProperties.get( "nonValidateAnswer" ) );
                        }
                        else
                        {
                            LOGGER.debug( "The system was set to ignore SSL validation, but {} either doesn't "
                                          + "support validation toggling or there isn't a non-default "
                                          + "property for its activation.", this.getDatabaseType() );
                        }
                    }
                }
                else
                {
                    LOGGER.debug( "The system was set to utilize SSL for its database connection, "
                                  + "but {} either doesn't support it or there isn't a non-default "
                                  + "property for its activation.", this.getDatabaseType() );
                }
            }
        }

        return this.connectionProperties;
    }

    Connection getRawConnection(String connectionString) throws SQLException
	{
	    if (!Strings.hasValue( connectionString ))
        {
            connectionString = this.getConnectionString( this.databaseName);
        }

		try
		{
			Class.forName(DRIVER_MAPPING.get(getDatabaseType()));
		}
		catch ( ClassNotFoundException e )
		{
			throw new SQLException( "The driver that will call the database "
									+ "could not be found.", e );
		}

		// Copy existing properties, don't modify them.
		Properties connectionProperties = new Properties( this.getConnectionProperties() );

        if ( this.getUsername() != null )
        {
            connectionProperties.setProperty( "user", this.getUsername() );
        }

        if ( this.password != null )
        {
            connectionProperties.setProperty( "password", this.password );
        }

        return DriverManager.getConnection( connectionString, connectionProperties );
	}

	ComboPooledDataSource createDatasource()
	{
		ComboPooledDataSource datasource = new ComboPooledDataSource();

		try {

            datasource.setProperties( this.getConnectionProperties() );
			datasource.setDriverClass(DRIVER_MAPPING.get(getDatabaseType()));
			datasource.setJdbcUrl(getConnectionString(this.databaseName));
			datasource.setUser(username);
			datasource.setPassword(password);
			datasource.setAutoCommitOnClose(true);
            datasource.setMaxIdleTime( maxIdleTime );
            datasource.setMaxPoolSize( maxPoolSize );
            datasource.setInitialPoolSize( maxPoolSize );
			datasource.setPreferredTestQuery("SELECT 1");
            datasource.setTestConnectionOnCheckin( true );
			datasource.setIdleConnectionTestPeriod( 5 );
			datasource.setAcquireRetryAttempts( 10 );
		} 
		catch (PropertyVetoException e)
        {
			LOGGER.warn( "A property veto issue occurred", e );
		}

        return datasource;
	}

	ComboPooledDataSource createHighPriorityDataSource()
	{
		ComboPooledDataSource highPrioritySource = new ComboPooledDataSource();

		try
		{
            highPrioritySource.setProperties( this.getConnectionProperties() );
			highPrioritySource.setDriverClass(DRIVER_MAPPING.get(getDatabaseType()));
			highPrioritySource.setJdbcUrl(getConnectionString(this.databaseName));
			highPrioritySource.setUser(username);
			highPrioritySource.setPassword(password);
			highPrioritySource.setAutoCommitOnClose(true);
			highPrioritySource.setMaxIdleTime(10);
			highPrioritySource.setMaxPoolSize(5);
			highPrioritySource.setPreferredTestQuery("SELECT 1");
			highPrioritySource.setTestConnectionOnCheckin( true );
            highPrioritySource.setIdleConnectionTestPeriod( 5 );
            highPrioritySource.setAcquireRetryAttempts( 5 );
		}
		catch (PropertyVetoException e)
		{
			LOGGER.error("Property Configuration on database connection pool failed.", e);
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

    private void setUseSSL(final boolean useSSL)
    {
        this.useSSL = useSSL;
    }

    private boolean shouldUseSSL()
    {
        return this.useSSL;
    }

    private void setValidateSSL(final boolean validateSSL)
    {
        this.validateSSL = validateSSL;
    }

    private boolean shouldValidateSSL()
    {
        return this.validateSSL;
    }

    private void setCertificateFileToTrust( String certificateFileToTrust )
    {
        this.certificateFileToTrust = certificateFileToTrust;
    }

    private String getCertificateFileToTrust()
    {
        return this.certificateFileToTrust;
    }


	/**
	 * Sets the name of the database to connect to
	 * @param databaseName The name of the database to access
	 */
    private void setDatabaseName (String databaseName) throws IOException
    {
	    if (databaseName.contains( ";" ) || databaseName.contains( "\"" ) || databaseName.contains( "'" ))
        {
            throw new IOException( String.format("%s is not a valid database name.", databaseName) );
        }
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
    private String getConnectionString (String databaseName)
    {
        if (!Strings.hasValue( databaseName ))
        {
            databaseName = this.getDatabaseName();
        }

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
        connectionString.append( databaseName );

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
	    try
        {
            if ( reader.isStartElement() )
            {
                String tagName = reader.getLocalName();
                reader.next();
                if ( reader.isCharacters() )
                {
                    int beginIndex = reader.getTextStart();
                    int endIndex = reader.getTextLength();
                    String value = new String( reader.getTextCharacters(), beginIndex, endIndex ).trim();

                    switch ( tagName )
                    {
                        case "database_type":
                            setDatabaseType( value );
                            break;
                        case "port":
                            setPort( value );
                            break;
                        case "name":
                            setDatabaseName( value );
                            break;
                        case "password":
                            setPassword( value );
                            break;
                        case "url":
                            setUrl( value );
                            break;
                        case "username":
                            setUsername( value );
                            break;
                        case "max_pool_size":
                            maxPoolSize = Integer.parseInt( value );
                            break;
                        case "max_idle_time":
                            maxIdleTime = Integer.parseInt( value );
                            break;
                        case "query_timeout":
                            queryTimeout = Integer.parseInt( value );
                            break;
                        case "use_ssl":
                            setUseSSL( Boolean.parseBoolean( value ) );
                            break;
                        case "validate_ssl":
                            this.setValidateSSL(Boolean.parseBoolean( value ));
                            break;
                        case "certificate_file_to_trust":
                            this.setCertificateFileToTrust( value );
                            break;
                        default:
                            LOGGER.error( "Tag of type: '{}' is not valid for database configuration.",
                                          tagName );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new XMLStreamException( "Invalid settings were found within the system configuration.", e );
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

	public int getQueryTimeout()
    {
        return this.queryTimeout;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                .append( "url", url )
                .append( "username", username )
                .append( "password", "(REDACTED)" )
                .append( "port", port )
                .append( "databaseName", databaseName )
                .append( "databaseType", databaseType )
                .append( "certificateFileToTrust", certificateFileToTrust )
                .append( "maxPoolSize", maxPoolSize )
                .append( "maxIdleTime", maxIdleTime )
                .append( "connectionProperties", connectionProperties )
                .append( "useSSL", useSSL )
                .append( "validateSSL", validateSSL )
                .append( "queryTimeout", queryTimeout )
                .toString();
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

		String useSSLOverride = System.getProperty( "wres.useSSL" );
		if (useSSLOverride != null)
        {
            this.useSSL = Boolean.parseBoolean( useSSLOverride );
        }

        String validateSSLOverride = System.getProperty( "wres.validateSSL" );
		if (validateSSLOverride != null)
        {
            this.validateSSL = Boolean.parseBoolean( validateSSLOverride );
        }

        String certificateFileToTrustOverride = System.getProperty( "wres.certificateFileToTrust" );
		if ( certificateFileToTrustOverride != null)
        {
            this.setCertificateFileToTrust( certificateFileToTrustOverride );
        }

		String timeoutOverride = System.getProperty( "wres.query_timeout" );
        if (timeoutOverride != null)
        {
            this.queryTimeout = Integer.parseInt( timeoutOverride );
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
}
