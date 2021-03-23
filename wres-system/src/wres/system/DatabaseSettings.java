package wres.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Supplier;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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

    /** From databaseType to the properties for its DataSource */
    private final Map<String, Properties> dataSourceProperties;

    /**
     * When the jdbcUrl is specified, it takes precedence over the fields used
     * to programmatically generate a jdbcUrl. The alternative to specifying a
     * jdbcUrl is to specify a type, host, port, and database name.
     * If username and password are specified or password is found in
     * pgpass file they will be added to the properties sent to the DataSource.
     * Furthermore, the jdbcUrl will override the specified database type.
     */

    private String jdbcUrl = null;
    private String host = "localhost";
	private String username = "wres";
	private String password;
	private String port = "5432";
	private String databaseName = "wres";
	private String databaseType = "postgresql";
	private String certificateFileToTrust;
	private int maxPoolSize = 10;
	private int maxIdleTime = 30;
	private boolean attemptToMigrate = true;

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
        mapping.put( "mariadb", "org.mariadb.jdbc.MariaDbDataSource" );
        mapping.put( "mysql", "org.mariadb.jdbc.MariaDbDataSource" );
        mapping.put( "postgresql", "org.postgresql.ds.PGSimpleDataSource" );
        mapping.put( "h2", "org.h2.jdbcx.JdbcDataSource");
		return mapping;
	}


    /** To be called after setting member variables based on wres config */
    private Map<String, Properties> createDatasourceProperties()
    {
        Map<String, Properties> mapping = new TreeMap<>();
        Map<String,String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( this.getUsername() ) )
        {
            commonProperties.put( "user", this.getUsername() );
        }

        if ( Objects.nonNull( this.password ) )
        {
            commonProperties.put( "password", this.password );
        }

        Properties postgresqlProperties = new Properties();
        postgresqlProperties.put( "ssl", Boolean.toString( this.shouldUseSSL() ) );

        if ( Objects.nonNull( this.getDatabaseName() ) )
        {
            postgresqlProperties.put( "databaseName", this.getDatabaseName() );
        }

        if ( Objects.nonNull( this.getPort() ) )
        {
            postgresqlProperties.put( "portNumber", this.getPort() );
        }

        if ( this.shouldValidateSSL() )
        {
            postgresqlProperties.put( "sslfactory", "wres.system.PgSSLSocketFactory" );

            if ( Objects.nonNull( this.getCertificateFileToTrust() ) )
            {
                postgresqlProperties.put( "sslfactoryarg",
                                          this.getCertificateFileToTrust() );
            }
        }
        else
        {
            postgresqlProperties.put("sslfactory", "org.postgresql.ssl.NonValidatingFactory");
        }

        postgresqlProperties.putAll( commonProperties );
        mapping.put("postgresql", postgresqlProperties );

        Properties mysqlProperties = new Properties();
        mysqlProperties.put( "useSSL", Boolean.toString( this.shouldUseSSL() ) );

        if ( this.shouldValidateSSL() )
        {
            mysqlProperties.put( "verifyServerCertificate", "true" );

            if ( Objects.nonNull( this.getCertificateFileToTrust() ) )
            {
                mysqlProperties.put( "serverSslCert",
                                     this.getCertificateFileToTrust() );
            }
        }
        else
        {
            mysqlProperties.put( "trustServerCertificate", "true" );
        }

        mysqlProperties.putAll( commonProperties );
        // If mysql had a custom "yes" property for validating ssl, that would go here

        mapping.put("mysql", mysqlProperties);

        // MariaDB and MySQL share a lineage and use the same jdbc driver.
        mapping.put( "mariadb", mysqlProperties );

        Properties h2Properties = new Properties();
        h2Properties.putAll( commonProperties );

        if ( Objects.nonNull( this.getJdbcUrl() ) )
        {
            h2Properties.put( "url", this.getJdbcUrl() );
        }

        mapping.put( "h2", h2Properties );
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

            this.overrideDatabaseTypeWithJdbcUrl();

            LOGGER.debug( "Db settings after applying jdbc url override: {}",
                          this );

            this.dataSourceProperties = this.createDatasourceProperties();

            LOGGER.debug( "Db settings after creating DataSource properties: {}",
                          this );

            testConnection();

            // TODO: move liquibase migration out of initialization.

            // Stop-gap measure between always-migrate and never-migrate.
            boolean migrate = this.attemptToMigrate;
            String attemptToMigrateSetting = System.getProperty( "wres.attemptToMigrate" );

            if ( attemptToMigrateSetting != null
                 && !attemptToMigrateSetting.isBlank() )
            {
                if ( attemptToMigrateSetting.toLowerCase()
                                            .equals( "true" ) )
                {
                    migrate = true;
                }
                else if ( attemptToMigrateSetting.toLowerCase()
                                                 .equals( "false" ) )
                {
                    migrate = false;
                }
                else
                {
                    LOGGER.warn( "Value for wres.attemptToMigrate must be 'true' or 'false', not '{}'",
                                 attemptToMigrateSetting );
                }
            }

            if ( migrate )
            {
                LOGGER.info( "Beginning database migration. This takes time." );
                DatabaseLockManager lockManager;

                if ( this.getDatabaseType()
                         .equalsIgnoreCase( "postgresql" ) )
                {
                    ConnectionSupplier connectionSupplier =
                            new ConnectionSupplier( this.getDatabaseName() );
                    lockManager = new DatabaseLockManagerPostgres( connectionSupplier );
                }
                else if ( this.getDatabaseType()
                              .equalsIgnoreCase( "h2" ) )
                {
                    lockManager = new DatabaseLockManagerNoop();
                }
                else
                {
                    throw new UnsupportedOperationException( "Only postgresql and h2 are currently supported" );
                }

                try ( DatabaseSchema schema = new DatabaseSchema( this.getDatabaseName(),
                                                                  lockManager );
                      Connection connection = this.getRawConnection( this.getConnectionString(
                              this.getDatabaseName() ) ) )
                {
                    schema.applySchema( connection );
                }
                finally
                {
                    lockManager.shutdown();
                }

                cleanPriorRuns();
                LOGGER.info( "Finished database migration." );
            }
        }
		catch ( XMLStreamException | SQLException | IOException e )
		{
			throw new ExceptionInInitializerError( e );
		}
	}

	private void cleanPriorRuns() throws SQLException
	{
        if ( this.getDatabaseType()
                 .equalsIgnoreCase( "postgresql" ) )
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

            try ( Connection connection = this.getRawConnection( null );
                  Statement clean = connection.createStatement()
            )
            {
                clean.execute( script );
                if ( clean.getResultSet().isBeforeFirst() )
                {
                    LOGGER.debug( "Lock(s) from previous runs of this applications "
                                  + "have been released." );
                }
            }
        }
	}

    /**
	 * For when there is something wrong with the file with database settings.
	 */
	DatabaseSettings()
	{
		this.applySystemPropertyOverrides();
        this.overrideDatabaseTypeWithJdbcUrl();
        this.dataSourceProperties = this.createDatasourceProperties();
	}

    private boolean hostIsValid()
	{
        if ( this.getHost()
                 .equalsIgnoreCase( "localhost" ) )
		{
			return true;
		}

        try (Socket socket = new Socket())
        {
            socket.connect( new InetSocketAddress( this.getHost(),
                                                   Integer.parseInt( this.getPort() ) ),
                            2000 );
            return true;
        }
        catch (IOException ioe)
        {
            LOGGER.warn( "The intended host:port combination ({}:{}) is not accessible due to",
                         this.host, this.port, ioe );
            return false;
        }
	}

	private void testConnection() throws SQLException, IOException
	{
        boolean validURL = this.hostIsValid();

        if (!validURL)
		{
            throw new IOException( "The given database host:port combination ('"
                                   + this.getHost() + ":" + this.getPort()
                                   + "') is not accessible." );
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

    private Properties getConnectionProperties()
    {
        return this.dataSourceProperties.get( this.getDatabaseType() );
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

        return DriverManager.getConnection( connectionString,
                                            this.getConnectionProperties() );
	}

    HikariDataSource createDatasource()
	{
        HikariConfig poolConfig = new HikariConfig();
        Properties properties = this.getConnectionProperties();
        poolConfig.setDataSourceProperties( properties );
        String type = this.getDatabaseType();
        String className = DRIVER_MAPPING.get( type );
        String name = this.getDatabaseName();
        poolConfig.setDataSourceClassName( className );
        int maxSize = this.maxPoolSize;
        poolConfig.setMaximumPoolSize( maxSize );
        poolConfig.setConnectionTimeout( 0 );
        return new HikariDataSource( poolConfig );
	}

    HikariDataSource createHighPriorityDataSource()
    {
        HikariConfig poolConfig = new HikariConfig();
        Properties properties = this.getConnectionProperties();
        poolConfig.setDataSourceProperties( properties );
        String type = this.getDatabaseType();
        String className = DRIVER_MAPPING.get( type );
        String name = this.getDatabaseName();
        poolConfig.setDataSourceClassName( className );
        int maxSize = 5;
        poolConfig.setMaximumPoolSize( maxSize );
        poolConfig.setConnectionTimeout( 0 );
        return new HikariDataSource( poolConfig );
	}

    private String getHost()
    {
        return this.host;
    }

	/**
     * Sets the host name of the database to connect to
     * @param host The host name of the database to connect to
	 */
    private void setHost( String host )
	{
        this.host = host;
    }


    /**
     * Get the jdbc url that was set. Takes precedence over type:host:port
     * @return
     */
    private String getJdbcUrl()
    {
        return this.jdbcUrl;
    }


    /**
     * Sets the jdbc url of the database DataSource. Takes precedence over .
     */

    private void setJdbcUrl( String jdbcUrl )
    {
        this.jdbcUrl = jdbcUrl;
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

    private void setAttemptToMigrate(String value)
    {
        this.attemptToMigrate = Boolean.parseBoolean( value );
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
        if ( Objects.nonNull( this.getJdbcUrl() )
             && !this.getJdbcUrl().isBlank() )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Using the jdbc url specified verbatim: '{}'",
                              this.getJdbcUrl() );
            }

            return this.getJdbcUrl();
        }

        if (!Strings.hasValue( databaseName ))
        {
            databaseName = this.getDatabaseName();
        }

        StringBuilder connectionString = new StringBuilder();
        connectionString.append( "jdbc:" );
        connectionString.append( this.getDatabaseType() );

        if (this.databaseType == "h2" && this.useSSL)
        {
            connectionString.append(":ssl");
        }
        connectionString.append( "://" );
        connectionString.append( this.getHost() );

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
                            LOGGER.warn( "Deprecated 'url' tag found, use 'host' for a hostname or 'jdbcUrl' for a jdbc url instead." );
                            setHost( value );
                            break;
                        case "host":
                            setHost( value );
                            break;
                        case "jdbcUrl":
                            setJdbcUrl( value );
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
                        case "attempt_to_migrate":
                            this.setAttemptToMigrate( value );
                            break;
                        default:
                            LOGGER.warn( "Database configuration option '{}'{}",
                                         tagName,
                                         " was skipped because it's not used." );
                    }
                }
            }
        }
        catch ( IOException e )
        {
            throw new XMLStreamException( "Invalid settings were found within the system configuration.", e );
        }
	}


    /**
     * If needed, override the database type with the jdbc url. To be called
     * after applying System Property overrides. If either the original config
     * or the System Properties has set the jdbc url with a string starting with
     * "jdbc:" and having more than five characters, the jdbc url takes
     * precedence and overrides any specified type.
     */

    private void overrideDatabaseTypeWithJdbcUrl()
    {

        // The jdbcUrl overrides the database type when present and starts with
        // "jdbc:"
        String jdbc = this.getJdbcUrl();

        if ( Objects.nonNull( jdbc )
             && !jdbc.isBlank()
             && jdbc.startsWith( "jdbc:" )
             && jdbc.length() > 5 )
        {
            String jdbcSubstring = jdbc.substring( 5 );
            int secondColonIndex = jdbcSubstring.indexOf( ":" );

            if ( secondColonIndex < 0 )
            {
                LOGGER.warn( "Unable to extract database type from jdbc url {}",
                             jdbc );
            }
            else
            {
                String type = jdbcSubstring.substring( 0, secondColonIndex );
                LOGGER.debug( "Extracted database type {} from jdbc url {}",
                              type, jdbc );
                setDatabaseType( type );
            }
        }
        else
        {
            LOGGER.debug( "No way to override database type with jdbc url {}",
                          jdbc );
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
                .append( "jdbcUrl", jdbcUrl )
                .append( "host", host )
                .append( "username", username )
                // Purposely do not print the password.
                .append( "port", port )
                .append( "databaseName", databaseName )
                .append( "databaseType", databaseType )
                .append( "certificateFileToTrust", certificateFileToTrust )
                .append( "maxPoolSize", maxPoolSize )
                .append( "maxIdleTime", maxIdleTime )
                .append( "dataSourceProperties", this.dataSourceProperties )
                .append( "useSSL", useSSL )
                .append( "validateSSL", validateSSL )
                .append( "queryTimeout", queryTimeout )
                .toString();
    }

    private void applySystemPropertyOverrides()
	{
        validateSystemPropertiesRelatedToDatabase();

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
            LOGGER.warn( "Deprecated wres.url property found. Use wres.databaseHost for a hostname or wres.databaseJdbcUrl for a jdbc url." );
            this.setHost( urlOverride );
        }

        String hostOverride = System.getProperty( "wres.databaseHost" );

        if ( Objects.nonNull( hostOverride ) )
        {
            this.setHost( hostOverride );
        }

        String jdbcUrlOverride = System.getProperty( "wres.databaseJdbcUrl" );
        if ( Objects.nonNull( jdbcUrlOverride ) )
        {
            this.setJdbcUrl( jdbcUrlOverride );
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
                pgPass = PgPassReader.getPassphrase( this.getHost(),
                                                     5432,
                                                     this.getDatabaseName(),
                                                     this.getUsername() );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to read pgpass file.", ioe );
            }

            if ( pgPass != null )
            {
                this.password = pgPass;
            }
            else if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "Could not find password for {}:{}:{}:{} in pgpass file.",
                             this.getHost(), 5432, this.getDatabaseName(), this.getUsername() );
            }
        }
	}

    /**
     * To create a DatabaseLockManager we need to supply connections.
     */
	private final class ConnectionSupplier implements Supplier<Connection>
    {
        private final String rootDbName;
        private ConnectionSupplier( String rootDbName )
        {
            this.rootDbName = rootDbName;
        }

        @Override
        public Connection get()
        {
            try
            {
                return getRawConnection( getConnectionString( rootDbName ) );
            }
            catch ( SQLException se )
            {
                throw new IllegalStateException( "Unable to get connection.", se );
            }
        }
    }

    /**
     * Throws exception when essential system properties are not set properly.
     */

    private void validateSystemPropertiesRelatedToDatabase()
    {
        String zoneProperty = System.getProperty( "user.timezone" );

        if ( !zoneProperty.equals( "UTC" ) )
        {
            // For both H2 and Postgres datetimes to work (as of 2020-06-22), the
            // Java user.timezone must be set to UTC. It might not be required if
            // we use unambiguous "with zone" columns in the timeseriesvalue tables.
            // That is something to explore whenever refactoring timeseriesvalue.
            throw new IllegalStateException( "It is required that the Java "
                                             + "system property 'user.timezone'"
                                             + " be set to 'UTC', e.g. "
                                             + "-Duser.timezone=UTC (found "
                                             + zoneProperty + " instead)." );
        }
    }
}
