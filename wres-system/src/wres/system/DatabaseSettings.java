package wres.system;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Supplier;

import javax.sql.DataSource;
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
 * 
 * TODO: this class and {@link SystemSettings} need work. Connection verification should happen separately from the
 * instantiation of settings. Connections should mostly be acquired from a common pool (HikariCP), which uses the 
 * {@link DataSource} route, not the {@link DriverManager} route. The latter should not be used. When acquiring 
 * connections for the {@link DatabaseLockManager}, these connections should not be from the same pool, but they should 
 * be obtained by creating a {@link DataSource}, not by using the {@link DriverManager}. See #103431 for further 
 * discussion. 
 * 
 * @author Christopher Tubbs
 */
final class DatabaseSettings
{
    private static final String USE_SSL = "useSSL";
    
	private static final Logger LOGGER =
			LoggerFactory.getLogger( DatabaseSettings.class );

    // Initialize all available driver classes for known databases. Strictly, this should not be necessary for JDBC 4.0+
    // drivers, which should be loaded automatically. However, #103770 suggests otherwise in some environments or class 
    // loading contexts.
    static
    {
        for ( DatabaseType nextType : DatabaseType.values() )
        {
            try
            {
                Class.forName( nextType.getDriverClassName() );
            }
            catch ( ClassNotFoundException classError )
            {
                LOGGER.debug( "Failed to load the database driver class {}.", nextType.getDriverClassName() );
            }
        }
    }

    /** From databaseType to the properties for its DataSource */
    private final Map<DatabaseType, Properties> dataSourceProperties;

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
	private int port = 5432;
	private String databaseName = "wres";
	private DatabaseType databaseType = DatabaseType.POSTGRESQL;
	private String certificateFileToTrust;
	private int maxPoolSize = 10;
	private int maxHighPriorityPoolSize = 5;
	private int maxIdleTime = 30;
	private boolean attemptToMigrate = true;

	private boolean useSSL = false;
	private boolean validateSSL = true;

	// The query timeout needs to be in seconds and we're setting the default for 5 hours (arbitrarily large)
	private int queryTimeout = 60 * 60 * 5;

    // Get-connection timeout is in milliseconds. Default to default query timeout.
    private int connectionTimeoutMs = queryTimeout * 1000;
	
    /** To be called after setting member variables based on wres config */
    private Map<DatabaseType, Properties> createDatasourceProperties()
    {
        Map<DatabaseType, Properties> mapping = new EnumMap<>( DatabaseType.class );
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( this.getUsername() ) )
        {
            commonProperties.put( "user", this.getUsername() );
        }

        if ( Objects.nonNull( this.password ) )
        {
            commonProperties.put( "password", this.password );
        }

        Properties postgresqlProperties = new Properties();
        Properties h2Properties = new Properties();
        Properties mariadbProperties = new Properties();
        Properties mysqlProperties = new Properties();

        postgresqlProperties.put( "ssl", Boolean.toString( this.shouldUseSSL() ) );
        mariadbProperties.put( USE_SSL, Boolean.toString( this.shouldUseSSL() ) );
        mysqlProperties.put( USE_SSL, Boolean.toString( this.shouldUseSSL() ) );

        if ( Objects.nonNull( this.getHost() ) )
        {
            postgresqlProperties.put( "serverName", this.getHost() );
            mariadbProperties.put( "host", this.getHost() );
            mysqlProperties.put( "host", this.getHost() );
        }

        if ( Objects.nonNull( this.getDatabaseName() ) )
        {
            postgresqlProperties.put( "databaseName", this.getDatabaseName() );
        }

        postgresqlProperties.put( "portNumber", this.getPort() );
        mariadbProperties.put( "port", this.getPort() );
        mysqlProperties.put( "port", this.getPort() );

        if ( this.shouldValidateSSL() )
        {
            postgresqlProperties.put( "sslfactory", "wres.system.PgSSLSocketFactory" );
            mariadbProperties.put( "verifyServerCertificate", "true" );
            mysqlProperties.put( "verifyServerCertificate", "true" );

            if ( Objects.nonNull( this.getCertificateFileToTrust() ) )
            {
                postgresqlProperties.put( "sslfactoryarg",
                                          this.getCertificateFileToTrust() );
                mariadbProperties.put( "serverSslCert",
                                       this.getCertificateFileToTrust() );
                mysqlProperties.put( "serverSslCert",
                                     this.getCertificateFileToTrust() );
            }
        }
        else
        {
            postgresqlProperties.put( "sslfactory", "org.postgresql.ssl.NonValidatingFactory" );
            mariadbProperties.put( "trustServerCertificate", "true" );
            mysqlProperties.put( "trustServerCertificate", "true" );
        }

        // Use server-side prepared statements eagerly
        postgresqlProperties.put( "prepareThreshold", "2" );
        mariadbProperties.put( "useServerPrepStmts", "true" );
        mysqlProperties.put( "useServerPrepStmts", "true" );

        if ( this.getQueryTimeout() > 0 )
        {
            // Postgresql has opportunity for multiple settings in 'options'.
            String pgStatementTimeout = "-c statement_timeout="
                                        + this.getQueryTimeout()
                                        + "s";
            String pgOptions = postgresqlProperties.getProperty( "options" );
            if ( pgOptions == null )
            {
                pgOptions = pgStatementTimeout;
            }
            else
            {
                pgOptions = pgOptions + " " + pgStatementTimeout;
            }

            postgresqlProperties.put( "options", pgOptions );
            mariadbProperties.put( "max_statement_time", this.getQueryTimeout() );

            // MySQL and H2 use milliseconds, not seconds.
            mysqlProperties.put( "max_statement_time",
                                 this.getQueryTimeout() * 1000 );

            // H2 MAX_QUERY_TIMEOUT property is added to the jdbc url below.
        }

        postgresqlProperties.putAll( commonProperties );
        mapping.put( DatabaseType.POSTGRESQL, postgresqlProperties );

        mariadbProperties.putAll( commonProperties );
        mapping.put( DatabaseType.MARIADB, mariadbProperties );

        mysqlProperties.putAll( commonProperties );
        mapping.put( DatabaseType.MYSQL, mysqlProperties );

        h2Properties.putAll( commonProperties );

        if ( Objects.nonNull( this.getJdbcUrl() ) )
        {
            String h2JdbcUrl = this.getJdbcUrl();

            if ( this.getQueryTimeout() > 0 )
            {
                h2JdbcUrl = h2JdbcUrl
                            + ";MAX_QUERY_TIMEOUT="
                            + this.getQueryTimeout() * 1000;
            }

            h2Properties.put( "url", h2JdbcUrl );
        }

        mapping.put( DatabaseType.H2, h2Properties );
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

            this.overrideDatabaseAttributesUsingJdbcUrl();

            LOGGER.debug( "Db settings after applying jdbc url override: {}",
                          this );

            this.applyPasswordOverrides();

            this.dataSourceProperties = this.createDatasourceProperties();

            LOGGER.debug( "Db settings after creating DataSource properties: {}",
                          this );

            testConnection();

            // TODO: move liquibase migration out of initialization.
            this.migrateAndClean();
        }
		catch ( XMLStreamException | SQLException | IOException e )
		{
			throw new ExceptionInInitializerError( e );
		}
    }

    /**
     * Attempts to migrate the database, respecting any system property override {@code wres.attemptToMigrate} when 
     * defined.
     * @throws IOException if the migration fails
     * @throws SQLException if cleaning fails after migration
     */

    private void migrateAndClean() throws SQLException, IOException
    {
        // Stop-gap measure between always-migrate and never-migrate.
        boolean migrate = this.attemptToMigrate;
        String attemptToMigrateSetting = System.getProperty( "wres.attemptToMigrate" );

        if ( attemptToMigrateSetting != null
             && !attemptToMigrateSetting.isBlank() )
        {
            if ( attemptToMigrateSetting.equalsIgnoreCase( "true" ) )
            {
                migrate = true;
            }
            else if ( attemptToMigrateSetting.equalsIgnoreCase( "false" ) )
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

            if ( this.getDatabaseType() == DatabaseType.POSTGRESQL )
            {
                ConnectionSupplier connectionSupplier = new ConnectionSupplier();
                lockManager = new DatabaseLockManagerPostgres( connectionSupplier );
            }
            else if ( this.getDatabaseType() == DatabaseType.H2 )
            {
                lockManager = new DatabaseLockManagerNoop();
            }
            else
            {
                throw new UnsupportedOperationException( "Only postgresql and h2 are currently supported" );
            }

            try ( DatabaseSchema schema = new DatabaseSchema( this.getDatabaseName(),
                                                              lockManager );
                  Connection connection = this.getRawConnection(); )
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
	
	private void cleanPriorRuns() throws SQLException
	{
        if ( this.getDatabaseType() == DatabaseType.POSTGRESQL )
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

            try ( Connection connection = this.getRawConnection();
                  Statement clean = connection.createStatement() )
            {
                clean.execute( script );
                try ( ResultSet resultSet = clean.getResultSet() )
                {
                    if ( resultSet.isBeforeFirst() )
                    {
                        LOGGER.debug( "Lock(s) from previous runs of this applications "
                                      + "have been released." );
                    }
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
        this.overrideDatabaseAttributesUsingJdbcUrl();
        this.applyPasswordOverrides();
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
                                                   this.getPort() ),
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

        try ( Connection connection = this.getRawConnection();
              Statement test = connection.createStatement() )
        {
            test.execute("SELECT 1;");
        }
        catch ( SQLException sqlError )
        {
            String message = "The database could not be reached for connection verification: "
                             + System.lineSeparator()
                             + System.lineSeparator();
            message += sqlError.getMessage() + System.lineSeparator() + System.lineSeparator();
            message += "Please ensure that you have:" + System.lineSeparator();
            message += "1) The correct URL to your database: " + this.getConnectionString( null )
                       + System.lineSeparator();
            message += "2) The correct username for your database: " + this.getUsername() + System.lineSeparator();
            message += "3) The correct password for your user in the database" + System.lineSeparator();
            message += "4) An active connection to a network that may reach the requested database server"
                       + System.lineSeparator();
            message += "5) The correct database driver class on the application classpath"
                       + System.lineSeparator()
                       + System.lineSeparator();
            message += "The application will now exit.";
            throw new SQLException( message, sqlError );
        }
    }

    private Properties getConnectionProperties()
    {
        return this.dataSourceProperties.get( this.getDatabaseType() );
    }

    Connection getRawConnection() throws SQLException
	{
        String connectionString = this.getConnectionString( this.databaseName );
        return DriverManager.getConnection( connectionString,
                                            this.getConnectionProperties() );
	}

    /**
     * @param maxPoolSize the maximum pool size
     * @param connectionTimeOutMs the maximum connection timeout in milliseconds
     * @return
     */
    DataSource createDataSource( int maxPoolSize, long connectionTimeOutMs )
	{
        HikariConfig poolConfig = new HikariConfig();
        Properties properties = this.getConnectionProperties();
        poolConfig.setDataSourceProperties( properties );
        DatabaseType type = this.getDatabaseType();
        String className = type.getDataSourceClassName();
        poolConfig.setDataSourceClassName( className );
        poolConfig.setMaximumPoolSize( maxPoolSize );
        poolConfig.setConnectionTimeout( connectionTimeOutMs );
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

    private int getPort()
    {
        return this.port;
    }

	/**
	 * Sets the identifier for the port used to connect to the database
	 * @param port
	 */
    private void setPort ( int port )
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
    private void setDatabaseName( String databaseName )
    {
	    if (databaseName.contains( ";" ) || databaseName.contains( "\"" ) || databaseName.contains( "'" ))
        {
            String message = String.format( "%s is not a valid database name.", databaseName );
            throw new IllegalArgumentException( message );
        }
		this.databaseName = databaseName;
	}

	/**
	 * Sets the name of the type of database in use (such as 'mysql', 'postgresql', etc)
	 * @param databaseType The name of the database to connect to
	 */
    private void setDatabaseType ( DatabaseType databaseType )
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

        if ( this.databaseType == DatabaseType.H2
             && this.useSSL )
        {
            connectionString.append(":ssl");
        }

        connectionString.append( "://" );
        connectionString.append( this.getHost() );

        connectionString.append( ":" );
        connectionString.append( this.getPort() );

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
                            String typeString = value.toUpperCase();
                            DatabaseType type = DatabaseType.valueOf( typeString );
                            setDatabaseType( type );
                            break;
                        case "port":
                            this.setPort( Integer.parseInt( value ) );
                            break;
                        case "name":
                            this.setDatabaseName( value );
                            break;
                        case "password":
                            this.setPassword( value );
                            break;
                        case "url":
                            LOGGER.warn( "Deprecated 'url' tag found, use 'host' for a hostname or 'jdbcUrl' for a jdbc url instead." );
                            setHost( value );
                            break;
                        case "host":
                            this.setHost( value );
                            break;
                        case "jdbcUrl":
                            this.setJdbcUrl( value );
                            break;
                        case "username":
                            this.setUsername( value );
                            break;
                        case "max_pool_size":
                            this.maxPoolSize = Integer.parseInt( value );
                            break;
                        case "max_idle_time":
                            this.maxIdleTime = Integer.parseInt( value );
                            break;
                        case "query_timeout":
                            this.queryTimeout = Integer.parseInt( value );
                            break;
                        case "connectionTimeoutMs":
                            this.connectionTimeoutMs = Integer.parseInt( value );
                            break;
                        case "use_ssl":
                            this.setUseSSL( Boolean.parseBoolean( value ) );
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
        catch ( IllegalArgumentException e )
        {
            throw new XMLStreamException( "Invalid settings were found within the system configuration.", e );
        }
	}


    /**
     * If needed, override the database attributes by parsing the jdbc url.
     * To be called after applying System Property overrides. If either the
     * original config or the System Properties has set the jdbc url with a
     * string starting with "jdbc:" and having more than five characters, the
     * jdbc url takes precedence and overrides any specified attributes. For
     * host name and port, only the first host name and port are found.
     */

    private void overrideDatabaseAttributesUsingJdbcUrl()
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
            int firstSlashIndex = jdbcSubstring.indexOf( '/' );
            int secondSlashIndex = jdbcSubstring.indexOf( '/',
                                                          firstSlashIndex + 1 );
            int thirdSlashIndex = jdbcSubstring.indexOf( '/',
                                                         secondSlashIndex + 1 );
            int questionMarkIndex = jdbcSubstring.indexOf( '?' );


            if ( secondColonIndex < 0 )
            {
                LOGGER.warn( "Unable to extract database type from jdbc url {}",
                             jdbc );
            }
            else
            {
                String type = jdbcSubstring.substring( 0, secondColonIndex )
                                           .toUpperCase();
                LOGGER.debug( "Extracted database type {} from jdbc url {}",
                              type, jdbc );
                DatabaseType typeEnum = DatabaseType.valueOf( type );
                setDatabaseType( typeEnum );
            }

            if ( firstSlashIndex > 0
                 && secondSlashIndex > firstSlashIndex
                 && thirdSlashIndex > secondSlashIndex )
            {
                // We should be able to extract the host name following '//'.
                String hostAndMaybePort = jdbcSubstring.substring( secondSlashIndex + 1,
                                                                   thirdSlashIndex );
                int portColonIndex = hostAndMaybePort.indexOf( ':' );

                if ( portColonIndex > 0 )
                {
                    // There is a port because a colon was found after host.
                    String portRaw = hostAndMaybePort.substring( portColonIndex + 1 );
                    String hostName = hostAndMaybePort.substring( 0, portColonIndex );

                    LOGGER.debug( "Extracted host {} from jdbcUrl {}",
                                  hostName, jdbc );
                    this.setHost( hostName );

                    try
                    {
                        int portNumber = Integer.parseInt( portRaw );
                        LOGGER.debug( "Extracted port {} from jdbcUrl {}",
                                      portNumber, jdbc );
                        this.setPort( portNumber );
                    }
                    catch ( NumberFormatException nfe )
                    {
                        LOGGER.warn( "Unable to parse port number from jdbc url {}. Attempt from substring {} failed.",
                                     jdbc, portRaw );
                    }
                }
                else
                {
                    // There is no port because colon was not found after host.
                    LOGGER.debug( "Extracted host {} from jdbcUrl {} (and no port)",
                                  hostAndMaybePort, jdbc );
                    this.setHost( hostAndMaybePort );
                }

                String dbName = "";

                // The db name follows the third slash but not including '?'
                if ( questionMarkIndex <= thirdSlashIndex )
                {
                    dbName = jdbcSubstring.substring( thirdSlashIndex + 1 );
                }
                else
                {
                    dbName = jdbcSubstring.substring( thirdSlashIndex + 1,
                                                      questionMarkIndex );
                }

                if ( !dbName.isBlank() )
                {
                    LOGGER.debug( "Extracted database name {} from jdbc url {}",
                                  dbName, jdbc );
                    this.setDatabaseName( dbName );
                }
                else
                {
                    LOGGER.warn( "Unable to extract database name from jdbc url {}",
                                 jdbc );
                }
            }
            else
            {
                LOGGER.warn( "Unable to extract database host, port, or name from jdbc url {}",
                             jdbc );
            }
        }
        else
        {
            LOGGER.debug( "No way to override database attributes with jdbc url {}",
                          jdbc );
        }
    }

	public DatabaseType getDatabaseType()
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
	
	/**
	 * @return the connection pool size
	 */
	int getMaxPoolSize()
	{
	    return this.maxPoolSize;
	}

	/**
	 * @return the high priority connection pool size
	 */
	int getMaxHighPriorityPoolSize()
	{
	    return this.maxHighPriorityPoolSize;
	}

    /**
     * @return the connection timeout in millseconds
     */
    
    int getConnectionTimeoutMs()
    {
        return this.connectionTimeoutMs;
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
                .append( "maxHighPriorityPoolSize", maxHighPriorityPoolSize )
                .append( "maxIdleTime", maxIdleTime )
                // Purposely do not print passphrases.
                //.append( "dataSourceProperties", this.dataSourceProperties )
                .append( USE_SSL, useSSL )
                .append( "validateSSL", validateSSL )
                .append( "queryTimeout", queryTimeout )
                .append( "connectionTimeoutMs", connectionTimeoutMs )
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

        String portOverride = System.getProperty( "wres.databasePort" );

        if ( Objects.nonNull( portOverride ) )
        {
            int portNumber = Integer.parseInt( portOverride );
            this.setPort( portNumber );
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

        String connectionTimeoutMsOverride = System.getProperty( "wres.databaseConnectionTimeoutMs" );
        if ( connectionTimeoutMsOverride != null )
        {
            this.connectionTimeoutMs = Integer.parseInt( connectionTimeoutMsOverride );
        }
	}

    /**
     * Figure out the password based on system properties and/or wres config
     * where host, port, and database name have already been set. Needs to be
     * called after all other overrides have been applied and/or parsing of
     * the jdbcUrl.
     */

    private void applyPasswordOverrides()
    {
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
            this.setPassword( passwordOverride );
        }
        else if ( this.getDatabaseType() == DatabaseType.POSTGRESQL
                  && PgPassReader.pgPassExistsAndReadable() )
        {
            String pgPass = null;

            try
            {
                pgPass = PgPassReader.getPassphrase( this.getHost(),
                                                     this.getPort(),
                                                     this.getDatabaseName(),
                                                     this.getUsername() );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to read pgpass file.", ioe );
            }

            if ( pgPass != null )
            {
                this.setPassword( pgPass );
            }
            else if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "Could not find password for {}:{}:{}:{} in pgpass file.",
                             this.getHost(), this.getPort(), this.getDatabaseName(), this.getUsername() );
            }
        }
    }

    /**
     * To create a DatabaseLockManager we need to supply connections.
     */
	private final class ConnectionSupplier implements Supplier<Connection>
    {
        private ConnectionSupplier() {}

        @Override
        public Connection get()
        {
            try
            {
                return getRawConnection();
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
