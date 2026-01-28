package wres.system;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Helper class for settings methods
 */
public class DatabaseSettingsHelper
{

    /** Re-used string. */
    private static final String PASSWORD = "password";

    /**
     * Creates a data source Property for the passed in databaseSettings
     * @param databaseSettings that we are getting the type of to create the Properties file
     * @return the Data Properties
     */
    public static Properties getDatasourceProperties( DatabaseSettings databaseSettings )
    {
        return switch ( databaseSettings.getDatabaseType() )
        {
            case POSTGRESQL -> createPostgresProperties( databaseSettings );
            case H2 -> createH2Properties( databaseSettings );
            case MYSQL -> createMysqlProperties( databaseSettings );
            case MARIADB -> createMariadbProperties( databaseSettings );
            default -> throw new IllegalArgumentException( "Settings for that DatabaseType are not yet supported" );
        };
    }

    /**
     * Returns the standard SQL for the lead duration unit.
     *
     * @return the lead duration unit string for use in queries
     */

    public static String getLeadDurationString()
    {
        return switch ( DatabaseSettings.LEAD_DURATION_UNIT )
        {
            case MILLIS -> "MILLISECOND";
            case SECONDS -> "SECOND";
            case MINUTES -> "MINUTE";
            case HOURS -> "HOUR";
            default -> throw new UnsupportedOperationException( "Unrecognized lead duration unit." );
        };
    }

    /**
     * Generate database properties for the {@link DatabaseType#POSTGRESQL} database type.
     * @param databaseSettings the database settings
     * @return the database properties
     */

    private static Properties createPostgresProperties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = DatabaseSettingsHelper.getCommonDatabaseProperties( databaseSettings );

        Properties postgresqlProperties = new Properties();

        postgresqlProperties.putAll( commonProperties );

        postgresqlProperties.put( "ssl", Boolean.toString( databaseSettings.isUseSSL() ) );

        if ( Objects.nonNull( databaseSettings.getHost() ) )
        {
            postgresqlProperties.put( "serverName", databaseSettings.getHost() );
        }

        if ( Objects.nonNull( databaseSettings.getDatabaseName() ) )
        {
            postgresqlProperties.put( "databaseName", databaseSettings.getDatabaseName() );
        }

        postgresqlProperties.put( "portNumber", databaseSettings.getPort() );

        if ( databaseSettings.isValidateSSL() )
        {
            postgresqlProperties.put( "sslfactory", "wres.system.PgSSLSocketFactory" );

            if ( Objects.nonNull( databaseSettings.getCertificateFileToTrust() ) )
            {
                postgresqlProperties.put( "sslfactoryarg",
                                          databaseSettings.getCertificateFileToTrust() );
            }
        }
        else
        {
            postgresqlProperties.put( "sslfactory", "org.postgresql.ssl.NonValidatingFactory" );
        }

        // Use server-side prepared statements eagerly
        postgresqlProperties.put( "prepareThreshold", "2" );

        if ( databaseSettings.getQueryTimeout() > 0 )
        {
            // Postgresql has opportunity for multiple settings in 'options'.
            String pgStatementTimeout = "-c statement_timeout="
                                        + databaseSettings.getQueryTimeout()
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
        }

        return postgresqlProperties;
    }

    /**
     * Generate database properties for the {@link DatabaseType#H2} database type.
     * @param databaseSettings the database settings
     * @return the database properties
     */

    private static Properties createH2Properties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = DatabaseSettingsHelper.getCommonDatabaseProperties( databaseSettings );

        Properties h2Properties = new Properties();

        h2Properties.putAll( commonProperties );

        if ( Objects.nonNull( databaseSettings.getJdbcUrl() ) )
        {
            String h2JdbcUrl = databaseSettings.getJdbcUrl();

            if ( databaseSettings.getQueryTimeout() > 0 )
            {
                h2JdbcUrl = h2JdbcUrl
                            + ";MAX_QUERY_TIMEOUT="
                            + databaseSettings.getQueryTimeout() * 1000;
            }

            h2Properties.put( "url", h2JdbcUrl );
        }

        return h2Properties;
    }

    /**
     * Generate database properties for the {@link DatabaseType#MARIADB} database type.
     * @param databaseSettings the database settings
     * @return the database properties
     */

    private static Properties createMariadbProperties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = DatabaseSettingsHelper.getCommonDatabaseProperties( databaseSettings );

        Properties mariadbProperties = new Properties();

        mariadbProperties.putAll( commonProperties );

        mariadbProperties.put( "useSSL", Boolean.toString( databaseSettings.isUseSSL() ) );
        if ( Objects.nonNull( databaseSettings.getHost() ) )
        {
            mariadbProperties.put( "host", databaseSettings.getHost() );
        }

        mariadbProperties.put( "port", databaseSettings.getPort() );
        if ( databaseSettings.isValidateSSL() )
        {
            mariadbProperties.put( "verifyServerCertificate", "true" );

            if ( Objects.nonNull( databaseSettings.getCertificateFileToTrust() ) )
            {
                mariadbProperties.put( "serverSslCert",
                                       databaseSettings.getCertificateFileToTrust() );
            }
        }
        else
        {
            mariadbProperties.put( "trustServerCertificate", "true" );
        }

        // Use server-side prepared statements eagerly
        mariadbProperties.put( "useServerPrepStmts", "true" );

        if ( databaseSettings.getQueryTimeout() > 0 )
        {
            mariadbProperties.put( "max_statement_time", databaseSettings.getQueryTimeout() );
        }

        mariadbProperties.putAll( commonProperties );

        return mariadbProperties;
    }

    /**
     * Generate database properties for the {@link DatabaseType#MYSQL} database type.
     * @param databaseSettings the database settings
     * @return the database properties
     */

    private static Properties createMysqlProperties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = DatabaseSettingsHelper.getCommonDatabaseProperties( databaseSettings );

        Properties mysqlProperties = new Properties();

        mysqlProperties.putAll( commonProperties );

        mysqlProperties.put( "useSSL", Boolean.toString( databaseSettings.isUseSSL() ) );

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            mysqlProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            mysqlProperties.put( PASSWORD, databaseSettings.getPassword() );
        }

        if ( Objects.nonNull( databaseSettings.getHost() ) )
        {
            mysqlProperties.put( "host", databaseSettings.getHost() );
        }

        mysqlProperties.put( "port", databaseSettings.getPort() );

        if ( databaseSettings.isValidateSSL() )
        {
            mysqlProperties.put( "verifyServerCertificate", "true" );

            if ( Objects.nonNull( databaseSettings.getCertificateFileToTrust() ) )
            {
                mysqlProperties.put( "serverSslCert",
                                     databaseSettings.getCertificateFileToTrust() );
            }
        }
        else
        {
            mysqlProperties.put( "trustServerCertificate", "true" );
        }

        mysqlProperties.put( "useServerPrepStmts", "true" );

        if ( databaseSettings.getQueryTimeout() > 0 )
        {
            // MySQL and H2 use milliseconds, not seconds.
            mysqlProperties.put( "max_statement_time",
                                 databaseSettings.getQueryTimeout() * 1000 );
        }

        return mysqlProperties;
    }

    /**
     * Returns the properties that are common to all databases in a writable map.
     * @param databaseSettings the database settings
     * @return the common properties
     */

    private static Map<String, String> getCommonDatabaseProperties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            commonProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            commonProperties.put( PASSWORD, databaseSettings.getPassword() );
        }

        return commonProperties;
    }

    /**
     * Do not construct.
     */

    private DatabaseSettingsHelper()
    {
    }
}
