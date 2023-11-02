package wres.system;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Helper class for settings methods
 */
public class SettingsHelper
{

    private SettingsHelper()
    {
        // Static utility helper class, disallow construction.
    }


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

    private static Properties createPostgresProperties( DatabaseSettings databaseSettings )
    {
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            commonProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            commonProperties.put( "password", databaseSettings.getPassword() );
        }

        Properties postgresqlProperties = new Properties();

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

        postgresqlProperties.putAll( commonProperties );

        return postgresqlProperties;
    }

    private static Properties createH2Properties( DatabaseSettings databaseSettings )
    {
        Map<DatabaseType, Properties> mapping = new EnumMap<>( DatabaseType.class );
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            commonProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            commonProperties.put( "password", databaseSettings.getPassword() );
        }

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

        mapping.put( DatabaseType.H2, h2Properties );

        return h2Properties;
    }

    private static Properties createMariadbProperties( DatabaseSettings databaseSettings )
    {
        Map<DatabaseType, Properties> mapping = new EnumMap<>( DatabaseType.class );
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            commonProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            commonProperties.put( "password", databaseSettings.getPassword() );
        }

        Properties mariadbProperties = new Properties();

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

    private static Properties createMysqlProperties( DatabaseSettings databaseSettings )
    {
        Map<DatabaseType, Properties> mapping = new EnumMap<>( DatabaseType.class );
        Map<String, String> commonProperties = new TreeMap<>();

        if ( Objects.nonNull( databaseSettings.getUsername() ) )
        {
            commonProperties.put( "user", databaseSettings.getUsername() );
        }

        if ( Objects.nonNull( databaseSettings.getPassword() ) )
        {
            commonProperties.put( "password", databaseSettings.getPassword() );
        }

        Properties mysqlProperties = new Properties();

        mysqlProperties.put( "useSSL", Boolean.toString( databaseSettings.isUseSSL() ) );

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

        mysqlProperties.putAll( commonProperties );

        return mysqlProperties;
    }
}
