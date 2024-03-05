package wres.system;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.function.IntConsumer;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.SystemSettings.SystemSettingsBuilder;

/**
 * A factory class for building system settings.
 */
public class SettingsFactory
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SettingsFactory.class );

    private static final URI DEFAULT_CONFIG_PATH = URI.create( "wresconfig.xml" );

    /**
     * Creates a SystemSettings with the default configs defined in the classes and are available in the classpath
     * @return SystemSettings with all overrides applied
     */
    public static SystemSettings createSettingsFromDefaultXml()
    {
        InputStream resourceAsStream =
                SettingsFactory.class.getClassLoader().getResourceAsStream( DEFAULT_CONFIG_PATH.getPath() );

        return createSettingsFromXml( resourceAsStream );
    }

    /**
     * Creates SystemSettings from the input stream of a config file
     * @param xmlInputStream the input stream
     * @return SystemSettings with all overrides applied
     */
    public static SystemSettings createSettingsFromXml( InputStream xmlInputStream )
    {
        try
        {
            JAXBContext jaxbContext = JAXBContext.newInstance( SystemSettings.class );
            Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
            SystemSettings systemSettings = ( SystemSettings ) unmarshaller.unmarshal( xmlInputStream );

            // Get database configurations and convert to builder
            DatabaseSettings.DatabaseSettingsBuilder databaseBuilder =
                    systemSettings.getDatabaseConfiguration().toBuilder();

            // Apply database overrides
            applyDatabaseSystemPropertyOverrides( databaseBuilder );
            String jdbcUrl;
            if ( ( jdbcUrl = databaseBuilder.build().getJdbcUrl() ) != null )
            {
                overrideDatabaseAttributesUsingJdbcUrl( databaseBuilder, jdbcUrl );
            }

            String passwordOverrides = getPasswordOverrides( databaseBuilder.build() );
            if ( passwordOverrides != null )
            {
                databaseBuilder.password( passwordOverrides );
            }

            // Get system configurations and convert to a builder
            SystemSettingsBuilder systemBuilder = systemSettings.toBuilder();

            // Apply system property overrides to the system configs
            applySystemPropertyOverrides( systemBuilder, systemSettings );

            //return SystemSettings with all overrides applied
            return systemBuilder
                    .databaseConfiguration( databaseBuilder.build() )
                    .build();
        }
        catch ( JAXBException e )
        {
            throw new SettingsReadException( "Could not read the system settings.,", e );
        }
    }

    /**
     * Method for construction of default settings used in testing
     * @return SystemSettings
     */
    public static SystemSettings defaultTest()
    {
        DatabaseSettings.DatabaseSettingsBuilder builder = DatabaseSettings.builder();
        applyDatabaseSystemPropertyOverrides( builder );
        builder.password( getPasswordOverrides( DatabaseSettings.builder().build() ) );

        return SystemSettings.builder().databaseConfiguration( builder.build() ).build();
    }

    /**
     * <p>Figure out the password based on system properties and/or wres config
     * where host, port, and database name have already been set. Needs to be
     * called after all other overrides have been applied and/or parsing of
     * the jdbcUrl.
     *
     * <p>If there is no value it returns null which is the default value of password
     * @param databaseSettings the database settings
     * @return the password override string
     */

    public static String getPasswordOverrides( DatabaseSettings databaseSettings )
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
            return passwordOverride;
        }
        else if ( databaseSettings.getDatabaseType() == DatabaseType.POSTGRESQL
                  && PgPassReader.pgPassExistsAndReadable() )
        {
            String pgPass = null;

            try
            {
                pgPass = PgPassReader.getPassphrase( databaseSettings.getHost(),
                                                     databaseSettings.getPort(),
                                                     databaseSettings.getDatabaseName(),
                                                     databaseSettings.getUsername() );
            }
            catch ( IOException ioe )
            {
                LOGGER.warn( "Failed to read pgpass file.", ioe );
            }

            if ( pgPass != null )
            {
                return pgPass;
            }
            else if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "Could not find password for {}:{}:{}:{} in pgpass file.",
                             databaseSettings.getHost(),
                             databaseSettings.getPort(),
                             databaseSettings.getDatabaseName(),
                             databaseSettings.getUsername() );
            }
        }
        return null;
    }

    /**
     * Applies system property overrides.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void applySystemPropertyOverrides( SystemSettingsBuilder systemBuilder,
                                                      SystemSettings systemSettings )
    {
        String useDatabaseString = System.getProperty( "wres.useDatabase" );
        if ( Objects.nonNull( useDatabaseString ) )
        {
            systemBuilder.useDatabase( "true".equalsIgnoreCase( useDatabaseString ) );
        }

        String storePath = System.getProperty( "wres.StorePath" );
        if ( storePath != null )
        {
            systemBuilder.netcdfStorePath( storePath );
        }

        SettingsFactory.setMaximumIngestThreads( systemBuilder, systemSettings );
        SettingsFactory.setFetchSize( systemBuilder, systemSettings );
        SettingsFactory.setMaximumCopies( systemBuilder, systemSettings );
        SettingsFactory.setNetcdfCachePeriod( systemBuilder, systemSettings );
        SettingsFactory.setMinimumCachedNetcdf( systemBuilder, systemSettings );
        SettingsFactory.setMaximumCachedNetcdf( systemBuilder, systemSettings );
        SettingsFactory.setHardNetcdfCacheLimit( systemBuilder, systemSettings );
        SettingsFactory.setMaximumWebClientThreads( systemBuilder, systemSettings );
        SettingsFactory.setMaximumArchiveThreads( systemBuilder, systemSettings );

        SettingsFactory.setMaximumPoolThreads( systemBuilder, systemSettings );
        SettingsFactory.setMaximumMetricThreads( systemBuilder, systemSettings );
        SettingsFactory.setMaximumSlicingThreads( systemBuilder, systemSettings );
        SettingsFactory.setMaximumProductThreads( systemBuilder, systemSettings );
        SettingsFactory.setMaximumReadThreads( systemBuilder, systemSettings );
        SettingsFactory.setFeatureBatchSize( systemBuilder, systemSettings );
        SettingsFactory.setFeatureBatchThreshold( systemBuilder, systemSettings );
    }

    /**
     * Sets the fetch size.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setFetchSize( SystemSettingsBuilder systemBuilder,
                                      SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.fetchSize",
                                                               systemSettings.getFetchSize(),
                                                               systemBuilder::fetchSize,
                                                               0 );
    }

    /**
     * Sets the maximum copies.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumCopies( SystemSettingsBuilder systemBuilder,
                                          SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumCopies",
                                                               systemSettings.getMaximumCopies(),
                                                               systemBuilder::maximumCopies,
                                                               0 );
    }

    /**
     * Sets the cache period for NetCDF.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setNetcdfCachePeriod( SystemSettingsBuilder systemBuilder,
                                              SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.netcdfCachePeriod",
                                                               systemSettings.getNetcdfCachePeriod(),
                                                               systemBuilder::netcdfCachePeriod,
                                                               0 );
    }

    /**
     * Sets the minimum cached NetCDF argument.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMinimumCachedNetcdf( SystemSettingsBuilder systemBuilder,
                                                SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.minimumCachedNetcdf",
                                                               systemSettings.getMinimumCachedNetcdf(),
                                                               systemBuilder::minimumCachedNetcdf,
                                                               0 );
    }

    /**
     * Sets the maximum cached NetCDF argument.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumCachedNetcdf( SystemSettingsBuilder systemBuilder,
                                                SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumCachedNetcdf",
                                                               systemSettings.getMaximumCachedNetcdf(),
                                                               systemBuilder::maximumCachedNetcdf,
                                                               0 );
    }

    /**
     * Sets the hard cache limit for NetCDF.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setHardNetcdfCacheLimit( SystemSettingsBuilder systemBuilder,
                                                 SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.hardNetcdfCacheLimit",
                                                               systemSettings.getHardNetcdfCacheLimit(),
                                                               systemBuilder::hardNetcdfCacheLimit,
                                                               -1 );
    }

    /**
     * Sets the maximum number of web client threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumWebClientThreads( SystemSettingsBuilder systemBuilder,
                                                    SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumWebClientThreads",
                                                               systemSettings.getMaximumWebClientThreads(),
                                                               systemBuilder::maximumWebClientThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of archive threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumArchiveThreads( SystemSettingsBuilder systemBuilder,
                                                  SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumArchiveThreads",
                                                               systemSettings.getMaximumArchiveThreads(),
                                                               systemBuilder::maximumArchiveThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of ingest threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumIngestThreads( SystemSettingsBuilder systemBuilder,
                                                 SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumIngestThreads",
                                                               systemSettings.getMaximumIngestThreads(),
                                                               systemBuilder::maximumIngestThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of pool threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumPoolThreads( SystemSettingsBuilder systemBuilder,
                                               SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumPoolThreads",
                                                               systemSettings.getMaximumPoolThreads(),
                                                               systemBuilder::maximumPoolThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of product threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumProductThreads( SystemSettingsBuilder systemBuilder,
                                                  SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumProductThreads",
                                                               systemSettings.getMaximumProductThreads(),
                                                               systemBuilder::maximumProductThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of metric threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumMetricThreads( SystemSettingsBuilder systemBuilder,
                                                 SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumMetricThreads",
                                                               systemSettings.getMaximumMetricThreads(),
                                                               systemBuilder::maximumMetricThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of slicing threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumSlicingThreads( SystemSettingsBuilder systemBuilder,
                                                  SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumSlicingThreads",
                                                               systemSettings.getMaximumSlicingThreads(),
                                                               systemBuilder::maximumSlicingThreads,
                                                               0 );
    }

    /**
     * Sets the maximum number of reading threads.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setMaximumReadThreads( SystemSettingsBuilder systemBuilder,
                                               SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.maximumReadThreads",
                                                               systemSettings.getMaximumReadThreads(),
                                                               systemBuilder::maximumReadThreads,
                                                               0 );
    }

    /**
     * Sets the feature batch size.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setFeatureBatchSize( SystemSettingsBuilder systemBuilder,
                                             SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.featureBatchSize",
                                                               systemSettings.getFeatureBatchSize(),
                                                               systemBuilder::featureBatchSize,
                                                               0 );
    }

    /**
     * Sets the feature batch threshold.
     * @param systemBuilder the system settings builder to update
     * @param systemSettings the existing system settings for defaults
     */
    private static void setFeatureBatchThreshold( SystemSettingsBuilder systemBuilder,
                                                  SystemSettings systemSettings )
    {
        SettingsFactory.setPropertyWithIntegerGreaterThanThis( "wres.featureBatchThreshold",
                                                               systemSettings.getFeatureBatchThreshold(),
                                                               systemBuilder::featureBatchThreshold,
                                                               0 );
    }

    /**
     * Sets a property whose value us greater than or equal to one.
     * @param propertyName the property NAME
     * @param defaultSetting the default property value
     * @param setter the setter
     * @param greaterThanThis the value above which the setting must fall
     */

    private static void setPropertyWithIntegerGreaterThanThis( String propertyName,
                                                               int defaultSetting,
                                                               IntConsumer setter,
                                                               int greaterThanThis )
    {
        String overrideProperty = System.getProperty( propertyName );

        if ( overrideProperty != null )
        {
            if ( StringUtils.isNumeric( overrideProperty ) )
            {
                int size = Integer.parseInt( overrideProperty );

                if ( size > greaterThanThis )
                {
                    setter.accept( size );
                }
                else
                {
                    LOGGER.warn( "'{}' is not a valid value for {}, which must be an integer "
                                 + "greater than {}. Falling back to {}.",
                                 size,
                                 propertyName,
                                 greaterThanThis,
                                 defaultSetting );
                }
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for {}, which must be an integer "
                             + "greater than or equal to {}. Falling back to {}.",
                             overrideProperty,
                             propertyName,
                             greaterThanThis,
                             defaultSetting );
            }
        }
    }

    /**
     * Applies system property overrides to the database settings.
     * @param builder the builder
     */
    private static void applyDatabaseSystemPropertyOverrides( DatabaseSettings.DatabaseSettingsBuilder builder )
    {
        validateSystemPropertiesRelatedToDatabase();

        String usernameOverride = System.getProperty( "wres.username" );
        if ( usernameOverride != null )
        {
            builder.username( usernameOverride );
        }

        String databaseNameOverride = System.getProperty( "wres.databaseName" );
        if ( databaseNameOverride != null )
        {
            builder.databaseName( databaseNameOverride );
        }

        String urlOverride = System.getProperty( "wres.url" );
        if ( urlOverride != null )
        {
            LOGGER.warn(
                    "Deprecated wres.url property found. Use wres.databaseHost for a hostname or wres.databaseJdbcUrl for a jdbc url." );
            builder.host( urlOverride );
        }

        String hostOverride = System.getProperty( "wres.databaseHost" );

        if ( Objects.nonNull( hostOverride ) )
        {
            builder.host( hostOverride );
        }

        String portOverride = System.getProperty( "wres.databasePort" );

        if ( Objects.nonNull( portOverride ) )
        {
            int portNumber = Integer.parseInt( portOverride );
            builder.port( portNumber );
        }

        String jdbcUrlOverride = System.getProperty( "wres.databaseJdbcUrl" );
        if ( Objects.nonNull( jdbcUrlOverride ) )
        {
            builder.jdbcUrl( jdbcUrlOverride );
        }

        String useSSLOverride = System.getProperty( "wres.useSSL" );
        if ( useSSLOverride != null )
        {
            builder.useSSL( Boolean.parseBoolean( useSSLOverride ) );
        }

        String validateSSLOverride = System.getProperty( "wres.validateSSL" );
        if ( validateSSLOverride != null )
        {
            builder.validateSSL( Boolean.parseBoolean( validateSSLOverride ) );
        }

        String certificateFileToTrustOverride = System.getProperty( "wres.certificateFileToTrust" );
        if ( certificateFileToTrustOverride != null )
        {
            builder.certificateFileToTrust( certificateFileToTrustOverride );
        }

        String timeoutOverride = System.getProperty( "wres.query_timeout" );
        if ( timeoutOverride != null )
        {
            builder.queryTimeout( Integer.parseInt( timeoutOverride ) );
        }

        String connectionTimeoutMsOverride = System.getProperty( "wres.databaseConnectionTimeoutMs" );
        if ( connectionTimeoutMsOverride != null )
        {
            builder.connectionTimeoutMs( Integer.parseInt( connectionTimeoutMsOverride ) );
        }

        String maxPoolSizeOverride = System.getProperty( "wres.maxPoolSize" );
        if ( maxPoolSizeOverride != null )
        {
            builder.maxPoolSize( Integer.parseInt( maxPoolSizeOverride ) );
        }
    }

    /**
     * Throws exception when essential system properties are not set properly.
     */

    private static void validateSystemPropertiesRelatedToDatabase()
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
                                             + zoneProperty
                                             + " instead)." );
        }
    }

    /**
     * If needed, override the database attributes by parsing the jdbc url.
     * To be called after applying System Property overrides. If either the
     * original config or the System Properties has set the jdbc url with a
     * string starting with "jdbc:" and having more than five characters, the
     * jdbc url takes precedence and overrides any specified attributes. For
     * host name and port, only the first host name and port are found.
     * @param builder the builder
     * @param jdbcUrl the JDBC URL
     */

    private static void overrideDatabaseAttributesUsingJdbcUrl( DatabaseSettings.DatabaseSettingsBuilder builder,
                                                                String jdbcUrl )
    {
        if ( Objects.isNull( jdbcUrl )
             || jdbcUrl.isBlank()
             || !jdbcUrl.startsWith( "jdbc:" )
             || jdbcUrl.length() < 6 )
        {
            LOGGER.debug( "Cannot override database system properties with jdbc url {}",
                          jdbcUrl );

            return;
        }

        String jdbcSubstring = jdbcUrl.substring( 5 );
        int firstSlashIndex = jdbcSubstring.indexOf( '/' );
        int secondSlashIndex = jdbcSubstring.indexOf( '/',
                                                      firstSlashIndex + 1 );
        int thirdSlashIndex = jdbcSubstring.indexOf( '/',
                                                     secondSlashIndex + 1 );

        // Set the database type, if possible
        SettingsFactory.setDatabaseType( builder, jdbcSubstring );

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
                              hostName,
                              jdbcUrl );
                builder.host( hostName );

                try
                {
                    int portNumber = Integer.parseInt( portRaw );
                    LOGGER.debug( "Extracted port {} from jdbcUrl {}",
                                  portNumber,
                                  jdbcUrl );
                    builder.port( portNumber );
                }
                catch ( NumberFormatException nfe )
                {
                    LOGGER.warn( "Unable to parse port number from jdbc url {}. Attempt from substring {} failed.",
                                 jdbcUrl,
                                 portRaw );
                }
            }
            else
            {
                // There is no port because colon was not found after host.
                LOGGER.debug( "Extracted host {} from jdbcUrl {} (and no port)",
                              hostAndMaybePort,
                              jdbcUrl );
                builder.host( hostAndMaybePort );
            }

            // Set the database name, if possible
            SettingsFactory.setDatabaseName( builder, jdbcSubstring, thirdSlashIndex );
        }
        else
        {
            LOGGER.warn( "Unable to extract database host, port, or name from jdbc url {}",
                         jdbcUrl );
        }
    }

    /**
     * Attempts to set the database type from the inputs.
     * @param builder the builder
     * @param jdbcUrl the JDBC URL
     */
    private static void setDatabaseType( DatabaseSettings.DatabaseSettingsBuilder builder, String jdbcUrl )
    {
        int secondColonIndex = jdbcUrl.indexOf( ":" );
        if ( secondColonIndex < 0 )
        {
            LOGGER.warn( "Unable to extract database type from jdbc url substring {}",
                         jdbcUrl );
        }
        else
        {
            String type = jdbcUrl.substring( 0, secondColonIndex )
                                 .toUpperCase();
            LOGGER.debug( "Extracted database type {} from jdbc url {}",
                          type,
                          jdbcUrl );
            DatabaseType typeEnum = DatabaseType.valueOf( type );
            builder.databaseType( typeEnum );
        }
    }

    /**
     * Attempts to set the database name from the inputs.
     * @param builder the builder
     * @param jdbcUrl the JDBC URL
     * @param thirdSlashIndex the index of the third slash in the JDBC URL
     */
    private static void setDatabaseName( DatabaseSettings.DatabaseSettingsBuilder builder,
                                         String jdbcUrl,
                                         int thirdSlashIndex )
    {
        String dbName;

        int questionMarkIndex = jdbcUrl.indexOf( '?' );

        // The db name follows the third slash but not including '?'
        if ( questionMarkIndex <= thirdSlashIndex )
        {
            dbName = jdbcUrl.substring( thirdSlashIndex + 1 );
        }
        else
        {
            dbName = jdbcUrl.substring( thirdSlashIndex + 1,
                                        questionMarkIndex );
        }

        if ( !dbName.isBlank() )
        {
            LOGGER.debug( "Extracted database name {} from the jdbc url substring {}",
                          dbName,
                          jdbcUrl );
            builder.databaseName( dbName );
        }
        else
        {
            LOGGER.warn( "Unable to extract database name from jdbc url substring {}",
                         jdbcUrl );
        }
    }

    /**
     * Exception encountered on reading system settings,
     */
    private static class SettingsReadException extends RuntimeException
    {
        /**
         * Constructs a {@link SettingsReadException} with the specified message.
         *
         * @param message the message.
         * @param cause the cause of the exception
         */

        public SettingsReadException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

    /**
     * Do not construct.
     */
    private SettingsFactory()
    {
    }
}
