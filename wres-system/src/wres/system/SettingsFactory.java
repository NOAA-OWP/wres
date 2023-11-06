package wres.system;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.SystemSettings.SystemSettingsBuilder;

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

    private static void applySystemPropertyOverrides( SystemSettingsBuilder systemBuilder,
                                                      SystemSettings systemSettings )
    {
        String maxIngestThreads = System.getProperty( "wres.maximumIngestThreads" );
        if ( maxIngestThreads != null )
        {
            if ( StringUtils.isNumeric( maxIngestThreads ) )
            {
                systemBuilder.maximumIngestThreads( systemSettings.getMaximumThreadCount() );

                // For backwards compatibility
                systemBuilder.maximumThreadCount( Integer.parseInt( maxIngestThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumIngestThreads. Falling back to {}.",
                             maxIngestThreads,
                             systemSettings.getMaximumIngestThreads() );
            }
        }

        String useDatabaseString = System.getProperty( "wres.useDatabase" );
        if ( Objects.nonNull( useDatabaseString ) )
        {
            systemBuilder.useDatabase( "true".equalsIgnoreCase( useDatabaseString ) );
        }

        String fetchCount = System.getProperty( "wres.fetchSize" );
        if ( fetchCount != null )
        {
            if ( StringUtils.isNumeric( fetchCount ) )
            {
                systemBuilder.fetchSize( Integer.parseInt( fetchCount ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.fetchSize. Falling back to {}.",
                             fetchCount,
                             systemSettings.getFetchSize() );
            }
        }

        String maxCopies = System.getProperty( "wres.maximumCopies" );
        if ( maxCopies != null )
        {
            if ( StringUtils.isNumeric( maxCopies ) )
            {
                systemBuilder.maximumCopies( Integer.parseInt( maxCopies ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCopies. Falling back to {}.",
                             maxCopies,
                             systemSettings.getMaximumCopies() );
            }
        }

        String netcdfPeriod = System.getProperty( "wres.netcdfCachePeriod" );
        if ( netcdfPeriod != null )
        {
            if ( StringUtils.isNumeric( netcdfPeriod ) )
            {
                systemBuilder.netcdfCachePeriod( Integer.parseInt( netcdfPeriod ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.netcdfCachePeriod. Falling back to {}.",
                             netcdfPeriod,
                             systemSettings.getNetcdfCachePeriod() );
            }
        }

        String minimumCache = System.getProperty( "wres.minimumCachedNetcdf" );
        if ( minimumCache != null )
        {
            if ( StringUtils.isNumeric( minimumCache ) )
            {
                systemBuilder.minimumCachedNetcdf( Integer.parseInt( minimumCache ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.minimumCachedNetcdf. Falling back to {}.",
                             minimumCache,
                             systemSettings.getMinimumCachedNetcdf() );
            }
        }

        String maximumCache = System.getProperty( "wres.maximumCachedNetcdf" );
        if ( maximumCache != null )
        {
            if ( StringUtils.isNumeric( maximumCache ) )
            {
                systemBuilder.maximumCachedNetcdf( Integer.parseInt( maximumCache ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCachedNetcdf. Falling back to {}.",
                             maximumCache,
                             systemSettings.getMaximumCachedNetcdf() );
            }
        }

        String hardNetcdfLimit = System.getProperty( "wres.hardNetcdfCacheLimit" );
        if ( hardNetcdfLimit != null )
        {
            if ( StringUtils.isNumeric( hardNetcdfLimit ) )
            {
                systemBuilder.hardNetcdfCacheLimit( Integer.parseInt( hardNetcdfLimit ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.hardNetcdfCacheLimit. Falling back to {}.",
                             hardNetcdfLimit,
                             systemSettings.getHardNetcdfCacheLimit() );
            }
        }

        String maxArchiveThreads = System.getProperty( "wres.maximumArchiveThreads" );
        if ( maxArchiveThreads != null )
        {
            if ( StringUtils.isNumeric( maxArchiveThreads ) )
            {
                systemBuilder.maximumArchiveThreads( Integer.parseInt( maxArchiveThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumArchiveThreads. Falling back to {}.",
                             maxArchiveThreads,
                             systemSettings.getMaximumArchiveThreads() );
            }
        }

        String storePath = System.getProperty( "wres.StorePath" );
        if ( storePath != null )
        {
            systemBuilder.netcdfStorePath( storePath );
        }

        String maxPoolThreads = System.getProperty( "wres.maximumPoolThreads" );

        if ( maxPoolThreads != null )
        {
            if ( StringUtils.isNumeric( maxPoolThreads ) )
            {
                systemBuilder.maximumPoolThreads( Integer.parseInt( maxPoolThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumPoolThreads. Falling back to {}.",
                             maxPoolThreads,
                             systemSettings.getMaximumPoolThreads() );
            }
        }


        String maxMetricThreads = System.getProperty( "wres.maximumMetricThreads" );

        if ( maxMetricThreads != null )
        {
            if ( StringUtils.isNumeric( maxMetricThreads ) )
            {
                systemBuilder.maximumMetricThreads( Integer.parseInt( maxMetricThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumMetricThreads. Falling back to {}.",
                             maxMetricThreads,
                             systemSettings.getMaximumMetricThreads() );
            }
        }


        String maxSlicingThreads = System.getProperty( "wres.maximumSlicingThreads" );

        if ( maxSlicingThreads != null )
        {
            if ( StringUtils.isNumeric( maxSlicingThreads ) )
            {
                systemBuilder.maximumSlicingThreads( Integer.parseInt( maxSlicingThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumSlicingThreads. Falling back to {}.",
                             maxSlicingThreads,
                             systemSettings.getMaximumSlicingThreads() );
            }
        }


        String maxProductThreads = System.getProperty( "wres.maximumProductThreads" );

        if ( maxProductThreads != null )
        {
            if ( StringUtils.isNumeric( maxProductThreads ) )
            {
                systemBuilder.maximumProductThreads( Integer.parseInt( maxProductThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumProductThreads. Falling back to {}.",
                             maxProductThreads,
                             systemSettings.getMaximumProductThreads() );
            }
        }

        String maxReadThreads = System.getProperty( "wres.maximumReadThreads" );

        if ( maxReadThreads != null )
        {
            if ( StringUtils.isNumeric( maxReadThreads ) )
            {
                systemBuilder.maximumReadThreads( Integer.parseInt( maxReadThreads ) );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumReadThreads. Falling back to {}.",
                             maxReadThreads,
                             systemSettings.getMaximumReadThreads() );
            }
        }

        String fBatchThreshold = System.getProperty( "wres.featureBatchThreshold" );

        if ( fBatchThreshold != null )
        {
            if ( StringUtils.isNumeric( fBatchThreshold ) )
            {
                int threshold = Integer.parseInt( fBatchThreshold );
                if ( threshold >= 1 )
                {
                    systemBuilder.featureBatchThreshold( threshold );
                }
                else
                {
                    LOGGER.warn( "'{}' is not a valid value for wres.featureBatchThreshold, which must be an integer "
                                 + "greater than or equal to 0. Falling back to {}.",
                                 threshold,
                                 systemSettings.getFeatureBatchThreshold() );
                }
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.featureBatchThreshold, which must be an integer " +
                             "greater than or equal to 0. Falling back to {}.",
                             fBatchThreshold,
                             systemSettings.getFeatureBatchThreshold() );
            }
        }

        String fBatchSize = System.getProperty( "wres.featureBatchSize" );

        if ( fBatchSize != null )
        {
            if ( StringUtils.isNumeric( fBatchSize ) )
            {
                int size = Integer.parseInt( fBatchSize );

                if ( size >= 1 )
                {
                    systemBuilder.featureBatchSize( size );
                }
                else
                {
                    LOGGER.warn( "'{}' is not a valid value for wres.featureBatchSize, which must be an integer "
                                 + "greater than or equal to 1. Falling back to {}.",
                                 size,
                                 systemSettings.getFeatureBatchSize() );
                }
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.featureBatchSize, which must be an integer " +
                             "greater than or equal to 1. Falling back to {}.",
                             fBatchSize,
                             systemSettings.getFeatureBatchSize() );
            }
        }
    }

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
     */

    private static void overrideDatabaseAttributesUsingJdbcUrl( DatabaseSettings.DatabaseSettingsBuilder builder,
                                                                String jdbcUrl )
    {
        if ( Objects.nonNull( jdbcUrl )
             && !jdbcUrl.isBlank()
             && jdbcUrl.startsWith( "jdbc:" )
             && jdbcUrl.length() > 5 )
        {
            String jdbcSubstring = jdbcUrl.substring( 5 );
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
                             jdbcUrl );
            }
            else
            {
                String type = jdbcSubstring.substring( 0, secondColonIndex )
                                           .toUpperCase();
                LOGGER.debug( "Extracted database type {} from jdbc url {}",
                              type,
                              jdbcUrl );
                DatabaseType typeEnum = DatabaseType.valueOf( type );
                builder.databaseType( typeEnum );
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

                String dbName;

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
                                  dbName,
                                  jdbcUrl );
                    builder.databaseName( dbName );
                }
                else
                {
                    LOGGER.warn( "Unable to extract database name from jdbc url {}",
                                 jdbcUrl );
                }
            }
            else
            {
                LOGGER.warn( "Unable to extract database host, port, or name from jdbc url {}",
                             jdbcUrl );
            }
        }
        else
        {
            LOGGER.debug( "No way to override database attributes with jdbc url {}",
                          jdbcUrl );
        }
    }

    /**
     * <p>Figure out the password based on system properties and/or wres config
     * where host, port, and database name have already been set. Needs to be
     * called after all other overrides have been applied and/or parsing of
     * the jdbcUrl.
     *
     * <p>If there is no value it returns null which is the default value of password
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
