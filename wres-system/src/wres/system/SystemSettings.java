package wres.system;

import java.io.IOException;
import java.net.URI;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Objects;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.marschall.jfr.jdbc.JfrDataSource;

import wres.system.xml.XMLHelper;
import wres.system.xml.XMLReader;

/**
 * The cache for all configured system settings
 * @author Christopher Tubbs
 */
public class SystemSettings extends XMLReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( SystemSettings.class );

    // The static path to the configuration path (on the classpath)
    private static final URI DEFAULT_CONFIG_PATH = URI.create( "wresconfig.xml" );

    private DatabaseSettings databaseConfiguration = null;
    private int maximumThreadCount = 10;
    private int poolObjectLifespan = 30000;
    private int fetchSize = 100;
    private int maximumCopies = 200;
    private int netcdfCachePeriod = 90;
    private int minimumCachedNetcdf = 100;
    private int maximumCachedNetcdf = 200;
    private int hardNetcdfCacheLimit = 0;
    private String netcdfStorePath = "systests/data/";
    private Integer maximumArchiveThreads = null;
    private int maximumWebClientThreads = 3;
    private int maximumNwmIngestThreads = 6;
    private int maximumPoolThreads = 6;
    private int maximumThresholdThreads = 1;
    private int maximumMetricThreads = 1;
    private int maximumProductThreads = 3;
    private int maximumReadThreads = 7;
    private int maximumIngestThreads = 7;

    /** The minimum number of singleton features per evaluation at which feature-batched retrieval is triggered. **/
    private int featureBatchThreshold = 10;
    /** The number of features contained within each feature batch when feature-batched retrieval is conducted. **/
    private int featureBatchSize = 50;

    /**
     * @return a default instance from an XML file discovered on the classpath
     */

    public static SystemSettings fromDefaultClasspathXmlFile()
    {
        try
        {
            return new SystemSettings( DEFAULT_CONFIG_PATH );
        }
        catch ( IOException ioe )
        {
            throw new IllegalStateException( "Could not read system settings from the classpath at "
                                             + DEFAULT_CONFIG_PATH,
                                             ioe );
        }
    }

    /**
     * @return a default instance
     */

    public static SystemSettings withDefaults()
    {
        return new SystemSettings();
    }

    /**
     * @return true if the evaluation is being performed in-memory, false otherwise.
     */

    public boolean isInMemory()
    {
        String useDatabaseString = System.getProperty( "wres.useDatabase" );

        boolean useDatabase = false;

        if ( Objects.nonNull( useDatabaseString ) )
        {
            useDatabase = "true".equalsIgnoreCase( useDatabaseString );
        }

        return !useDatabase;
    }

    /**
     * @return the opposite of {@link #isInMemory()}
     */

    public boolean isInDatabase()
    {
        return !this.isInMemory();
    }

    /**
     * @return The path where the system should store Netcdf files internally
     */
    public String getNetCDFStorePath()
    {
        return this.netcdfStorePath;
    }

    /**
     * @return The number of allowable threads
     */
    public int maximumThreadCount()
    {
        return this.maximumThreadCount;
    }

    /**
     * @return the maximum number of archive threads
     */
    public int maximumArchiveThreads()
    {
        if ( this.maximumArchiveThreads == null )
        {
            int threadCount = ( ( Double ) Math.ceil( this.maximumThreadCount() / 10F ) ).intValue();
            return Math.max( threadCount, 2 );
        }

        return this.maximumArchiveThreads;
    }

    /**
     * @return the maximum number of web client threads
     */
    public int getMaximumWebClientThreads()
    {
        return this.maximumWebClientThreads;
    }

    /**
     * @return The maximum life span for an object in an object pool
     */
    public int poolObjectLifespan()
    {
        return this.poolObjectLifespan;
    }

    /**
     * @return The maximum number of rows to retrieve
     */
    public int fetchSize()
    {
        return this.fetchSize;
    }

    /**
     * @return The maximum number of values that may be copied into the database at once
     */
    public int getMaximumCopies()
    {
        return this.maximumCopies;
    }

    /**
     * @return the amount of NetcdfDatasets that may be cached before the
     * calling thread is responsible for closing datasets
     */
    public int getHardNetcdfCacheLimit()
    {
        return this.hardNetcdfCacheLimit;
    }

    /**
     * @return The amount of seconds a NetcdfDataset cache should wait before
     * looking for cached files to close
     */
    public int getNetcdfCachePeriod()
    {
        return this.netcdfCachePeriod;
    }

    /**
     * @return The minimum number of cached NetCDFDatasets to persist
     */
    public int getMinimumCachedNetcdf()
    {
        return this.minimumCachedNetcdf;
    }

    /**
     * @return The maximum number of cached NetCDFDatasets to persist before
     * attempting to close files to make room
     */
    public int getMaximumCachedNetcdf()
    {
        return this.maximumCachedNetcdf;
    }

    /**
     * @return the connection pool size
     */
    public int getDatabaseMaximumPoolSize()
    {
        return this.databaseConfiguration.getMaxPoolSize();
    }

    /**
     * @return the high priority connection pool size
     */
    public int getDatabaseMaximumHighPriorityPoolSize()
    {
        return this.databaseConfiguration.getMaxHighPriorityPoolSize();
    }

    /**
     * @return the maximum number of ingest threads
     */

    public int getMaximumIngestThreads()
    {
        return this.maximumIngestThreads;
    }

    /**
     * @return the maximum number of read threads
     */

    public int getMaximumReadThreads()
    {
        return this.maximumReadThreads;
    }

    /**
     * @return A new instance of a connection pool that is built for the system wide configuration
     */
    public DataSource getConnectionPool()
    {
        int maxPoolSize = this.databaseConfiguration.getMaxPoolSize();
        LOGGER.info( "Creating a database connection pool with {} connections...", maxPoolSize );
        long connectionTimeoutMs = this.databaseConfiguration.getConnectionTimeoutMs();
        DataSource inner = this.databaseConfiguration.createDataSource( maxPoolSize, connectionTimeoutMs );
        return new JfrDataSource( inner ); // Monitor JDBC traffic with JFR: #61680
    }

    /**
     * @return a high-priority connection pool
     */
    public DataSource getHighPriorityConnectionPool()
    {
        int maxPoolSize = this.databaseConfiguration.getMaxHighPriorityPoolSize();
        LOGGER.info( "Creating a high-priority database connection pool with {} connections...", maxPoolSize );
        long connectionTimeoutMs = this.databaseConfiguration.getConnectionTimeoutMs();
        DataSource inner = this.databaseConfiguration.createDataSource( maxPoolSize, connectionTimeoutMs );
        return new JfrDataSource( inner ); // Monitor JDBC traffic with JFR: #61680
    }

    /**
     * @return Returns the number of seconds that should elapse before a database query should timeout
     */
    public int getQueryTimeout()
    {
        return this.databaseConfiguration.getQueryTimeout();
    }

    /**
     * @return the maximum number of pool threads
     */

    public int getMaximumPoolThreads()
    {
        return maximumPoolThreads;
    }

    /**
     * @return The minimum number of singleton feature groups within an evaluation when using feature-batched retrieval
     */
    public int getFeatureBatchThreshold()
    {
        return this.featureBatchThreshold;
    }

    /**
     * @return The number of features within each feature-batched retrieval when using feature-batched retrieval
     */
    public int getFeatureBatchSize()
    {
        return this.featureBatchSize;
    }

    /**
     * @return the maximum number of threshold threads
     */

    public int getMaximumThresholdThreads()
    {
        return maximumThresholdThreads;
    }

    /**
     * @return the database settings
     */

    public DatabaseSettings getDatabaseSettings()
    {
        return this.databaseConfiguration;
    }

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "databaseConfiguration",
                         this.databaseConfiguration )
                .append( "maximumThreadCount",
                         this.maximumThreadCount )
                .append( "poolObjectLifespan",
                         this.poolObjectLifespan )
                .append( "fetchSize", this.fetchSize )
                .append( "maximumCopies",
                         this.maximumCopies )
                .append( "netcdfCachePeriod",
                         this.netcdfCachePeriod )
                .append( "minimumCachedNetcdf",
                         this.minimumCachedNetcdf )
                .append( "maximumCachedNetcdf",
                         this.maximumCachedNetcdf )
                .append( "hardNetcdfCacheLimit",
                         this.hardNetcdfCacheLimit )
                .append( "netcdfStorePath",
                         this.netcdfStorePath )
                .append( "maximumReadThreads",
                         this.maximumReadThreads )
                .append( "maximumIngestThreads",
                         this.maximumIngestThreads )
                .append( "maximumArchiveThreads",
                         this.maximumArchiveThreads )
                .append( "maximumWebClientThreads",
                         this.maximumWebClientThreads )
                .append( "maximumNwmIngestThreads",
                         this.maximumNwmIngestThreads )
                .append( "maximumPoolThreads",
                         this.maximumPoolThreads )
                .append( "maximumThresholdThreads",
                         this.maximumThresholdThreads )
                .append( "maximumMetricThreads",
                         this.maximumMetricThreads )
                .append( "maximumProductThreads",
                         this.maximumProductThreads )
                .append( "featureBatchThreshold",
                         this.featureBatchThreshold )
                .append( "featureBatchSize",
                         this.featureBatchSize )
                .toString();
    }

    /**
     * @return The database type.
     */

    public DatabaseType getDatabaseType()
    {
        return this.databaseConfiguration.getDatabaseType();
    }

    /**
     * @return the maximum number of product threads
     */

    public int getMaximumProductThreads()
    {
        return maximumProductThreads;
    }

    /**
     * @return the maximum number of metric threads
     */

    public int getMaximumMetricThreads()
    {
        return maximumMetricThreads;
    }

    @Override
    protected void parseElement( XMLStreamReader reader )
            throws IOException
    {
        try
        {
            if ( reader.getEventType() == XMLStreamConstants.START_ELEMENT )
            {
                String tagName = reader.getLocalName().toLowerCase();

                switch ( tagName )
                {
                    case "database":
                        this.createDatabaseSettingsIfNeeded( reader );
                        break;
                    case "maximum_thread_count":
                        this.setMaximumThreadCount( reader );
                        break;
                    case "maximum_archive_threads":
                        this.setMaximumArchiveThreads( reader );
                        break;
                    case "maximum_web_client_threads":
                        this.setMaximumWebClientThreads( reader );
                        break;
                    case "maximum_nwm_ingest_threads":
                        this.setMaximumNwmIngestThreads( reader );
                        break;
                    case "pool_object_lifespan":
                        this.setPoolObjectLifespan( reader );
                        break;
                    case "maximum_copies":
                        this.setMaximumCopies( reader );
                        break;
                    case "update_frequency":
                        this.setUpdateFrequency( reader );
                        break;
                    case "fetch_size":
                        this.setFetchSize( reader );
                        break;
                    case "update_progress_monitor":
                        this.setUpdateProgressMonitor( reader );
                        break;
                    case "netcdf_store_path":
                        this.setNetcdfStorePath( reader );
                        break;
                    case "cached_netcdf_lifespan":
                        this.setNetcdfCachePeriod( reader );
                        break;
                    case "minimum_cached_netcdf":
                        this.setMinimumCachedNetcdf( reader );
                        break;
                    case "maximum_cached_netcdf":
                        this.setMaximumCachedNetcdf( reader );
                        break;
                    case "hard_netcdf_cache_limit":
                        this.setHardNetcdfCacheLimit( reader );
                        break;
                    case "maximum_pool_threads":
                        this.setMaximumPoolThreads( reader );
                        break;
                    case "maximum_threshold_threads":
                        this.setMaximumThresholdThreads( reader );
                        break;
                    case "maximum_metric_threads":
                        this.setMaximumMetricThreads( reader );
                        break;
                    case "maximum_product_threads":
                        this.setMaximumProductThreads( reader );
                        break;
                    case "feature_batch_threshold":
                        this.setFeatureBatchThreshold( reader );
                        break;
                    case "feature_batch_size":
                        this.setFeatureBatchSize( reader );
                        break;
                    case "maximum_read_threads":
                        this.setMaximumReadThreads( reader );
                        break;
                    case "maximum_ingest_threads":
                        this.setMaximumIngestThreads( reader );
                        break;
                    case "wresconfig":
                        //Do nothing, but make sure no debug message implying it is skipped is output.
                        break;
                    default:
                        LOGGER.warn( "The configuration option '{}' was {}",
                                     tagName,
                                     "skipped because it's not used." );
                }
            }
        }
        catch ( XMLStreamException xse )
        {
            String message = "While reading system settings, at line"
                             + reader.getLocation().getLineNumber()
                             + " and column "
                             + reader.getLocation().getColumnNumber()
                             + ", an issue occurred.";
            throw new IOException( message, xse );
        }
    }

    @Override
    protected void completeParsing() throws IOException
    {
        // Check for expected database settings where required
        if ( !this.isInMemory() && Objects.isNull( this.databaseConfiguration ) )
        {
            throw new IllegalArgumentException( "Could not find the expected database configuration. Check the "
                                                + "system configuration for database settings, which should be "
                                                + "contained inside a <database> stanza. If you intended to perform an "
                                                + "in-memory evaluation, then do not ask for a database evaluation via "
                                                + "the WRES system property, \"wres.useDatabase\"." );
        }

        this.applySystemPropertyOverrides();
    }

    /**
     * Creates a new XMLReader and parses the System Configuration document
     * Looks on the classpath for the default filename
     * <br/><br/>
     */
    private SystemSettings( URI configPath ) throws IOException
    {
        super( configPath, null );
        this.parse();
    }

    /**
     * Fall back on default values when file cannot be found or parsed.
     */
    private SystemSettings()
    {
        super();
        this.databaseConfiguration = new DatabaseSettings();
    }

    /**
     * Creates the database settings if needed. They are only needed when {@link #isInMemory()} returns {@code false}.
     * @param reader the stream reader
     * @throws XMLStreamException if the database settings could not be skipped
     */

    private void createDatabaseSettingsIfNeeded( XMLStreamReader reader ) throws XMLStreamException
    {
        // Do not create database settings for in-memory mode as this immediately attempts to connect 
        // and interact with a database that may not exist
        if ( !this.isInMemory() )
        {
            LOGGER.debug( "Using database mode. Database settings will now be read." );
            this.databaseConfiguration = new DatabaseSettings( reader );
        }
        else
        {
            LOGGER.debug( "Using in-memory mode. Database settings will not be read." );

            // Put the reader in the correct position, ignoring all database settings
            // This method is only called if there is database configuration present
            while ( reader.hasNext() )
            {
                if ( reader.isEndElement() && reader.getLocalName()
                                                    .equalsIgnoreCase( "database" ) )
                {
                    break;
                }
                else
                {
                    reader.next();
                }
            }
        }
    }

    private void setMinimumCachedNetcdf( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.minimumCachedNetcdf = Integer.parseInt( value );
        }
    }

    private void setMaximumCachedNetcdf( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumCachedNetcdf = Integer.parseInt( value );
        }
    }

    private void setNetcdfCachePeriod( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.netcdfCachePeriod = Integer.parseInt( value );
        }
    }

    private void setHardNetcdfCacheLimit( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.hardNetcdfCacheLimit = Integer.parseInt( value );
        }
    }

    private void setMaximumThreadCount( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumThreadCount = Integer.parseInt( value );
        }
    }

    private void setMaximumArchiveThreads( XMLStreamReader reader ) throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumArchiveThreads = Integer.parseInt( value );
        }
    }

    private void setMaximumWebClientThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumWebClientThreads = Integer.parseInt( value );
        }
    }

    private void setMaximumReadThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumReadThreads = Integer.parseInt( value );
        }
    }

    private void setMaximumIngestThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumIngestThreads = Integer.parseInt( value );
        }
    }

    private void setMaximumNwmIngestThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumNwmIngestThreads = Integer.parseInt( value );
        }
        else
        {
            LOGGER.warn( "Unable to set maximum_nwm_ingest_threads to non-numeric value '{}'.",
                         value );
        }
    }

    private void setPoolObjectLifespan( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.poolObjectLifespan = Integer.parseInt( value );
        }
    }

    private void setMaximumCopies( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumCopies = Integer.parseInt( value );
        }
    }

    private void setUpdateFrequency( XMLStreamReader reader )
    {
        LOGGER.debug( "Detected an update frequency for progress monitoring, but progress monitoring is disabled." );
    }

    private void setFetchSize( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if ( StringUtils.isNumeric( value ) )
        {
            this.fetchSize = Integer.parseInt( value );
        }
    }

    private void setNetcdfStorePath( XMLStreamReader reader )
            throws XMLStreamException
    {
        String path = XMLHelper.getXMLText( reader );
        if ( SystemSettings.isValidPathFormat( path ) )
        {
            this.netcdfStorePath = path;
        }
    }

    private void setUpdateProgressMonitor( XMLStreamReader reader )
    {
        LOGGER.debug( "Detected an attempt to set a flag for progress monitoring, but progress monitoring is "
                      + "disabled." );
    }

    private void setFeatureBatchThreshold( XMLStreamReader reader )
            throws XMLStreamException
    {
        String threshold = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( threshold ) )
        {
            int thresholdInt = Integer.parseInt( threshold );
            if ( thresholdInt >= 0 )
            {
                this.featureBatchThreshold = thresholdInt;
            }
            else
            {
                LOGGER.warn( "Failed to set the feature_batch_threshold from the input because it was not a valid "
                             + "number: {}. Provide an integer that is greater than or equal to zero.",
                             thresholdInt );
            }
        }
        else
        {
            LOGGER.warn( "Failed to set the feature_batch_threshold from the input because it was not a valid number: "
                         + "{}. Provide a non-null integer that is greater than or equal to zero.",
                         threshold );
        }
    }

    private void setFeatureBatchSize( XMLStreamReader reader )
            throws XMLStreamException
    {
        String size = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( size ) )
        {
            int sizeInt = Integer.parseInt( size );
            if ( sizeInt >= 1 )
            {
                this.featureBatchSize = sizeInt;
            }
            else
            {
                LOGGER.warn( "Failed to set the feature_batch_size from the input because it was not a valid number: "
                             + "{}. Provide an integer that is greater than or equal to one.",
                             sizeInt );
            }
        }
        else
        {
            LOGGER.warn( "Failed to set the feature_batch_size from the input because it was not a valid number: {}. "
                         + "Provide a non-null integer that is greater than or equal to one.",
                         size );
        }
    }

    private void setMaximumPoolThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumPoolThreads = Integer.parseInt( value );
        }
        else
        {
            LOGGER.warn( "'{}' is not a valid value for maximum_pool_threads. Falling back to {}",
                         value,
                         this.maximumPoolThreads );
        }
    }

    private void setMaximumThresholdThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumThresholdThreads = Integer.parseInt( value );
        }
        else
        {
            LOGGER.warn( "'{}' is not a valid value for maximum_threshold_threads. Falling back to {}",
                         value,
                         this.maximumThresholdThreads );
        }
    }

    private void setMaximumMetricThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumMetricThreads = Integer.parseInt( value );
        }
        else
        {
            LOGGER.warn( "'{}' is not a valid value for maximum_metric_threads. Falling back to {}",
                         value,
                         this.maximumMetricThreads );
        }
    }

    private void setMaximumProductThreads( XMLStreamReader reader )
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );

        if ( StringUtils.isNumeric( value ) )
        {
            this.maximumProductThreads = Integer.parseInt( value );
        }
        else
        {
            LOGGER.warn( "'{}' is not a valid value for maximum_product_threads. Falling back to {}",
                         value,
                         this.maximumProductThreads );
        }
    }

    /**
     * @param path the path to check
     * @return true if the path format is valid, otherwise false
     */

    private static boolean isValidPathFormat( final String path )
    {
        boolean isValid = false;

        if ( Objects.isNull( path ) || path.isBlank() )
        {
            return false;
        }

        try
        {
            Paths.get( path );
            isValid = true;
        }
        catch ( InvalidPathException invalid )
        {
            // If it isn't valid, we want to catch this, but not break
            LOGGER.trace( "The path '{}' doesn't exist.", path );
        }

        return isValid;
    }

    private void applySystemPropertyOverrides()
    {
        String maxIngestThreads = System.getProperty( "wres.maximumIngestThreads" );
        if ( maxIngestThreads != null )
        {
            if ( StringUtils.isNumeric( maxIngestThreads ) )
            {
                this.maximumIngestThreads = this.maximumThreadCount;

                // For backwards compatibility
                this.maximumThreadCount = Integer.parseInt( maxIngestThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumIngestThreads. Falling back to {}.",
                             maxIngestThreads,
                             this.maximumIngestThreads );
            }
        }

        String fetchCount = System.getProperty( "wres.fetchSize" );
        if ( fetchCount != null )
        {
            if ( StringUtils.isNumeric( fetchCount ) )
            {
                this.fetchSize = Integer.parseInt( fetchCount );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.fetchSize. Falling back to {}.",
                             fetchCount,
                             this.fetchSize );
            }
        }

        String maxCopies = System.getProperty( "wres.maximumCopies" );
        if ( maxCopies != null )
        {
            if ( StringUtils.isNumeric( maxCopies ) )
            {
                this.maximumCopies = Integer.parseInt( maxCopies );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCopies. Falling back to {}.",
                             maxCopies,
                             this.maximumCopies );
            }
        }

        String netcdfPeriod = System.getProperty( "wres.netcdfCachePeriod" );
        if ( netcdfPeriod != null )
        {
            if ( StringUtils.isNumeric( netcdfPeriod ) )
            {
                this.netcdfCachePeriod = Integer.parseInt( netcdfPeriod );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.netcdfCachePeriod. Falling back to {}.",
                             netcdfPeriod,
                             this.netcdfCachePeriod );
            }
        }

        String minimumCache = System.getProperty( "wres.minimumCachedNetcdf" );
        if ( minimumCache != null )
        {
            if ( StringUtils.isNumeric( minimumCache ) )
            {
                this.minimumCachedNetcdf = Integer.parseInt( minimumCache );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.minimumCachedNetcdf. Falling back to {}.",
                             minimumCache,
                             this.minimumCachedNetcdf );
            }
        }

        String maximumCache = System.getProperty( "wres.maximumCachedNetcdf" );
        if ( maximumCache != null )
        {
            if ( StringUtils.isNumeric( maximumCache ) )
            {
                this.maximumCachedNetcdf = Integer.parseInt( maximumCache );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCachedNetcdf. Falling back to {}.",
                             maximumCache,
                             this.maximumCachedNetcdf );
            }
        }

        String hardNetcdfLimit = System.getProperty( "wres.hardNetcdfCacheLimit" );
        if ( hardNetcdfLimit != null )
        {
            if ( StringUtils.isNumeric( hardNetcdfLimit ) )
            {
                this.hardNetcdfCacheLimit = Integer.parseInt( hardNetcdfLimit );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.hardNetcdfCacheLimit. Falling back to {}.",
                             hardNetcdfLimit,
                             this.hardNetcdfCacheLimit );
            }
        }

        String maxArchiveThreads = System.getProperty( "wres.maximumArchiveThreads" );
        if ( maxArchiveThreads != null )
        {
            if ( StringUtils.isNumeric( maxArchiveThreads ) )
            {
                this.maximumArchiveThreads = Integer.parseInt( maxArchiveThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumArchiveThreads. Falling back to {}.",
                             maxArchiveThreads,
                             this.maximumArchiveThreads );
            }
        }

        String storePath = System.getProperty( "wres.StorePath" );
        if ( storePath != null )
        {
            this.netcdfStorePath = storePath;
        }

        String maxPoolThreads = System.getProperty( "wres.maximumPoolThreads" );

        if ( maxPoolThreads != null )
        {
            if ( StringUtils.isNumeric( maxPoolThreads ) )
            {
                this.maximumPoolThreads = Integer.parseInt( maxPoolThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumPoolThreads. Falling back to {}.",
                             maxPoolThreads,
                             this.maximumPoolThreads );
            }
        }


        String maxMetricThreads = System.getProperty( "wres.maximumMetricThreads" );

        if ( maxMetricThreads != null )
        {
            if ( StringUtils.isNumeric( maxMetricThreads ) )
            {
                this.maximumMetricThreads = Integer.parseInt( maxMetricThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumMetricThreads. Falling back to {}.",
                             maxMetricThreads,
                             this.maximumMetricThreads );
            }
        }


        String maxThresholdThreads = System.getProperty( "wres.maximumThresholdThreads" );

        if ( maxThresholdThreads != null )
        {
            if ( StringUtils.isNumeric( maxThresholdThreads ) )
            {
                this.maximumThresholdThreads = Integer.parseInt( maxThresholdThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumThresholdThreads. Falling back to {}.",
                             maxThresholdThreads,
                             this.maximumThresholdThreads );
            }
        }


        String maxProductThreads = System.getProperty( "wres.maximumProductThreads" );

        if ( maxProductThreads != null )
        {
            if ( StringUtils.isNumeric( maxProductThreads ) )
            {
                this.maximumProductThreads = Integer.parseInt( maxProductThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumProductThreads. Falling back to {}.",
                             maxProductThreads,
                             this.maximumProductThreads );
            }
        }

        String maxReadThreads = System.getProperty( "wres.maximumReadThreads" );

        if ( maxReadThreads != null )
        {
            if ( StringUtils.isNumeric( maxReadThreads ) )
            {
                this.maximumReadThreads = Integer.parseInt( maxReadThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumReadThreads. Falling back to {}.",
                             maxReadThreads,
                             this.maximumReadThreads );
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
                    this.featureBatchThreshold = threshold;
                }
                else
                {
                    LOGGER.warn( "'{}' is not a valid value for wres.featureBatchThreshold, which must be an integer "
                                 + "greater than or equal to 0. Falling back to {}.",
                                 threshold,
                                 this.featureBatchThreshold );
                }
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.featureBatchThreshold, which must be an integer " +
                             "greater than or equal to 0. Falling back to {}.",
                             fBatchThreshold,
                             this.featureBatchThreshold );
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
                    this.featureBatchSize = size;
                }
                else
                {
                    LOGGER.warn( "'{}' is not a valid value for wres.featureBatchSize, which must be an integer "
                                 + "greater than or equal to 1. Falling back to {}.",
                                 size,
                                 this.featureBatchSize );
                }
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.featureBatchSize, which must be an integer " +
                             "greater than or equal to 1. Falling back to {}.",
                             fBatchSize,
                             this.featureBatchSize );
            }
        }
    }
}
