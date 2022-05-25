package wres.system;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.marschall.jfr.jdbc.JfrDataSource;

import wres.system.xml.XMLHelper;
import wres.system.xml.XMLReader;
import wres.util.Strings;

/**
 * The cache for all configured system settings
 * @author Christopher Tubbs
 */
public class SystemSettings extends XMLReader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SystemSettings.class);

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
	private Path dataDirectory = Paths.get( System.getProperty( "user.dir" ) );
	private boolean updateProgressMonitor = false;
    private int maximumPoolThreads = 6;
	private int maximumThresholdThreads = 1;
	private int maximumMetricThreads = 1;
	private int maximumProductThreads = 3;
	/** The minimum number of singleton features per evaluation at which feature-batched retrieval is triggered. **/
	private int featureBatchThreshold = 10;
    /** The number of features contained within each feature batch when feature-batched retrieval is conducted. **/
    private int featureBatchSize = 50;

    public static SystemSettings fromDefaultClasspathXmlFile()
    {
        try
        {
            return new SystemSettings( DEFAULT_CONFIG_PATH );
        }
        catch ( IOException ioe )
        {
            throw new IllegalStateException( "Could not read system settings from the classpath at "
                                             + DEFAULT_CONFIG_PATH, ioe );
        }
    }

    public static SystemSettings withDefaults()
    {
        return new SystemSettings();
    }
    
    public static SystemSettings fromUri( URI uri )
    {
        try
        {
            return new SystemSettings( uri );
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "Could not read system settings from " + uri, e );
        }
    }
    
	/**
	 * The Default constructor
	 * 
	 * Creates a new XMLReader and parses the System Configuration document
	 * Looks on the classpath for the default filename
	 * <br/><br/>
	 */
    private SystemSettings( URI configPath ) throws IOException
    {
        super( configPath, null );
        parse();
    }

    /**
     * Fall back on default values when file cannot be found or parsed.
     */
    private SystemSettings()
    {
        super();
		databaseConfiguration = new DatabaseSettings();
    }

	@Override
	protected void parseElement(XMLStreamReader reader)
            throws IOException
	{
		try
		{
			if ( reader.getEventType() == XMLStreamConstants.START_ELEMENT)
			{
			    String tagName = reader.getLocalName().toLowerCase();

			    switch (tagName)
                {
                    case "database":
                        databaseConfiguration = new DatabaseSettings(reader);
                        break;
                    case "maximum_thread_count":
                        this.setMaximumThreadCount( reader );
                        break;
                    case "maximum_archive_threads":
                        this.setMaximumArchiveThreads(reader);
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
                    case "data_directory":
                        this.setDataDirectory( reader );
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
                    case "wresconfig":
                        //Do nothing, but make sure no debug message implying it is skipped is output.
                        break;
                    default:
                        LOGGER.warn( "The configuration option '{}' was {}",
                                     tagName, "skipped because it's not used." );
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
        this.applySystemPropertyOverrides();
    }

	private void setMinimumCachedNetcdf(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.minimumCachedNetcdf = Integer.parseInt( value );
        }
    }

    private void setMaximumCachedNetcdf(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.maximumCachedNetcdf = Integer.parseInt( value );
        }
    }

    private void setNetcdfCachePeriod( XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.netcdfCachePeriod = Integer.parseInt( value );
        }
    }

    private void setHardNetcdfCacheLimit(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.hardNetcdfCacheLimit = Integer.parseInt( value );
        }
    }

	private void setMaximumThreadCount(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.maximumThreadCount = Integer.parseInt( value );
        }
    }

    private void setMaximumArchiveThreads(XMLStreamReader reader) throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric( value ))
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

    private void setPoolObjectLifespan(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.poolObjectLifespan = Integer.parseInt(value);
        }
    }

    private void setMaximumCopies(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.maximumCopies = Integer.parseInt(value);
        }
    }

    private void setUpdateFrequency(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            ProgressMonitor.setUpdateFrequency(Long.parseLong( value ));
        }
    }

    private void setFetchSize(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.fetchSize = Integer.parseInt(value);
        }
    }

    private void setNetcdfStorePath(XMLStreamReader reader)
        throws XMLStreamException
    {
        String path = XMLHelper.getXMLText( reader );
        if ( Strings.hasValue( path ) && Strings.isValidPathFormat( path ))
        {
            this.netcdfStorePath = path;
        }
    }

    private void setDataDirectory( XMLStreamReader reader )
            throws XMLStreamException
    {
        String dir = XMLHelper.getXMLText( reader );

        if ( dir != null && !dir.isEmpty() && Strings.isValidPathFormat( dir ) )
        {
            this.dataDirectory = Paths.get( dir );
        }
    }
    
    private void setUpdateProgressMonitor( XMLStreamReader reader ) throws XMLStreamException
    {
        this.updateProgressMonitor = Strings.isTrue( XMLHelper.getXMLText( reader) );
        ProgressMonitor.setShouldUpdate( this.getUpdateProgressMonitor() );
    }

    private void setFeatureBatchThreshold( XMLStreamReader reader )
            throws XMLStreamException
    {
        String threshold = XMLHelper.getXMLText( reader );

        if ( threshold != null && StringUtils.isNumeric( threshold ) )
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

        if ( size != null && StringUtils.isNumeric( size ) )
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
    
    /**
     * @return The path where the system should store NetCDF files internally
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

    public int maximumArchiveThreads()
    {
        if (this.maximumArchiveThreads == null)
        {
            int threadCount = ((Double)Math.ceil(
                    this.maximumThreadCount() / 10F)).intValue();
            return Math.max(threadCount, 2);
        }

        return this.maximumArchiveThreads;
    }

    public int getMaximumWebClientThreads()
    {
        return this.maximumWebClientThreads;
    }

    public int getMaxiumNwmIngestThreads()
    {
        return this.maximumNwmIngestThreads;
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
    public int getMaximumCopies() {
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
     * @return The (file) source directory prefix to use when loading source
     * files that are not absolute
     */
    public Path getDataDirectory()
    {
        return this.dataDirectory;
    }
    
    /**
     * @return the connection pool size
     */
    public int getMaximumPoolSize()
    {
        return this.databaseConfiguration.getMaxPoolSize();
    }

    /**
     * @return the high priority connection pool size
     */
    public int getMaximumHighPriorityPoolSize()
    {
        return this.databaseConfiguration.getMaxHighPriorityPoolSize();
    }
    
	/**
	 * @return A new instance of a connection pool that is built for the system wide configuration
	 */
    public DataSource getConnectionPool()
    {
        int maxPoolSize = this.databaseConfiguration.getMaxPoolSize();
        long connectionTimeoutMs = this.databaseConfiguration.getConnectionTimeoutMs();
        DataSource inner = this.databaseConfiguration.createDataSource( maxPoolSize, connectionTimeoutMs );
        return new JfrDataSource( inner );  // Monitor JDBC traffic with JFR: #61680
    }

    /**
     * @return a high-priority connection pool
     */
    public DataSource getHighPriorityConnectionPool()
    {
        int maxPoolSize = this.databaseConfiguration.getMaxHighPriorityPoolSize();
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
     * @return Return <code>true</code> if progress monitoring is turned on, <code>false</code> if turned off.
     */
    
    public boolean getUpdateProgressMonitor()
    {
        return this.updateProgressMonitor;
    }
    
    Connection getRawDatabaseConnection() throws SQLException
    {
        return this.databaseConfiguration.getRawConnection();
    }

    public int getMaximumPoolThreads()
    {
        return maximumPoolThreads;
    }
    
    /**
     * @return The minimum number of singleton feature groups within an evaluation when using feature-batched retrieval
     */
    public int getFeatureBatchThreshold() {
        return this.featureBatchThreshold;
    }
    
    /**
     * @return The number of features within each feature-batched retrieval when using feature-batched retrieval
     */
    public int getFeatureBatchSize() {
        return this.featureBatchSize;
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
                         value, this.maximumPoolThreads );
        }
    }

    public int getMaximumThresholdThreads()
    {
        return maximumThresholdThreads;
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
                         value, this.maximumThresholdThreads );
        }
    }

    public int getMaximumMetricThreads()
    {
        return maximumMetricThreads;
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
                         value, this.maximumMetricThreads );
        }
    }

    public int getMaximumProductThreads()
    {
        return maximumProductThreads;
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
                         value, this.maximumProductThreads );
        }
    }

    private void applySystemPropertyOverrides()
    {
        String maxIngestThreads = System.getProperty( "wres.maxIngestThreads" );
        if (maxIngestThreads != null)
        {
            if (StringUtils.isNumeric( maxIngestThreads ))
            {
                this.maximumThreadCount = Integer.parseInt( maxIngestThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maxIngestThreads. Falling back to {}",
                             maxIngestThreads, this.maximumThreadCount );
            }
        }

        String fetchCount = System.getProperty( "wres.fetchSize" );
        if (fetchCount != null)
        {
            if (StringUtils.isNumeric( fetchCount ))
            {
                this.fetchSize = Integer.parseInt( fetchCount );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.fetchSize. Falling back to {}",
                             fetchCount, this.fetchSize );
            }
        }

        String maxCopies = System.getProperty( "wres.maximumCopies" );
        if (maxCopies != null)
        {
            if (StringUtils.isNumeric( maxCopies ))
            {
                this.maximumCopies = Integer.parseInt( maxCopies );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCopies. Falling back to {}",
                             maxCopies, this.maximumCopies );
            }
        }

        String netcdfPeriod = System.getProperty( "wres.netcdfCachePeriod" );
        if (netcdfPeriod != null)
        {
            if (StringUtils.isNumeric( netcdfPeriod ))
            {
                this.netcdfCachePeriod = Integer.parseInt( netcdfPeriod );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.netcdfCachePeriod. Falling back to {}",
                             netcdfPeriod, this.netcdfCachePeriod );
            }
        }

        String minimumCache = System.getProperty( "wres.minimumCachedNetcdf" );
        if (minimumCache != null)
        {
            if (StringUtils.isNumeric( minimumCache ))
            {
                this.minimumCachedNetcdf = Integer.parseInt( minimumCache );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.minimumCachedNetcdf. Falling back to {}",
                             minimumCache, this.minimumCachedNetcdf );
            }
        }

        String maximumCache = System.getProperty( "wres.maximumCachedNetcdf" );
        if (maximumCache != null)
        {
            if (StringUtils.isNumeric( maximumCache ))
            {
                this.maximumCachedNetcdf = Integer.parseInt( maximumCache );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumCachedNetcdf. Falling back to {}",
                             maximumCache, this.maximumCachedNetcdf );
            }
        }

        String hardNetcdfLimit = System.getProperty( "wres.hardNetcdfCacheLimit" );
        if (hardNetcdfLimit != null)
        {
            if (StringUtils.isNumeric( hardNetcdfLimit ))
            {
                this.hardNetcdfCacheLimit = Integer.parseInt( hardNetcdfLimit );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.hardNetcdfCacheLimit. Falling back to {}",
                             hardNetcdfLimit, this.hardNetcdfCacheLimit );
            }
        }

        String maxArchiveThreads = System.getProperty( "wres.maximumArchiveThreads" );
        if (maxArchiveThreads != null)
        {
            if (StringUtils.isNumeric( maxArchiveThreads ))
            {
                this.maximumArchiveThreads = Integer.parseInt( maxArchiveThreads );
            }
            else
            {
                LOGGER.warn( "'{}' is not a valid value for wres.maximumArchiveThreads. Falling back to {}",
                             maxArchiveThreads, this.maximumArchiveThreads );
            }
        }

        String storePath = System.getProperty( "wres.StorePath" );
        if (storePath != null)
        {
            this.netcdfStorePath = storePath;
        }

        String directory = System.getProperty( "wres.dataDirectory" );

        if ( directory != null && !directory.isEmpty() )
        {
             if ( Strings.isValidPathFormat( directory ) )
             {
                 this.dataDirectory = Paths.get( directory );
             }
             else
             {
                 LOGGER.warn( "'{}' is not a valid path for wres.dataDirectory. Falling back to {}",
                              directory, this.dataDirectory );
             }
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
                LOGGER.warn( "'{}' is not a valid value for wres.maximumPoolThreads. Falling back to {}",
                             maxPoolThreads, this.maximumPoolThreads );
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
                LOGGER.warn( "'{}' is not a valid value for wres.maximumMetricThreads. Falling back to {}",
                             maxMetricThreads, this.maximumMetricThreads );
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
                LOGGER.warn( "'{}' is not a valid value for wres.maximumThresholdThreads. Falling back to {}",
                             maxThresholdThreads, this.maximumThresholdThreads );
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
                LOGGER.warn( "'{}' is not a valid value for wres.maximumProductThreads. Falling back to {}",
                             maxProductThreads, this.maximumProductThreads );
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

    @Override
    public String toString()
    {
        return new ToStringBuilder( this )
                                          .append( "databaseConfiguration", this.databaseConfiguration )
                                          .append( "maximumThreadCount", this.maximumThreadCount )
                                          .append( "poolObjectLifespan", this.poolObjectLifespan )
                                          .append( "fetchSize", this.fetchSize )
                                          .append( "maximumCopies", this.maximumCopies )
                                          .append( "netcdfCachePeriod", this.netcdfCachePeriod )
                                          .append( "minimumCachedNetcdf", this.minimumCachedNetcdf )
                                          .append( "maximumCachedNetcdf", this.maximumCachedNetcdf )
                                          .append( "hardNetcdfCacheLimit", this.hardNetcdfCacheLimit )
                                          .append( "netcdfStorePath", this.netcdfStorePath )
                                          .append( "maximumArchiveThreads", this.maximumArchiveThreads )
                                          .append( "maximumWebClientThreads", this.maximumWebClientThreads )
                                          .append( "maximumNwmIngestThreads", this.maximumNwmIngestThreads )
                                          .append( "dataDirectory", this.dataDirectory )
                                          .append( "updateProgressMonitor", this.updateProgressMonitor )
                                          .append( "maximumPoolThreads", this.maximumPoolThreads )
                                          .append( "maximumThresholdThreads", this.maximumThresholdThreads )
                                          .append( "maximumMetricThreads", this.maximumMetricThreads )
                                          .append( "maximumProductThreads", this.maximumProductThreads )
                                          .append( "featureBatchThreshold", this.featureBatchThreshold )
                                          .append( "featureBatchSize", this.featureBatchSize )
                                          .toString();
    }

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }


    /**
     * @return The database type.
     */

    public DatabaseType getDatabaseType()
    {
        return this.databaseConfiguration.getDatabaseType();
    }
}
