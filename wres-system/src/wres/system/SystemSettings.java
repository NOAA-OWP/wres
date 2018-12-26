package wres.system;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.xml.XMLHelper;
import wres.system.xml.XMLReader;
import wres.util.Strings;

/**
 * The cache for all configured system settings
 * @author Christopher Tubbs
 */
public final class SystemSettings extends XMLReader
{
	private static final Logger LOGGER = LoggerFactory.getLogger(SystemSettings.class);

    // The static path to the configuration path
    private static final String CONFIG_PATH = "wresconfig.xml";

    // The global, static system configuration
    private static SystemSettings instance;

	static
	{
		try
        {
            instance = new SystemSettings( URI.create( CONFIG_PATH ) );
        }
		catch (IOException ioe)
		{
			LOGGER.warn("Using default system settings due to problem reading config:", ioe);
            instance = new SystemSettings();
        }
	}

    private DatabaseSettings databaseConfiguration = null;
    private int maximumThreadCount = 10;
    private int poolObjectLifespan = 30000;
    private int fetchSize = 100;
    private int maximumCopies = 200;
    private int defaultChartWidth = 800;
    private int defaultChartHeight = 600;
    private int netcdfCachePeriod = 90;
    private int minimumCachedNetcdf = 100;
    private int maximumCachedNetcdf = 200;
    private int hardNetcdfCacheLimit = 0;
	private String netcdfStorePath = "systests/data/";
	private Integer maximumArchiveThreads = null;

	/**
	 * The Default constructor
	 * 
	 * Creates a new XMLReader and parses the System Configuration document
	 * Looks on the classpath for the default filename
	 * <br/><br/>
	 * Private because only one SystemSettings should exist as it is the global cache
	 * of configured system settings
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
                        ProgressMonitor.setShouldUpdate(
                                Strings.isTrue( XMLHelper.getXMLText( reader))
                        );
                        break;
                    case "default_chart_width":
                        this.setDefaultChartWidth( reader );
                        break;
                    case "default_chart_height":
                        this.setDefaultChartHeight( reader );
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
                    default:
                        LOGGER.debug( "The tag '{}' was skipped because it's "
                                      + "not used in configuration.", tagName );
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

    private void setDefaultChartWidth(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.defaultChartWidth = Integer.parseInt(value);
        }
    }

    private void setDefaultChartHeight(XMLStreamReader reader)
        throws XMLStreamException
    {
        String value = XMLHelper.getXMLText( reader );
        if (value != null && StringUtils.isNumeric(value))
        {
            this.defaultChartHeight = Integer.parseInt(value);
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

    /**
     * @return The path where the system should store NetCDF files internally
     */
	public static String getNetCDFStorePath()
    {
        return instance.netcdfStorePath;
    }

	/**
	 * @return The number of allowable threads
	 */
	public static int maximumThreadCount()
    {
        return instance.maximumThreadCount;
    }

    public static int maximumArchiveThreads()
    {
        if (instance.maximumArchiveThreads == null)
        {
            int threadCount = ((Double)Math.ceil(
                    SystemSettings.maximumThreadCount() / 10F)).intValue();
            return Math.max(threadCount, 2);
        }

        return instance.maximumArchiveThreads;
    }

	/**
	 * @return The maximum life span for an object in an object pool
	 */
	public static int poolObjectLifespan()
    {
        return instance.poolObjectLifespan;
    }

	/**
	 * @return The maximum number of rows to retrieve
	 */
	public static int fetchSize()
    {
        return instance.fetchSize;
    }

	/**
	 * @return The maximum number of values that may be copied into the database at once
	 */
	public static int getMaximumCopies() {
        return instance.maximumCopies;
    }

    /**
     * @return The default to use for chart width
     */
    public static int getDefaultChartWidth()
    {
        return instance.defaultChartWidth;
    }

	/**
	 * @return The default to use for chart height
	 */
	public static int getDefaultChartHeight()
    {
        return instance.defaultChartHeight;
    }

    /**
     * @return the amount of NetcdfDatasets that may be cached before the
     * calling thread is responsible for closing datasets
     */
    public static int getHardNetcdfCacheLimit()
    {
        return instance.hardNetcdfCacheLimit;
    }

    /**
     * @return The amount of seconds a NetcdfDataset cache should wait before
     * looking for cached files to close
     */
    public static int getNetcdfCachePeriod()
    {
        return instance.netcdfCachePeriod;
    }

    /**
     * @return The minimum number of cached NetCDFDatasets to persist
     */
    public static int getMinimumCachedNetcdf()
    {
        return instance.minimumCachedNetcdf;
    }

    /**
     * @return The maximum number of cached NetCDFDatasets to persist before
     * attempting to close files to make room
     */
    public static int getMaximumCachedNetcdf()
    {
        return instance.maximumCachedNetcdf;
    }

	/**
	 * @return A new instance of a connection pool that is built for the system wide configuration
	 */
	public static ComboPooledDataSource getConnectionPool()
    {
        return instance.databaseConfiguration.createDatasource();
    }

	public static ComboPooledDataSource getHighPriorityConnectionPool()
    {
        return instance.databaseConfiguration.createHighPriorityDataSource();
    }

    /**
     * @return Returns the number of seconds that should elapse before a database query should timeout
     */
    public static int getQueryTimeout()
    {
        return SystemSettings.instance.databaseConfiguration.getQueryTimeout();
    }

    public static Connection getRawDatabaseConnection() throws SQLException
    {
        return instance.databaseConfiguration.getRawConnection(null);
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
    }

	@Override
	public String toString()
	{
		String stringRep = "System Configuration:";
		stringRep += System.lineSeparator();
		stringRep += System.lineSeparator();
		stringRep += "Maximum # of Threads:\t";
		stringRep += String.valueOf(maximumThreadCount);
		stringRep += System.lineSeparator();
		stringRep += "Lifespan of pooled objects (in ms):\t";
		stringRep += String.valueOf(poolObjectLifespan);
		stringRep += System.lineSeparator();
		stringRep += "Most amount of rows that can be loaded from the database at once:\t";
		stringRep += String.valueOf(fetchSize);
		stringRep += System.lineSeparator();
        stringRep += "Default chart width:\t";
        stringRep += String.valueOf(defaultChartWidth);
        stringRep += System.lineSeparator();
        stringRep += "Default chart height:\t";
        stringRep += String.valueOf(defaultChartHeight);
        stringRep += System.lineSeparator();

		if (databaseConfiguration != null)
		{
			stringRep += System.lineSeparator();
			stringRep += databaseConfiguration.toString();
			stringRep += System.lineSeparator();
		}
		
		return stringRep;
	}

    @Override
    protected Logger getLogger()
    {
        return LOGGER;
    }
}
