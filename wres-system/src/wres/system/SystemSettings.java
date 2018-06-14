package wres.system;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.TransformerException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.system.xml.XML;
import wres.system.xml.XMLReader;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.XML;

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
            instance = new SystemSettings( CONFIG_PATH);
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
    private int maximumInserts = 5000;
    private int maximumCopies = 200;
    private int defaultChartWidth = 800;
    private int defaultChartHeight = 600;
	private String remoteNetCDFURL = "http://***REMOVED***dstore.***REMOVED***.***REMOVED***/nwm/";
	private String netcdfStorePath = "systests/data/";

	/**
	 * The Default constructor
	 * 
	 * Creates a new XMLReader and parses the System Configuration document
	 * Looks on the classpath for the default filename
	 * <br/><br/>
	 * Private because only one SystemSettings should exist as it is the global cache
	 * of configured system settings
	 */
    private SystemSettings(String configPath) throws IOException
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
                    case "pool_object_lifespan":
                        this.setPoolObjectLifespan( reader );
                        break;
                    case "maximum_inserts":
                        this.setMaximumInserts( reader );
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
                        ProgressMonitor.setShouldUpdate(Strings.isTrue( XML.getXMLText( reader)));
                        break;
                    case "default_chart_width":
                        this.setDefaultChartWidth( reader );
                        break;
                    case "default_chart_height":
                        this.setDefaultChartHeight( reader );
                        break;
                    case "netcdf_repo_url":
                        this.setRemoteNetCDFURL( reader );
                        break;
                    case "netcdf_store_path":
                        this.setNetcdfStorePath( reader );
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

	private void setMaximumThreadCount(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.maximumThreadCount = Integer.parseInt( value );
        }
    }

    private void setPoolObjectLifespan(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.poolObjectLifespan = Integer.parseInt(value);
        }
    }

    private void setMaximumInserts(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.maximumInserts = Integer.parseInt(value);
        }
    }

    private void setMaximumCopies(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.maximumCopies = Integer.parseInt(value);
        }
    }

    private void setUpdateFrequency(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            ProgressMonitor.setUpdateFrequency(Long.parseLong( value ));
        }
    }

    private void setFetchSize(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.fetchSize = Integer.parseInt(value);
        }
    }

    private void setDefaultChartWidth(XMLStreamReader reader)
            throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.defaultChartWidth = Integer.parseInt(value);
        }
    }

    private void setDefaultChartHeight(XMLStreamReader reader)
        throws XMLStreamException
    {
        String value = XML.getXMLText( reader );
        if (value != null && Strings.isNumeric( value ))
        {
            this.defaultChartHeight = Integer.parseInt(value);
        }
    }

    private void setRemoteNetCDFURL(XMLStreamReader reader)
        throws XMLStreamException
    {
        String url = XML.getXMLText(reader);
        if (Strings.hasValue(url))
        {
            this.remoteNetCDFURL = url;
        }
    }

    private void setNetcdfStorePath(XMLStreamReader reader)
        throws XMLStreamException
    {
        String path = XML.getXMLText( reader );
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
     * @return The URL where NetCDF files may be downloaded from
     */
	public static String getRemoteNetcdfURL()
    {
        return instance.remoteNetCDFURL;
    }

	/**
	 * @return The number of allowable threads
	 */
	public static int maximumThreadCount()
    {
        return instance.maximumThreadCount;
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
	 * @return The maximum number of values that may be inserted into the database at once
	 */
    public static int maximumDatabaseInsertStatements()
    {
        return instance.maximumInserts;
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

	public static String getUserName()
    {
        String name = instance.databaseConfiguration.getUsername();

        // If the user is using the default user name, pull their actual user
        // name, not their database user name
        if (name.equals( "wres_user" ))
        {
            name = System.getProperty( "user.name" );
        }

        return name;
    }

    public static String getRawConfiguration()
            throws IOException, XMLStreamException, TransformerException
    {
        return instance.getRawXML();
    }

    public static Connection getRawDatabaseConnection() throws SQLException
    {
        return instance.databaseConfiguration.getRawConnection();
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
		stringRep += "Maximum number of inserts into the database at any given time:\t";
		stringRep += String.valueOf(maximumInserts);
		stringRep += System.lineSeparator();
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
