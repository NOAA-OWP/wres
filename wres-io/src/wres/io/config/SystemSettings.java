package wres.io.config;

import java.io.FileNotFoundException;
import java.io.IOException;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.TransformerException;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.reading.XMLReader;
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
        super(configPath, true);
        parse();
    }

    /**
     * Fall back on default values when file cannot be found or parsed.
     */
    private SystemSettings()
    {
        super(null, false);
		databaseConfiguration = new DatabaseSettings();
    }

	@Override
	protected void parseElement(XMLStreamReader reader)
            throws IOException
	{
		try
		{
			if (reader.getEventType() == XMLStreamConstants.START_ELEMENT)
			{
			    String value;
			    switch (reader.getLocalName().toLowerCase())
                {
                    case "database":
                        databaseConfiguration = new DatabaseSettings(reader);
                        break;
                    case "maximum_thread_count":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.maximumThreadCount = Integer.parseInt( value );
                        }
                        break;
                    case "pool_object_lifespan":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.poolObjectLifespan = Integer.parseInt(value);
                        }
                        break;
                    case "maximum_inserts":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.maximumInserts = Integer.parseInt(value);
                        }
                        break;
                    case "maximum_copies":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.maximumCopies = Integer.parseInt(value);
                        }
                        break;
                    case "update_frequency":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            ProgressMonitor.setUpdateFrequency(Long.parseLong( value ));
                        }
                        break;
                    case "fetch_size":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.fetchSize = Integer.parseInt(value);
                        }
                        break;
                    case "update_progress_monitor":
                        ProgressMonitor.setShouldUpdate(Strings.isTrue(XML.getXMLText(reader)));
                        break;
                    case "default_chart_width":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.defaultChartWidth = Integer.parseInt(value);
                        }
                        break;
                    case "default_chart_height":
                        value = XML.getXMLText( reader );
                        if (value != null && Strings.isNumeric( value ))
                        {
                            this.defaultChartHeight = Integer.parseInt(value);
                        }
                        break;
                    case "netcdf_repo_url":
                        String URL = XML.getXMLText(reader);
                        if (Strings.hasValue(URL))
                        {
                            this.remoteNetCDFURL = URL;
                        }
                        break;
                    case "netcdf_store_path":
                        String path = XML.getXMLText( reader );
                        if ( Strings.hasValue( path ) && Strings.isValidPathFormat( path ))
                        {
                            this.netcdfStorePath = path;
                        }
                        break;
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
        return instance.databaseConfiguration.getUsername();
    }

    public static String getRawConfiguration()
            throws FileNotFoundException, XMLStreamException, TransformerException
    {
        return instance.getRawXML();
    }

	@Override
	public String toString()
	{
		String string_rep = "System Configuration:";
		string_rep += System.lineSeparator();
		string_rep += System.lineSeparator();
		string_rep += "Maximum # of Threads:\t";
		string_rep += String.valueOf(maximumThreadCount);
		string_rep += System.lineSeparator();
		string_rep += "Lifespan of pooled objects (in ms):\t";
		string_rep += String.valueOf(poolObjectLifespan);
		string_rep += System.lineSeparator();
		string_rep += "Most amount of rows that can be loaded from the database at once:\t";
		string_rep += String.valueOf(fetchSize);
		string_rep += System.lineSeparator();
		string_rep += "Maximum number of inserts into the database at any given time:\t";
		string_rep += String.valueOf(maximumInserts);
		string_rep += System.lineSeparator();
		string_rep += System.lineSeparator();
        string_rep += "Default chart width:\t";
        string_rep += String.valueOf(defaultChartWidth);
        string_rep += System.lineSeparator();
        string_rep += "Default chart height:\t";
        string_rep += String.valueOf(defaultChartHeight);
        string_rep += System.lineSeparator();

		if (databaseConfiguration != null)
		{
			string_rep += System.lineSeparator();
			string_rep += databaseConfiguration.toString();
			string_rep += System.lineSeparator();
		}
		
		return string_rep;
	}
}
