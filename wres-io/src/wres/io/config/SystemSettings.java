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

	// The global, static system configuration
    private static SystemSettings INSTANCE;

    // The static path to the configuration path
    private static final String CONFIG_PATH = "wresconfig.xml";

	static
	{
		try
		{
			INSTANCE = new SystemSettings(CONFIG_PATH);
		}
		catch (IOException ioe)
		{
			LOGGER.warn("Using default system settings due to problem reading config:", ioe);
			INSTANCE = new SystemSettings();
		}
	}

    private DatabaseSettings databaseConfiguration = null;
    private int maximumThreadCount = 10;
    private int poolObjectLifespan = 30000;
    private int fetchSize = 100;
    private int maximumInserts = 5000;
    private int maximumCopies = 200;
    private Long updateFrequency = new Long(5000);
    private boolean updateProgressMonitor = true;
    private int defaultChartWidth = 800;
    private int defaultChartHeight = 600;
	private String remoteNetCDFURL = "http://***REMOVED***dstore.***REMOVED***.***REMOVED***/nwm/";

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
	{
		try
		{
			String value;

			if (reader.getEventType() == XMLStreamConstants.START_ELEMENT)
			{
				if (reader.getLocalName().equalsIgnoreCase("database"))
				{
					databaseConfiguration = new DatabaseSettings(reader);
				}				
				else if (reader.getLocalName().equalsIgnoreCase("maximum_thread_count"))
				{
					reader.next();
					if (reader.isCharacters())
					{
						int begin_index = reader.getTextStart();
						int end_index = reader.getTextLength();
						value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
						maximumThreadCount = Integer.parseInt(value);
					}
				}
				else if (reader.getLocalName().equalsIgnoreCase("pool_object_lifespan"))
				{
					reader.next();
					if (reader.isCharacters())
					{
						int begin_index = reader.getTextStart();
						int end_index = reader.getTextLength();
						value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
						poolObjectLifespan = Integer.parseInt(value);
					}
				}
				else if (reader.getLocalName().equalsIgnoreCase("maximum_inserts"))
				{
					maximumInserts = Integer.parseInt(XML.getXMLText(reader));
				}
				else if (reader.getLocalName().equalsIgnoreCase("maximum_copies"))
				{
					maximumCopies = Integer.parseInt(XML.getXMLText(reader));
				}
				else if (XML.tagIs(reader, "update_frequency"))
				{
					String frequency = XML.getXMLText(reader);
					if (Strings.isNumeric(frequency))
					{
						this.updateFrequency = Long.parseLong(frequency);
						ProgressMonitor.setUpdateFrequency(this.updateFrequency);
					}
				}
				else if (XML.tagIs(reader, "fetch_size"))
				{
					this.fetchSize = Integer.parseInt(XML.getXMLText(reader));
				}
				else if (XML.tagIs(reader, "update_progress_monitor"))
				{
					this.updateProgressMonitor = Strings.isTrue(XML.getXMLText(reader));
					ProgressMonitor.setShouldUpdate(this.updateProgressMonitor);
				}
                else if (XML.tagIs(reader, "default_chart_width"))
                {
                    this.defaultChartWidth = Integer.parseInt(XML.getXMLText(reader));
                }
                else if (XML.tagIs(reader, "default_chart_height"))
                {
                    this.defaultChartHeight = Integer.parseInt(XML.getXMLText(reader));
                }
                else if (XML.tagIs(reader, "netcdf_repo_url"))
				{
					String URL = XML.getXMLText(reader);
					if (Strings.hasValue(URL))
					{
						this.remoteNetCDFURL = URL;
					}
				}
			}
		}
		catch (Exception error)
		{
			LOGGER.error("Failed to parse system settings:", error);
		}
	}

	public static Long getUpdateFrequency()
    {
        return INSTANCE.updateFrequency;
    }

	public static String getRemoteNetcdfURL()
	{
		return INSTANCE.remoteNetCDFURL;
	}
	/**
	 * @return The number of allowable threads
	 */
	public static int maximumThreadCount()
	{
		return INSTANCE.maximumThreadCount;
	}

	/**
	 * @return The maximum life span for an object in an object pool
	 */
	public static int poolObjectLifespan()
	{
		return INSTANCE.poolObjectLifespan;
	}

	/**
	 * @return The maximum number of rows to retrieve
	 */
	public static int fetchSize()
	{
		return INSTANCE.fetchSize;
	}

	/**
	 * @return The maximum number of values that may be inserted into the database at once
	 */
	public static int maximumDatabaseInsertStatements() {
		return INSTANCE.maximumInserts;
	}

	/**
	 * @return The maximum number of values that may be copied into the database at once
	 */
	public static int getMaximumCopies() {
		return INSTANCE.maximumCopies;
	}

    /**
     * @return The default to use for chart width
     */
    public static int getDefaultChartWidth()
    {
        return INSTANCE.defaultChartWidth;
    }

	/**
	 * @return The default to use for chart height
	 */
	public static int getDefaultChartHeight()
	{
		return INSTANCE.defaultChartHeight;
	}

	/**
	 * @return A new instance of a connection pool that is built for the system wide configuration
	 */
	public static ComboPooledDataSource getConnectionPool()
	{
        return INSTANCE.databaseConfiguration.createDatasource();
	}

	public static ComboPooledDataSource getHighPriorityConnectionPool()
	{
		return INSTANCE.databaseConfiguration.createHighPriorityDataSource();
	}

	public static String getUserName()
	{
		return INSTANCE.databaseConfiguration.getUsername();
	}

	public static String getRawConfiguration() throws FileNotFoundException, XMLStreamException, TransformerException {
        return INSTANCE.getRawXML();
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
