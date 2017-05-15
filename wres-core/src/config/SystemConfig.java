package config;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import config.DatabaseConfig;
import util.Utilities;

/**
 * The cache for all configured system settings
 * @author Christopher Tubbs
 */
public final class SystemConfig extends reading.XMLReader
{

	// The global, static system configuration
    private static final SystemConfig INSTANCE = new SystemConfig();

    private DatabaseConfig databaseConfiguration = null;
    private int maximumThreadCount = 0;
    private int poolObjectLifespan = 30000;
    private int fetchSize = 100;
    private int maximumInserts = 5000;
    private int maximumCopies = 200;
    private String projectDirectory = "projects";

	// The static path to the configuration path
    private static final String config_path = "conf/wresconfig.xml";

    private final Logger LOGGER = LoggerFactory.getLogger(SystemConfig.class);

	/**
	 * The Default constructor
	 * 
	 * Creates a new XMLReader and parses the System Configuration document
	 * Looks on the classpath for the default filename
	 * <br/><br/>
	 * Private because only one SystemConfig should exist as it is the global cache
	 * of configured system settings
	 */
    private SystemConfig()
    {
        super(config_path, true);
        LOGGER.debug("Created SystemConfig using default constructor");
        parse();
    }

	@Override
	/**
	 * Parses all system settings and objects from the XML
	 * 
	 * @param reader The reader containing the XML data 
	 */
	protected void parseElement(XMLStreamReader reader)
	{
	    LOGGER.trace("parsing element");

		try
		{
			String value = null;

			if (reader.getEventType() == XMLStreamConstants.START_ELEMENT)
			{
				if (reader.getLocalName().equalsIgnoreCase("database"))
				{
					databaseConfiguration = new DatabaseConfig(reader);
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
					maximumInserts = Integer.parseInt(Utilities.getXMLText(reader));
				}
				else if (reader.getLocalName().equalsIgnoreCase("maximum_copies"))
				{
					maximumCopies = Integer.parseInt(Utilities.getXMLText(reader));
				}
				else if (reader.getLocalName().equalsIgnoreCase("project_directory"))
				{
					projectDirectory = Utilities.getXMLText(reader);
				}
			}
		}
		catch (Exception error)
		{
			error.printStackTrace();
		}
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
	 * @return The directory of the project configuration files
	 */
	public static String getProjectDirectory() {
		return INSTANCE.projectDirectory;
	}

	/**
	 * @return The type of database to build queries for
	 * <br><br>
	 * <b>Acceptable Values are:</b>
	 * <ul>
	 *     <li>PostgreSQL</li>
	 *     <li>MySQL</li>
	 * </ul>
	 */
	public static String getDatabaseType() {
	    return INSTANCE.databaseConfiguration.getDatabaseType();
	}

	/**
	 * @return A new instance of a connection pool that is built for the system wide configuration
	 */
	public static ComboPooledDataSource getConnectionPool() {
        return INSTANCE.databaseConfiguration.createDatasource();
	}

	@Override
	/**
	 * Returns a multiline description of all set values for the configuration
	 */
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
		string_rep += "Project XML is stored in:\t";
		string_rep += String.valueOf(projectDirectory);
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
