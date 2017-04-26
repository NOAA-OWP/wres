/**
 * Contains Accessors for global system settings
 */
package config;
import java.sql.Connection;
import java.sql.SQLException;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import config.DatabaseConfig;

/**
 * The reader and container for all system settings
 */
public final class SystemConfig extends reading.XMLReader
{

	// The global, static system configuration
    private static final SystemConfig INSTANCE = new SystemConfig();

    private DatabaseConfig database_configuration = null;
    private int maximum_thread_count = 0;
    private int pool_object_lifespan = 30000;
    private int fetch_size = 100;
    private int maximum_inserts = 5000;
    private int maximum_copies = 200;
    private String project_directory = "projects";

	// The static path to the configuration path
    private static final String config_path = "wresconfig.xml";

    private final Logger LOGGER = LoggerFactory.getLogger(SystemConfig.class);

	/**
	 * The Default constructor
	 * 
	 * Creates a new XMLReader and parses the System Configuration document
	 * Looks on the classpath for the default filename
	 */
    public SystemConfig()
    {
        super(config_path, true);
        LOGGER.debug("Created SystemConfig using default constructor");
        parse();
    }

	/**
	 * Creates a System Configuration collection reading from the alternate location
	 * 
	 * @param alternate_config_location An alternate path to the System Configuration
	 */
    public SystemConfig(String alternate_config_location)
    {
        super(alternate_config_location, false);
        LOGGER.debug("Created SystemConfig using String constructor");
        parse();
    }

    public static SystemConfig instance()
    {
        return INSTANCE;
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
					database_configuration = new DatabaseConfig(reader);
				}				
				else if (reader.getLocalName().equalsIgnoreCase("maximum_thread_count"))
				{
					reader.next();
					if (reader.isCharacters())
					{
						int begin_index = reader.getTextStart();
						int end_index = reader.getTextLength();
						value = new String(reader.getTextCharacters(), begin_index, end_index).trim();
						maximum_thread_count = Integer.parseInt(value);
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
						pool_object_lifespan = Integer.parseInt(value);
					}
				}
				else if (reader.getLocalName().equalsIgnoreCase("maximum_inserts"))
				{
					maximum_inserts = Integer.parseInt(tagValue(reader));
				}
				else if (reader.getLocalName().equalsIgnoreCase("maximum_copies"))
				{
					maximum_copies = Integer.parseInt(tagValue(reader));
				}
				else if (reader.getLocalName().equalsIgnoreCase("project_directory"))
				{
					project_directory = tagValue(reader);
				}
			}
		}
		catch (Exception error)
		{
			error.printStackTrace();
		}
	}

	/**
	 * Uses the database configuration to create a connection to the configured database
	 * @return An open connection to the configured database
	 * @throws SQLException An exception is thrown if a connection cannot be created to the database
	 */
	public Connection get_database_connection() throws SQLException
	{
		return this.database_configuration.create_connection();
	}

	/**
	 * Returns the maximum number of allowable threads dictated via the configuration
	 * @return The number of allowable threads
	 */
	public int get_maximum_thread_count()
	{
		return this.maximum_thread_count;
	}

	/**
	 * Returns the amount of time (in milliseconds) that an object may live in an object pool without being used 
	 * @return The maximum life span for an object in an object pool
	 */
	public int get_pool_object_lifespan()
	{
		return this.pool_object_lifespan;
	}

	/**
	 * Returns the maximum number of rows that may be fetched from the database at any given time
	 * @return The maximum number of rows to retrieve
	 */
	public int get_fetch_size()
	{
		return this.fetch_size;
	}

	/**
	 * Returns the maximum number of values that may be inserted into the database at any given time
	 * @return The maximum number of insert values
	 */
	public int get_maximum_inserts()
	{
		return this.maximum_inserts;
	}

	public int get_maximum_copies()
	{
		return this.maximum_copies;
	}

	public String get_project_directory()
	{
		return this.project_directory;
	}

	public String get_database_type()
	{
	    if (this.database_configuration != null)
	    {
	        return this.database_configuration.get_database_type();
	    }
	    else
	    {
	        return null;
	    }
	}

	public ComboPooledDataSource get_connection_pool()
	{
	    if (this.database_configuration != null)
	    {
	        return this.database_configuration.create_datasource();
	    }
	    else
	    {
	        return null;
	    }
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
		string_rep += String.valueOf(maximum_thread_count);
		string_rep += System.lineSeparator();
		string_rep += "Lifespan of pooled objects (in ms):\t";
		string_rep += String.valueOf(pool_object_lifespan);
		string_rep += System.lineSeparator();
		string_rep += "Most amount of rows that can be loaded from the database at once:\t";
		string_rep += String.valueOf(fetch_size);
		string_rep += System.lineSeparator();
		string_rep += "Maximum number of inserts into the database at any given time:\t";
		string_rep += String.valueOf(maximum_inserts);
		string_rep += System.lineSeparator();
		string_rep += "Project XML is stored in:\t";
		string_rep += String.valueOf(project_directory);
		string_rep += System.lineSeparator();
		
		if (database_configuration != null)
		{
			string_rep += System.lineSeparator();
			string_rep += database_configuration.toString();
			string_rep += System.lineSeparator();
		}
		
		return string_rep;
	}
}
