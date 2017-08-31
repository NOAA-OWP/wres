package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails;
import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class DataSources extends Cache<SourceDetails, SourceKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSources.class);
    private static final Object CACHE_LOCK = new Object();

    /**
     * Global Cache of basic source data
     */
	private static DataSources INTERNAL_CACHE = null;

	private static DataSources getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if (INTERNAL_CACHE == null)
            {
                INTERNAL_CACHE = new DataSources();
                INTERNAL_CACHE.init();
            }
            return INTERNAL_CACHE;
        }
    }
	
	/**
	 * Gets the ID of source metadata from the global cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generated
	 * @param lead the lead time
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	public static Integer getSourceID(String path, String outputTime, Integer lead) throws SQLException
    {
		return getCache().getID(path, outputTime, lead);
	}
	
	/**
	 * Gets the ID of source metadata from the global cache based on basic source specifications
	 * @param detail A basic specification for a data source
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	public static Integer getSourceID(SourceDetails detail) throws SQLException
    {
		return getCache().getID(detail);
	}	
	
	/**
	 * Gets the ID of source metadata from the instanced cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generation
	 * @param lead the lead time
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	public Integer getID(String path, String outputTime, Integer lead) throws SQLException
    {
		return this.getID(SourceDetails.createKey(path, outputTime, lead));
	}
	
	@Override
    public Integer getID(SourceKey key) throws SQLException {
	    if (!this.hasID(key))
	    {
	        addElement(new SourceDetails(key));
	    }

	    return super.getID(key);
	}

	public static String getPath(int sourceID)
    {
        String path = null;
        SourceKey key = getCache().getKey(sourceID);
        if (key != null)
        {
            path = key.getSourcePath();
        }
        return path;
    }

	@Override
	protected int getMaxDetails() {
		return 400;
	}

    @Override
    protected synchronized void init()
    {
        // Exit if there are details populated and the keys and details are synced
        if (getKeyIndex().size() > 0) {
            this.clearCache();
        }

        Connection connection = null;
        ResultSet sources = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            String loadScript = "SELECT source_id, path, CAST(output_time AS TEXT) AS output_time" + System.lineSeparator();
            loadScript += "FROM wres.Source" + System.lineSeparator();
            loadScript += "LIMIT " + getMaxDetails();
            
            sources = Database.getResults(connection, loadScript);
            SourceDetails detail;
            
            while (sources.next()) {
                detail = new SourceDetails();
                detail.setOutputTime(sources.getString("output_time"));
                detail.setSourcePath(sources.getString("path"));
                
                this.getKeyIndex().put(detail.getKey(), sources.getInt("source_id"));
            }
        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Source cache.");
            LOGGER.error(Strings.getStackTrace(error));
        }
        finally
        {
            if (sources != null)
            {
                try
                {
                    sources.close();
                }
                catch(SQLException e)
                {
                    LOGGER.error("An error was encountered when trying to close the resultset that contained data source information.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
    }
}
