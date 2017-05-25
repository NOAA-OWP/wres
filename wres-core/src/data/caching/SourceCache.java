/**
 * 
 */
package data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import collections.TwoTuple;
import data.details.SourceDetails;
import util.Database;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class SourceCache extends Cache<SourceDetails, TwoTuple<String, String>> {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    /**
     * Global Cache of basic source data
     */
	private static SourceCache internalCache = new SourceCache();
	
	/**
	 * Gets the ID of source metadata from the global cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generated
	 * @return The ID of the source in the database
	 * @throws Exception Thrown when interaction with the database failed
	 */
	public static Integer getSourceID(String path, String outputTime) throws Exception {
		return internalCache.getID(path, outputTime);
	}
	
	/**
	 * Gets the ID of source metadata from the global cache based on basic source specifications
	 * @param detail A basic specification for a data source
	 * @return The ID of the source in the database
	 * @throws Exception Thrown when interaction with the database failed
	 */
	public static Integer getSourceID(SourceDetails detail) throws Exception {
		return internalCache.getID(detail);
	}	
	
	/**
	 * Gets the ID of source metadata from the instanced cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generation
	 * @return The ID of the source in the database
	 * @throws Exception Thrown when interaction with the database failed
	 */
	public Integer getID(String path, String outputTime) throws Exception {
		return getID(new TwoTuple<String, String>(path, outputTime));
	}
	
	@Override
    public Integer getID(TwoTuple<String, String> key) throws Exception {
	    if (!this.hasID(key))
	    {
	        addElement(new SourceDetails(key));
	    }

	    Integer ID = null;
	    synchronized(keyIndex)
	    {
	        ID = keyIndex.get(key);
	    }
		return ID;
	}
	
	@Override
	protected int getMaxDetails() {
		return 400;
	}

	/**
	 * Loads the maximum amount of current data into the global cache 
	 * @throws SQLException Thrown when data couldn't be loaded into the database
	 */
	public static void initialize() throws SQLException {
	    internalCache.init();
	}

    @Override
    protected synchronized void init() throws SQLException
    {
        synchronized (keyIndex)
        {
            // Exit if there are details populated and the keys and details are synced
            if (keyIndex.size() > 0) {
                this.clearCache();
            }

            Connection connection = null;
            Statement sourceQuery = null;
            ResultSet sources = null;

            try
            {
                connection = Database.getConnection();
                sourceQuery = connection.createStatement();
                String loadScript = "SELECT source_id, path, CAST(output_time AS TEXT) AS output_time" + System.lineSeparator();
                loadScript += "FROM wres.Source" + System.lineSeparator();
                loadScript += "LIMIT " + getMaxDetails();
                
                sources = sourceQuery.executeQuery(loadScript);
                SourceDetails detail = null;
                
                while (sources.next()) {
                    detail = new SourceDetails();
                    detail.setOutputTime(sources.getString("output_time"));
                    detail.setSourcePath(sources.getString("path"));
                    
                    this.keyIndex.put(detail.getKey(), sources.getInt("source_id"));
                }
            }
            catch (SQLException error)
            {
                LOGGER.error("An error was encountered when trying to populate the Source cache. {}",
                             error);
                throw error;
            }
            finally
            {
                if (sources != null)
                {
                    sources.close();
                }

                if (sourceQuery != null)
                {
                    sourceQuery.close();
                }

                if (connection != null)
                {
                    Database.returnConnection(connection);
                }
            }
        }
    }
}
