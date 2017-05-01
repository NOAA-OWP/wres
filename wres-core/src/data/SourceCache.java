/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import collections.TwoTuple;
import data.details.SourceDetails;
import util.Database;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class SourceCache extends Cache<SourceDetails, TwoTuple<String, String>> {

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
		if (!keyIndex.containsKey(key)) {
			addElement(new SourceDetails(key));
		}
		return this.keyIndex.get(key);
	}
	
	@Override
	protected int getMaxDetails() {
		return 100;
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
        // Exit if there are details populated and the keys and details are synced
        if (details.size() > 0 && keyIndex.size() == details.size()) {
            return;
        }
        
        // Ensure that the cache is clear
        clearCache();
        
        Connection connection = Database.getConnection();
        Statement sourceQuery = connection.createStatement();
        String loadScript = "SELECT source_id, path, CAST(output_time AS TEXT) AS output_time" + System.lineSeparator();
        loadScript += "FROM wres.Source" + System.lineSeparator();
        loadScript += "LIMIT " + getMaxDetails();
        
        ResultSet sources = sourceQuery.executeQuery(loadScript);
        SourceDetails detail = null;
        
        while (sources.next()) {
            detail = new SourceDetails();
            detail.setOutputTime(sources.getString("output_time"));
            detail.setSourcePath(sources.getString("path"));
            detail.setID(sources.getInt("source_id"));
            
            this.details.put(detail.getId(), detail);
            this.keyIndex.put(detail.getKey(), detail.getId());
        }
        
        sources.close();
        sourceQuery.close();
        Database.returnConnection(connection);
        
    }
	
}
