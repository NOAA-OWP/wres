/**
 * 
 */
package data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentSkipListMap;

import data.details.FeatureDetails;
import util.Database;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
public class FeatureCache extends Cache<FeatureDetails, String> {

    /**
     *  Global cache for all Features
     */
	private static FeatureCache internalCache = new FeatureCache();
	
	private FeatureCache() {
	    this.details = new ConcurrentSkipListMap<>();
	}
	
	/**
	 * Returns the ID of a Feature from the global cache based on a full Feature specification
	 * @param detail The full specification for a Feature
	 * @return The ID for the specified feature
	 * @throws Exception Thrown if the ID could not be retrieved from the Database
	 */
	public static Integer getFeatureID(FeatureDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	/**
	 * Returns the ID of a Feature from the global cache based on its LID and name
	 * @param lid The location ID of the Feature
	 * @param stationName The name of the feature
	 * @return The ID of the Feature
	 * @throws Exception Thrown if the ID could not be loaded from the database
	 */
	public static Integer getFeatureID(String lid, String stationName) throws Exception {
		FeatureDetails detail = new FeatureDetails();
		detail.setLID(lid);
		detail.station_name = stationName;
		return getFeatureID(detail);
	}
	
	/**
	 * Returns the ID of the variable position that a Feature in the global cache is tied to in the Database
	 * @param lid The location ID of the feature to look for
	 * @param stationName The name of the Feature to look for
	 * @param variableID The ID of the variable to look for
	 * @return The ID of the found variable position
	 * @throws Exception Thrown if the variable position could not be loaded from the database
	 */
	public static Integer getVariablePositionID(String lid, String stationName, Integer variableID) throws Exception {
		return internalCache.getVarPosID(lid, stationName, variableID);
	}
    
    /**
     * Returns the ID of the variable position from the instanced cache that a Feature is tied to in the Database
     * @param lid The location ID of the feature to look for
     * @param stationName The name of the Feature to look for
     * @param variableID The ID of the variable to look for
     * @return The ID of the found variable position
     * @throws Exception Thrown if the variable position could not be loaded from the database
     */
	public Integer getVarPosID(String lid, String stationName, Integer variableID) throws Exception {
		if (!keyIndex.containsKey(lid))
		{
			FeatureDetails detail = new FeatureDetails();
			detail.setLID(lid);
			detail.station_name = stationName;
			getID(detail);
		}
		FeatureDetails detail = null;
		try
		{
	        detail = details.get(keyIndex.get(lid));
		}
		catch (NullPointerException error)
		{
		    System.err.println();
		    System.err.println("A variable position could not be retrieved with the parameters of:");
		    System.err.println("\tLID: " + lid);
		    System.err.println("\tStation Name: " + stationName);
		    System.err.println("\tVariableID: " + variableID);
            System.err.println();
		    error.printStackTrace();
            System.err.println();
            throw error;
		}
		return detail.getVariablePositionID(variableID);
	}
	
	@Override
	protected int getMaxDetails() {
		return 1000;
	}

	/**
	 * Loads all preexisting Features into the global cache
	 * @throws SQLException Thrown if Features could not be loaded from the database
	 */
	public synchronized static void initialize() throws SQLException
	{
		internalCache.init();
	}
	
	/**
	 * Loads all pre-existing Features into the instanced cache
	 * @throws SQLException Thrown if Features could not be loaded from the database
	 */
	@Override
    public synchronized void init() throws SQLException {
	    synchronized (keyIndex)
	    {
            Connection connection = Database.getConnection();
            Statement featureQuery = connection.createStatement();
            featureQuery.setFetchSize(100);
            
            String loadScript = "SELECT F.lid, F.feature_id, F.feature_name" + System.lineSeparator();
            loadScript += "FROM wres.Feature F" + System.lineSeparator();
            loadScript += "INNER JOIN wres.FeaturePosition FP" + System.lineSeparator();
            loadScript += " ON F.feature_id = FP.feature_id;";
            
            ResultSet features = featureQuery.executeQuery(loadScript);
            
            FeatureDetails detail = null;
            
            while (features.next()) {
                detail = new FeatureDetails();
                detail.setLID(features.getString("lid"));
                detail.station_name = features.getString("feature_name");
                detail.setID(features.getInt("feature_id"));
                
                detail.loadVariablePositionIDs();
                
                this.details.put(detail.getId(), detail);
                this.keyIndex.put(detail.getKey(), detail.getId());
            }
            
            features.close();
            featureQuery.close();
            Database.returnConnection(connection);
	    }
	}
}
