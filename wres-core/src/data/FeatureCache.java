/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import data.details.EnsembleDetails;
import data.details.FeatureDetails;
import util.Database;

/**
 * @author ctubbs
 *
 */
public class FeatureCache extends Cache<FeatureDetails, String> {

	private static FeatureCache internalCache = new FeatureCache();
	
	public static Integer getFeatureID(FeatureDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	public static Integer getFeatureID(String lid, String stationName) throws Exception {
		FeatureDetails detail = new FeatureDetails();
		detail.set_lid(lid);
		detail.station_name = stationName;
		return getFeatureID(detail);
	}
	
	public static Integer getVariablePositionID(String lid, String stationName, Integer variableID) throws Exception {
		return internalCache.getVarPosID(lid, stationName, variableID);
	}
	
	public Integer getVarPosID(String lid, String stationName, Integer variableID) throws Exception {
		if (!keyIndex.containsKey(lid))
		{
			FeatureDetails detail = new FeatureDetails();
			detail.set_lid(lid);
			detail.station_name = stationName;
			getID(detail);
		}
		FeatureDetails detail = details.get(keyIndex.get(lid));
		return detail.getVariablePositionID(variableID);
	}
	
	@Override
	protected Integer getMaxDetails() {
		return 1000;
	}

	public synchronized static void initialize() throws SQLException
	{
		Connection connection = Database.getConnection();
		Statement featureQuery = connection.createStatement();
		featureQuery.setFetchSize(100);
		
		String loadScript = "SELECT F.lid, F.feature_id, F.feature_name" + System.lineSeparator();
		loadScript += "FROM wres.Feature F" + System.lineSeparator();
		loadScript += "INNER JOIN wres.FeaturePosition FP" + System.lineSeparator();
		loadScript += "	ON F.feature_id = FP.feature_id;";
		
		ResultSet features = featureQuery.executeQuery(loadScript);
		
		FeatureDetails detail = null;
		
		while (features.next()) {
			detail = new FeatureDetails();
			detail.set_lid(features.getString("lid"));
			detail.station_name = features.getString("feature_name");
			detail.setID(features.getInt("feature_id"));
			
			detail.loadVariablePositionIDs();
			
			internalCache.details.put(detail.getId(), detail);
			internalCache.keyIndex.put(detail.getKey(), detail.getId());
		}
		
		features.close();
		featureQuery.close();
		Database.returnConnection(connection);
	}
}
