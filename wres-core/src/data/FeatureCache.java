/**
 * 
 */
package data;

import data.details.FeatureDetails;

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

}
