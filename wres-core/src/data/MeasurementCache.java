/**
 * 
 */
package data;

import data.details.MeasurementDetails;

/**
 * @author ctubbs
 *
 */
public class MeasurementCache extends Cache<MeasurementDetails, String> {

	private static MeasurementCache internalCache = new MeasurementCache();
	
	public static Integer getMeasurementUnitID(MeasurementDetails detail) throws Exception
	{
		return internalCache.getID(detail);
	}
	
	public static Integer getMeasurementUnitID(String unit) throws Exception {
		return internalCache.getUnitID(unit);
	}
	
	public static String getUnit(Integer measurementunitID)
	{
		return internalCache.getKey(measurementunitID);
	}
	
	public Integer getUnitID(String unit) throws Exception
	{
		if (!keyIndex.containsKey(unit))
		{
			MeasurementDetails details = new MeasurementDetails();
			details.set_unit(unit);
			addElement(details);
		}
		
		return keyIndex.get(unit);
	}

	@Override
	protected Integer getMaxDetails() {
		return 100;
	}

}
