/**
 * 
 */
package data;

import data.details.VariableDetails;

/**
 * @author ctubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public final class VariableCache extends Cache<VariableDetails, String> {
	
	private static VariableCache internalCache = new VariableCache();
	
	public static Integer getVariableID(String variableName, String measurementUnit) throws Exception {
		return internalCache.getVarId(variableName, measurementUnit);
	}
	
	public static Integer getVariableID(VariableDetails detail) throws Exception
	{
		return internalCache.getID(detail);
	}
	
	public Integer getVarId(String variableName, String measurementUnit) throws Exception
	{
		if (!keyIndex.containsKey(variableName))
		{
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.measurementunit_id = MeasurementCache.getMeasurementUnitID(measurementUnit);
			addElement(detail);
		}

		return this.keyIndex.get(variableName);
	}

	@Override
	protected Integer getMaxDetails() {
		return 100;
	}
}
