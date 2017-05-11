/**
 * 
 */
package config.data;

import data.caching.VariableCache;

/**
 * A specification for a variable to perform queries against
 * @author Christopher Tubbs
 */
public class Variable {

	/**
	 * Constructor
	 * @param name The name of the specified variable
	 * @param unit The unit that the specified variable should be measured in
	 */
	public Variable(String name, String unit) {
		this.variableName = name;
		this.unitOfMeasurement = unit;
	}

	/**
	 * @return The name of the specified variable
	 */
	public String name() {
		return this.variableName;
	}
	
	/**
	 * @return The unit that the specified variable is measured in
	 */
	public String getUnit() {
		return this.unitOfMeasurement;
	}
	
	public Integer getVariableID() throws Exception {
	    return VariableCache.getVariableID(variableName, unitOfMeasurement);
	}
	
	private final String variableName;
	private final String unitOfMeasurement;
}
