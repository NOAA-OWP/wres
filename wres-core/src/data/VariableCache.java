/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import data.details.VariableDetails;
import util.Database;

/**
 * @author Christopher Tubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public final class VariableCache extends Cache<VariableDetails, String> {
	
    /**
     * The global cache of variables whose details may be accessed through static methods
     */
	private static VariableCache internalCache = new VariableCache();
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws Exception Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public static Integer getVariableID(String variableName, String measurementUnit) throws Exception {
		return internalCache.getID(variableName, measurementUnit);
	}
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param detail Detailed specification for a variable
	 * @return The ID of the variable
	 * @throws Exception Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public static Integer getVariableID(VariableDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @param measurementUnitID The ID of the unit of measurement belonging to the variable
	 * @return The ID of the variable
	 * @throws Exception Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public static Integer getVariableID(String variableName, Integer measurementUnitID) throws Exception {
		return internalCache.getID(variableName, measurementUnitID);
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws Exception Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public Integer getID(String variableName, String measurementUnit) throws Exception {
		if (!keyIndex.containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.measurementunit_id = MeasurementCache.getMeasurementUnitID(measurementUnit);
			addElement(detail);
		}

		return this.keyIndex.get(variableName);
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnitID The ID of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws Exception Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public Integer getID(String variableName, Integer measurementUnitID) throws Exception {
		if (!keyIndex.containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.measurementunit_id = measurementUnitID;
			addElement(detail);
		}
		return this.keyIndex.get(variableName);
	}

	@Override
	protected int getMaxDetails() {
		return 100;
	}
	
	/**
	 * Loads all variables into the global cache
	 * @throws SQLException
	 */
	public static void initialize() throws SQLException {
		internalCache.init();
	}

    @Override
    protected void init() throws SQLException
    {       
        Connection connection = Database.getConnection();
        Statement variableQuery = connection.createStatement();
        String loadScript = "SELECT variable_id, variable_name, measurementunit_id" + System.lineSeparator();
        loadScript += "FROM wres.variable;";
        
        ResultSet variables = variableQuery.executeQuery(loadScript);
        VariableDetails detail = null;
        
        while (variables.next()) {
            detail = new VariableDetails();
            detail.setVariableName(variables.getString("variable_name"));
            detail.measurementunit_id = variables.getInt("measurementunit_id");
            detail.setID(variables.getInt("variable_id"));
            
            internalCache.details.put(detail.getId(), detail);
            internalCache.keyIndex.put(detail.getKey(), detail.getId());
        }
        
        variables.close();
        variableQuery.close();
        Database.returnConnection(connection);        
    }
}
