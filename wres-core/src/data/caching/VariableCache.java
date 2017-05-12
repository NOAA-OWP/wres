/**
 * 
 */
package data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import config.specification.FeatureRangeSpecification;
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
	 * @throws SQLException Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public static Integer getVariableID(String variableName, String measurementUnit) throws SQLException {
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
	 * Returns a list of all variable position IDs for a range of IDs for a given variable
	 * @param range The range of all variable positions to obtain
	 * @param variableName The name of the variable to look for
	 * @return A list of all obtained variable position IDs
	 * @throws Exception
	 */
	public static List<Integer> getVariablePositionIDs(FeatureRangeSpecification range, String variableName) throws Exception
	{
	    return internalCache.getVarPosIDs(range, variableName);
	}
	
	public List<Integer> getVarPosIDs(FeatureRangeSpecification range, String variableName) throws Exception
	{
	    List<Integer> IDs = new ArrayList<Integer>();
	    
	    int variableID = this.getID(variableName);

	    String script = "";
	    script += "SELECT variableposition_id" + newline;
	    script += "FROM wres.VariablePosition VP" + newline;
	    script += "WHERE variable_id = " + variableID;
	    
	    if (range.xMinimum() != null)
	    {
	        script += newline;
	        script += "    AND x_position >= " + range.xMinimum();
	    }
	    
	    if (range.xMaximum() != null)
	    {
	        script += newline;
	        script += "    AND x_position <= " + range.xMaximum();
	    }
	    
	    if (range.yMinimum() != null)
	    {
	        script += newline;
            script += "    AND y_position >= " + range.yMinimum();
	    }
        
        if (range.yMaximum() != null)
        {
            script += newline;
            script += "    AND y_position <= " + range.yMaximum();
        }

	    script += ";";
	    
	    Connection connection = null;
	    
	    try
	    {
	        connection = Database.getConnection();
	        ResultSet results = Database.getResults(connection, script);
	        
	        while (results.next())
	        {
	            IDs.add(results.getInt("variableposition_id"));
	        }
	    }
	    catch (Exception error)
	    {
	        System.err.println("The list of possible variable possible IDs for the given range could not be retrieved.");
	        System.err.println();
	        System.err.println(range.toString());
	        System.err.println();
	        throw error;
	    }
	    finally
	    {
	        if (connection != null)
	        {
	            Database.returnConnection(connection);
	        }
	    }
	    
	    return IDs;
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws SQLException Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public Integer getID(String variableName, String measurementUnit) throws SQLException {
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
