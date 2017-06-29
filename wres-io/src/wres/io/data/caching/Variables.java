package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.config.SystemSettings;
import wres.io.config.specification.FeatureRangeSpecification;
import wres.io.data.details.VariableDetails;
import wres.io.utilities.Database;
import wres.io.utilities.Debug;

/**
 * @author Christopher Tubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public final class Variables extends Cache<VariableDetails, String>
{
	private static final String DELIMITER = ",";
    private static final CopyOnWriteArrayList<String> savingVariables = new CopyOnWriteArrayList<>();

    /**
     *
     * @param variableName
     * @return true if was added, false if already present
     */
    private static boolean addToSavingIfNotPresent(String variableName)
    {
        return savingVariables.addIfAbsent(variableName);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Variables.class);
    /**
     * The global cache of variables whose details may be accessed through static methods
     */
	private static final Variables internalCache = new Variables();
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
	 * @throws Exception 
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

	public static VariableDetails getDetails(String variableName)
    {
        VariableDetails detail = null;

        if (internalCache.details != null)
        {
            try {
                Integer id = internalCache.getID(variableName);
                detail = internalCache.get(id);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return detail;
    }

	public static Integer addVariable(VariableDetails detail, Integer xSize, Integer ySize) throws Exception {

	    Integer variableID = null;
	    VariableDetails preexistingDetails = null;
        try {
            if (internalCache.keyIndex.containsKey(detail.getKey())) {
                preexistingDetails = getDetails(detail.getKey());
                detail.measurementunitId = preexistingDetails.measurementunitId;
                detail.setID(preexistingDetails.getId());
                variableID = detail.getVariableID();
            } else {
                variableID = internalCache.getID(detail.getKey(), detail.measurementunitId);
            }
        }
        catch (Exception error)
        {
            LOGGER.info("An error was encountered when trying to either add or retrieve variable details.");
            if (preexistingDetails == null)
            {
                LOGGER.info("There were no preexisting details found.");
            }
            else
            {
                LOGGER.info("A set of preexisting details were found:");
                LOGGER.info("The ID was: " + String.valueOf(preexistingDetails.getId()));
                LOGGER.info("The unit was" + String.valueOf(preexistingDetails.measurementunitId));
                LOGGER.info(preexistingDetails.toString());
            }
        }
        detail.recentlyAdded = addToSavingIfNotPresent(detail.getKey());

        if (detail.recentlyAdded)
        {
			StringBuilder builder = new StringBuilder();
			builder.append("SELECT NOT EXISTS (").append(newline);
			builder.append("	SELECT 1").append(newline);
			builder.append("	FROM wres.VariablePosition").append(newline);
			builder.append("	WHERE variable_id = ").append(variableID).append(newline);
			builder.append(") AS positions_exist;");

        	boolean needsPositions = Database.getResult(builder.toString(), "positions_exist");

        	if (needsPositions) {/*
            String script = "-- Current Thread: ";
            script += Thread.currentThread().getName() + newline;
            script += "SELECT wres.add_variablepositions(";
            script += String.valueOf(variableID);
            script += ", ";
            script += String.valueOf(xSize);
            script += ", ";
            script += String.valueOf(ySize);
            script += ");";
			*/
				final String tableDefinition = "wres.VariablePosition(variable_id, x_position, y_position)";
				short saveCounter = 0;
				builder = new StringBuilder();
				for (int xIndex = 0; xIndex < xSize; ++xIndex) {
					for (int yIndex = 0; yIndex < ySize; ++yIndex) {
						builder.append(variableID);
						builder.append(DELIMITER);
						builder.append(xIndex);
						builder.append(DELIMITER);
						builder.append(yIndex);
						builder.append(newline);
						saveCounter++;

						if (saveCounter > SystemSettings.getMaximumCopies())
						{
							Database.copy(tableDefinition, builder.toString(), DELIMITER);
							builder = new StringBuilder();
							saveCounter = 0;
						}
					}
				}

				if (saveCounter > 0)
				{
                    Database.copy(tableDefinition, builder.toString(), DELIMITER);
				}
			}
        }
		/*String script = "-- Current Thread: ";
		script += Thread.currentThread().getName() + newline;
		script += "SELECT EXISTS (" + newline;
		script += "     SELECT 1" + newline;
		script += "     FROM wres.VariablePosition" + newline;
		script += "     WHERE variable_id = " + String.valueOf(result) + newline;
		script += ");";

		Boolean positionsExist = Database.getResult(script, "exists");
		if (!positionsExist)
        {
            script = "";

            script += "SELECT variable_id" + newline;
            script += "FROM wres.VariablePosition" + newline;
            script += "WHERE x_position = " + String.valueOf(xSize) + newline;

            if (ySize == null)
            {
                script += "     AND y_position IS NULL" + newline;
            }
            else
            {
                script += "     AND y_position = " + String.valueOf(ySize) + newline;
            }

            script += "LIMIT 1;";

            Integer similarVariableID = Database.getResult(script, "variable_id");

            script = "";

            script += "WITH x_positions AS" + newline;
            script += "(" + newline;
            script += "     SELECT generate_series(0, " + String.valueOf(xSize) + ") AS pos" + newline;
            script += ")," + newline;
            script += "y_positions AS" + newline;
            script += "(" + newline;
            script += "     SELECT generate_series(0, " + String.valueOf(ySize) + ") AS pos" + newline;
            script += ")" + newline;
            script += "INSERT INTO wres.VariablePosition (variable_id, x_position, y_position)" + newline;
            script += "SELECT " + String.valueOf(result) + ", X.pos, Y.pos" + newline;
            script += "FROM x_positions X" + newline;
            script += "CROSS JOIN y_positions Y" + newline;
            script += "WHERE NOT EXISTS (" + newline;
            script += "     SELECT 1" + newline;
            script += "     FROM wres.VariablePosition VP" + newline;
            script += "     WHERE VP.variable_id = " + String.valueOf(result) + newline;
            script += ");";

            Database.execute(script);

            if (similarVariableID != null)
            {
                script = "";
                script += "INSERT INTO wres.FeaturePosition(variableposition_id, feature_id)" + newline;
                script += "SELECT VP.variableposition_id, FP.feature_id" + newline;
                script += "FROM wres.VariablePosition VP" + newline;
                script += "INNER JOIN wres.VariablePosition SVP" + newline;
                script += "     ON SVP.x_position = VP.x_position" + newline;
                script += "         AND SVP.y_position = VP.y_position" + newline;
                script += "INNER JOIN wres.FeaturePosition FP" + newline;
                script += "     ON FP.variableposition_id = SVP.variableposition_id" + newline;
                script += "WHERE VP.variable_id = " + String.valueOf(result) + newline;
                script += "     AND SVP.variable_id = " + String.valueOf(similarVariableID) +  ");";

                Database.execute(script);
            }
        }*/

		return variableID;
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
	    List<Integer> IDs = new ArrayList<>();
	    
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
	    ResultSet results = null;

	    try
	    {
	        connection = Database.getConnection();
	        results = Database.getResults(connection, script);
	        
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
	        if (results != null)
	        {
	            results.close();
	        }

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
	 * @throws Exception 
	 */
	public Integer getID(String variableName, String measurementUnit) throws Exception {
		if (!keyIndex.containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.measurementunitId = MeasurementUnits.getMeasurementUnitID(measurementUnit);
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
			detail.measurementunitId = measurementUnitID;
			addElement(detail);
		}
		return this.keyIndex.get(variableName);
	}

	@Override
	protected int getMaxDetails() {
		return 100;
	}

    @Override
    protected void init()
    {       
        synchronized(keyIndex)
        {
            Connection connection = null;
            Statement variableQuery = null;
            ResultSet variables = null;
            try
            {
                connection = Database.getConnection();
                variableQuery = connection.createStatement();

                String loadScript = "SELECT variable_id, variable_name, measurementunit_id" + System.lineSeparator();
                loadScript += "FROM wres.variable;";

                variables = variableQuery.executeQuery(loadScript);
                VariableDetails detail = null;

                while (variables.next()) {
                    detail = new VariableDetails();
                    detail.setVariableName(variables.getString("variable_name"));
                    detail.measurementunitId = variables.getInt("measurementunit_id");
                    detail.setID(variables.getInt("variable_id"));
                    keyIndex.put(detail.getKey(), detail.getId());

                    if (this.details == null)
                    {
                        this.details = new ConcurrentSkipListMap<>();
                    }

                    this.details.put(detail.getId(), detail);
                }
            }
            catch (SQLException error)
            {
                Debug.error(LOGGER, "An error was encountered when trying to populate the Variable cache.");
                Debug.error(LOGGER, error);
            }
            finally
            {
                if (variables != null)
                {
                    try
                    {
                        variables.close();
                    }
                    catch(SQLException e)
                    {
                        Debug.error(LOGGER, "An error was encountered when trying to close the result set containing Variable information.");
                        Debug.error(LOGGER, e);
                    }
                }

                if (variableQuery != null)
                {
                    try
                    {
                        variableQuery.close();
                    }
                    catch(SQLException e)
                    {
                        Debug.error(LOGGER, "An error was encountered when trying to close the statement that loaded Variable information.");
                        Debug.error(LOGGER, e);
                    }
                }

                if (connection != null)
                {
                    Database.returnConnection(connection);
                }
            }
        }
    }
}
