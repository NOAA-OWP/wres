/**
 * 
 */
package data.details;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.Database;

/**
 * Defines the important details of a feature as stored in the database
 * @author Christopher Tubbs
 */
public final class FeatureDetails extends CachedDetail<FeatureDetails, String>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDetails.class);

	private String lid = null;
	public String station_name = null;
	private Integer feature_id = null;
	
	// A concurrent mapping of the feature to its index for a variable
	private ConcurrentMap<Integer, Integer> variablePositions = new ConcurrentSkipListMap<Integer, Integer>();
	
	/**
	 * Finds the variable position id of the feature for a given variable. A position is
	 * added if there is not one for this pair of variable and feature
	 * @param variableID The ID of the variable to look for
	 * @return The id of the variable position mapping the feature to the variable
	 * @throws SQLException 
	 */
	public Integer getVariablePositionID(Integer variableID) throws SQLException
	{
		if (!variablePositions.containsKey(variableID))
		{			
			String script = "SELECT wres.get_variableposition_id(" + getId() + ", " + variableID + ") AS variableposition_id;";

			LOGGER.trace("getVariablePositionID - script: {}", script);

			Integer dbResult = Database.getResult(script, "variableposition_id");

            LOGGER.trace("getVariablePositionID - dbResult: {}", dbResult);

            if (dbResult == null)
            {
                // TODO: throw an appropriate checked exception instead of RuntimeException
                throw new RuntimeException("Possibly missing data in the wres.variableposition table?");
            }
			variablePositions.put(variableID, dbResult);
		}
		
		return variablePositions.get(variableID);
	}
	
	/**
	 * Sets the LID of the Feature
	 * @param lid The value used to update the current LID with
	 */
	public void setLID(String lid)
	{
		if (this.lid == null || !this.lid.equalsIgnoreCase(lid))
		{
			this.lid = lid;
			this.feature_id = null;
		}
	}
	
	/**
	 * @return The name of this Feature's corresponding station in a format that may be used
	 * to query the database
	 */
	private String stationName()
	{
		String name = null;
		
		if (station_name == null)
		{
			name = "null";
		}
		else
		{
			name = "'" + station_name + "'";
		}
		
		return name;
	}

	@Override
	public int compareTo(FeatureDetails other) {
		Integer id = this.feature_id;
		
		if (id == null)
		{
			id = -1;
		}

		return id.compareTo(other.feature_id);
	}

	@Override
	public String getKey() {
		return this.lid;
	}

	@Override
	public Integer getId() {
		return this.feature_id;
	}

	@Override
	protected String getIDName() {
		return "feature_id";
	}

	@Override
	public void setID(Integer id) {
		this.feature_id = id;		
	}

	@Override
	protected String getInsertSelectStatement() {
		String script = "";
		
		script += "WITH new_feature AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Feature (lid, feature_name)" + newline;
		script += "		SELECT '" + lid + "'," + newline;
		script += "			" + stationName() + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Feature" + newline;
		script += "			WHERE lid = '" + lid + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING feature_id" + newline;
		script += ")" + newline;
		script += "SELECT feature_id" + newline;
		script += "FROM new_feature" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT feature_id" + newline;
		script += "FROM wres.feature" + newline;
		script += "WHERE lid = '" + lid + "'" + newline;
		script += "LIMIT 1;";
		return script;
	}

	/**
	 * Loads all variable positions from the database and stores them in the store of all positions mapped to IDs of variables
	 * @throws SQLException Thrown if the database cannot adequately load values from the database
	 */
	public void loadVariablePositionIDs() throws SQLException {
		Connection connection = Database.getConnection();
		Statement loadQuery = connection.createStatement();
		loadQuery.setFetchSize(100);
		
		String loadScript = "SELECT VP.variable_id, VP.variableposition_id" + System.lineSeparator();
		loadScript += "FROM wres.FeaturePosition FP" + System.lineSeparator();
		loadScript += "INNER JOIN wres.VariablePosition VP" + System.lineSeparator();
		loadScript += "	ON VP.variableposition_id = FP.variableposition_id" + System.lineSeparator();
		loadScript += "WHERE FP.feature_id = " + this.getId();
		
		ResultSet variablePositions = loadQuery.executeQuery(loadScript);
		
		while (variablePositions.next()) {
			this.variablePositions.put(variablePositions.getInt("variable_id"), variablePositions.getInt("variableposition_id"));
		}
		
		variablePositions.close();
		loadQuery.close();
		Database.returnConnection(connection);
	}
}
