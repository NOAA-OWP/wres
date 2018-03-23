package wres.io.data.details;

import java.sql.ResultSet;
import java.sql.SQLException;

import wres.io.utilities.Database;

/**
 * Details about a variable as defined in the Database
 * @author Christopher Tubbs
 */
public final class VariableDetails extends CachedDetail<VariableDetails, String>
{
	/**
	 * Prevents asynchronous saving of identical variables
	 */
	private static final Object VARIABLE_SAVE_LOCK = new Object();

	public static VariableDetails from (ResultSet resultSet) throws SQLException
	{
	    VariableDetails details = new VariableDetails();
        details.setVariableName(resultSet.getString("variable_name"));
        details.setMeasurementunitId( Database.getValue( resultSet, "measurementunit_id" ));
        details.setID(Database.getValue( resultSet,"variable_id"));
        return details;
	}

	private String variableName = null;

    public Integer getMeasurementunitId()
    {
        return measurementunitId;
    }

    public void setMeasurementunitId( Integer measurementunitId )
    {
        this.measurementunitId = measurementunitId;
    }

    private Integer measurementunitId = null;
	private Integer variableID = null;
	private String variablePositionPartitionName;
	private static final Object saveLock = new Object();

	/**
	 * Sets the name of the variable. The ID of the variable is invalidated if its name changes
	 * @param variableName The new name of the variable
	 */
	public void setVariableName(String variableName)
	{
		if (this.variableName != null && !this.variableName.equalsIgnoreCase(variableName))
		{
			this.variableID = null;
		}
        this.variableName = variableName;
	}

	public String getVariablePositionPartitionName()
	{
		if (this.variablePositionPartitionName == null)
		{
			this.variablePositionPartitionName = "partitions.VARIABLEPOSITION_VARIABLE_" + this.getId().toString();
		}
		return this.variablePositionPartitionName;
	}

	@Override
	protected void update( ResultSet databaseResults ) throws SQLException
	{
		super.update( databaseResults );
		String partition = "";
		partition += "CREATE TABLE IF NOT EXISTS ";
		partition += this.getVariablePositionPartitionName();
		partition += " ( " + NEWLINE;
		partition += "	CHECK (variable_id = ";
		partition += this.getId().toString();
		partition += ")" + NEWLINE;
		partition += ") INHERITS (wres.VariablePosition);";

		Database.execute(partition);
	}

	@Override
	public int compareTo(VariableDetails other) {
		Integer id = this.variableID;
		
		if (id == null)
		{
			id = -1;
		}
		
		return id.compareTo(other.variableID);
	}

	@Override
	public String getKey() {
		return this.variableName;
	}

	@Override
	public Integer getId() {
		return this.variableID;
	}

	@Override
	public String getInsertSelectStatement()
    {
		String script = "";
		
		script += "WITH new_variable_id AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Variable(variable_name, variable_type, measurementunit_id)" + NEWLINE;
		script += "		SELECT '" + variableName + "'," + NEWLINE;
		script += "			'Double'," + NEWLINE;
		script += "			" + measurementunitId + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Variable" + NEWLINE;
		script += "			WHERE variable_name = '" + variableName + "'" + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING variable_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT variable_id" + NEWLINE;
		script += "FROM new_variable_id" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT variable_id" + NEWLINE;
		script += "FROM wres.Variable" + NEWLINE;
		script += "WHERE variable_name = '" + variableName + "';";
		
		return script;
	}

	@Override
	protected Object getSaveLock()
	{
		return VariableDetails.VARIABLE_SAVE_LOCK;
	}

	@Override
	protected String getIDName() {
		return "variable_id";
	}

	@Override
	public void setID(Integer id)
	{
		this.variableID = id;
	}
}
