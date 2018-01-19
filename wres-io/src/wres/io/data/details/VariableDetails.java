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
	private Integer variable_id = null;
	private String variablePositionPartitionName;
	private static final Object saveLock = new Object();

	/**
	 * Sets the name of the variable. The ID of the variable is invalidated if its name changes
	 * @param variable_name The new name of the variable
	 */
	public void setVariableName(String variable_name)
	{
		if (this.variableName != null && !this.variableName.equalsIgnoreCase(variable_name))
		{
			this.variable_id = null;
		}
        this.variableName = variable_name;
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
	public void save() throws SQLException {

		synchronized (saveLock)
		{
			super.save();
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
	}

	@Override
	public int compareTo(VariableDetails other) {
		Integer id = this.variable_id;
		
		if (id == null)
		{
			id = -1;
		}
		
		return id.compareTo(other.variable_id);
	}

	@Override
	public String getKey() {
		return this.variableName;
	}

	@Override
	public Integer getId() {
		return this.variable_id;
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
	protected String getIDName() {
		return "variable_id";
	}

	@Override
	public void setID(Integer id)
	{
		this.variable_id = id;
	}
}
