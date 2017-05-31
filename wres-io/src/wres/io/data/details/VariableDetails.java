package wres.io.data.details;

import java.sql.SQLException;

/**
 * Details about a variable as defined in the Database
 * @author Christopher Tubbs
 */
public final class VariableDetails extends CachedDetail<VariableDetails, String>{
	
	private String variable_name = null;
	public Integer measurementunit_id = null;
	private Integer variable_id = null;
	
	/**
	 * Sets the name of the variable. The ID of the variable is invalidated if its name changes
	 * @param variable_name The new name of the variable
	 */
	public void setVariableName(String variable_name)
	{
		if (this.variable_name != null && !this.variable_name.equalsIgnoreCase(variable_name))
		{
			this.variable_id = null;
		}
        this.variable_name = variable_name;
	}
	
	/**
	 * @return The ID of the variable in the database
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public Integer getVariableID() throws SQLException
	{
		if (variable_id == null)
		{
			save();
		}
		
		return variable_id;
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
		return this.variable_name;
	}

	@Override
	public Integer getId() {
		return this.variable_id;
	}

	@Override
	public String getInsertSelectStatement() {
		String script = "";
		
		script += "WITH new_variable_id AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Variable(variable_name, variable_type, measurementunit_id)" + NEWLINE;
		script += "		SELECT '" + variable_name + "'," + NEWLINE;
		script += "			'Double'," + NEWLINE;
		script += "			" + measurementunit_id + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Variable" + NEWLINE;
		script += "			WHERE variable_name = '" + variable_name + "'" + NEWLINE;
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
		script += "WHERE variable_name = '" + variable_name + "';";
		
		return script;
	}

	@Override
	protected String getIDName() {
		return "variable_id";
	}

	@Override
	public void setID(Integer id) {
		this.variable_id = id;
		
	}
}
