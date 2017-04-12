/**
 * 
 */
package data.details;

import java.sql.SQLException;

import util.Database;

/**
 * @author ctubbs
 *
 */
public final class VariableDetails {
	private static final String newline = System.lineSeparator();
	
	private String variable_name = null;
	public Integer measurementunit_id = null;
	private Integer variable_id = null;
	
	public void set_variable_name(String variable_name)
	{
		if (this.variable_name == null || !this.variable_name.equalsIgnoreCase(variable_name))
		{
			this.variable_name = variable_name;
			this.variable_id = null;
		}
	}
	
	public Integer get_variable_id() throws SQLException
	{
		if (variable_id == null)
		{
			save();
		}
		
		return variable_id;
	}
	
	public void save() throws SQLException
	{
		String script = "";
		
		script += "WITH new_variable_id AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Variable(variable_name, variable_type, measurementunit_id)" + newline;
		script += "		SELECT '" + variable_name + "'," + newline;
		script += "			'Double'," + newline;
		script += "			" + measurementunit_id + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Variable" + newline;
		script += "			WHERE variable_name = '" + variable_name + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING variable_id" + newline;
		script += ")" + newline;
		script += "SELECT variable_id" + newline;
		script += "FROM new_variable_id" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT variable_id" + newline;
		script += "FROM wres.Variable" + newline;
		script += "WHERE variable_name = '" + variable_name + "';";
		
		variable_id = Database.get_result(script, "variable_id");
	}
}
