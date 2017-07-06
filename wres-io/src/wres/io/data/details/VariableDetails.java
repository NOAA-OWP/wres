package wres.io.data.details;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.utilities.Database;

import java.sql.SQLException;

/**
 * Details about a variable as defined in the Database
 * @author Christopher Tubbs
 */
public final class VariableDetails extends CachedDetail<VariableDetails, String>{

	private static final Logger LOGGER = LoggerFactory.getLogger(VariableDetails.class);
	private String variable_name = null;
	public Integer measurementunitId = null;
	private Integer variable_id = null;
	public boolean recentlyAdded = false;
	private Integer maxXIndex;
	private Integer maxYIndex;
	private static Object saveLock = new Object();

	//private final static Object partitionLock = new Object();

	public Integer getMaxXIndex()
	{
		if (this.variable_id == null)
		{
			LOGGER.info("The maximum x position was requested for this variable, but there is no id for it.");
			return null;
		}

		if (this.maxXIndex == null)
		{
			String script = "";
			script += "SELECT MAX(x_position) AS max_index" + NEWLINE;
			script += "FROM wres.VariablePosition" + NEWLINE;
			script += "WHERE variable_id = " + String.valueOf(this.variable_id) + ";";

			try {
				this.maxXIndex = Database.getResult(script, "max_index");
			} catch (SQLException e) {
				String message = "The maximum x position for the variable with the name '";
				message += String.valueOf(this.variable_name);
				message += "' could not be retrieved.";
				LOGGER.info(message);
				e.printStackTrace();
			}
		}

		return this.maxXIndex;
	}

	public Integer getMaxYIndex()
	{
		if (this.variable_id == null)
        {
			LOGGER.info("The maximum y position was requested for this variable, but there is no id for it.");
            return null;
        }

        if (this.maxYIndex == null)
        {
            String script = "";
            script += "SELECT MAX(y_position) AS max_index" + NEWLINE;
            script += "FROM wres.VariablePosition" + NEWLINE;
            script += "WHERE variable_id = " + String.valueOf(this.variable_id) + ";";

            try {
                this.maxYIndex = Database.getResult(script, "max_index");
            } catch (SQLException e) {
                String message = "The maximum x position for the variable with the name '";
                message += String.valueOf(this.variable_name);
                message += "' could not be retrieved.";
                LOGGER.info(message);
                e.printStackTrace();
            }
        }

        return this.maxYIndex;
	}

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
	public void save() throws SQLException {

		synchronized (saveLock)
		{
			super.save();
			String partition = "";
			partition += "CREATE TABLE IF NOT EXISTS partitions.VARIABLEPOSITION_VARIABLE_";
			partition += this.getId().toString();
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
		script += "			" + measurementunitId + NEWLINE;
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
