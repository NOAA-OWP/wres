package wres.io.data.details;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;

/**
 * Details about a variable as defined in the Database
 * @author Christopher Tubbs
 */
public final class VariableDetails extends CachedDetail<VariableDetails, String>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( VariableDetails.class );

	/**
	 * Prevents asynchronous saving of identical variables
	 */
	private static final Object VARIABLE_SAVE_LOCK = new Object();

	public static VariableDetails from (ResultSet resultSet) throws SQLException
	{
	    VariableDetails details = new VariableDetails();
        details.setVariableName(resultSet.getString("variable_name"));
        details.setID(Database.getValue( resultSet,"variable_id"));
        return details;
	}

	private String variableName = null;
	private Integer variableID = null;
	private String variablePositionPartitionName;

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

	public String getVariableFeaturePartitionName()
	{
		if (this.variablePositionPartitionName == null)
		{
			this.variablePositionPartitionName = "partitions.VARIABLEFEATURE_VARIABLE_" + this.getId().toString();
		}
		return this.variablePositionPartitionName;
	}

	@Override
	protected void update( ResultSet databaseResults ) throws SQLException
	{
		super.update( databaseResults );
		String partition = "";
		partition += "CREATE TABLE IF NOT EXISTS ";
		partition += this.getVariableFeaturePartitionName();
		partition += " ( " + NEWLINE;
		partition += "	CHECK (variable_id = ";
		partition += this.getId().toString();
		partition += ")" + NEWLINE;
		partition += ") INHERITS (wres.VariableFeature);" + NEWLINE;
		partition += "ALTER TABLE " + this.getVariableFeaturePartitionName() + " OWNER TO wres;";

		Database.execute(partition);
	}

    @Override
    protected Logger getLogger()
    {
        return VariableDetails.LOGGER;
    }

    @Override
	public String toString()
	{
		return "Variable { " + this.variableName + " }";
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
	protected PreparedStatement getInsertSelectStatement( Connection connection )
			throws SQLException
	{
		List<Object> args = new ArrayList<>(  );
		ScriptBuilder script = new ScriptBuilder(  );

		script.addLine("WITH new_variable_id AS");
		script.addLine("(");
		script.addTab().addLine("INSERT INTO wres.Variable(variable_name)");
		script.addTab().addLine("SELECT ?");

		args.add(this.variableName);

		script.addTab().addLine("WHERE NOT EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.Variable");
		script.addTab(  2  ).addLine("WHERE variable_name = ?");

		args.add(this.variableName);

		script.addTab().addLine(")");
		script.addTab().addLine("RETURNING variable_id");
		script.addLine(")");
		script.addLine("SELECT variable_id");
		script.addLine("FROM new_variable_id");
		script.addLine();
		script.addLine("UNION");
		script.addLine();
		script.addLine("SELECT variable_id");
		script.addLine("FROM wres.Variable");
		script.addLine("WHERE variable_name = ?;");

		args.add( this.variableName );

		return script.getPreparedStatement( connection, args );
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
