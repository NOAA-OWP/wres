package wres.io.data.details;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Details about a variable as defined in the Database
 * @author Christopher Tubbs
 */
public final class VariableDetails extends CachedDetail<VariableDetails, String>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( VariableDetails.class );

	public static VariableDetails from (DataProvider data)
	{
	    VariableDetails details = new VariableDetails();
        details.setVariableName(data.getString("variable_name"));
        details.setID( data.getValue("variable_id"));
        return details;
	}

	private String variableName = null;
	private Integer variableID = null;
	private boolean performedInsert;

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
	protected DataScripter getInsertSelect( Database database )
	{
        DataScripter script = new DataScripter( database );
        script.setUseTransaction( true );

		script.retryOnSerializationFailure();
		script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addLine( "INSERT INTO wres.Variable ( variable_name )" );
        script.addTab().addLine( "SELECT ?" );

        script.addArgument( this.variableName );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.Variable" );
        script.addTab( 2 ).addLine( "WHERE variable_name = ?" );

        script.addArgument( this.variableName );

        script.addTab().addLine( ");" );

		return script;
	}

    @Override
    public void save( Database database ) throws SQLException
    {
        LOGGER.trace( "save() started for {}.", this.variableName );
        DataScripter script = this.getInsertSelect( database );
        this.performedInsert = script.execute() > 0;

        LOGGER.trace( "save() performed insert for {}? {}.",
                      this.variableName,
                      this.performedInsert );

        if ( this.performedInsert )
        {
            this.variableID = script.getInsertedIds()
                                    .get( 0 )
                                    .intValue();
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.add( "SELECT " ).addLine( this.getIDName() );
            scriptWithId.addLine( "FROM wres.Variable" );
            scriptWithId.addLine( "WHERE variable_name = ? " );
            scriptWithId.addArgument( this.variableName );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.variableID = data.getInt( this.getIDName() );
            }
        }

        LOGGER.trace( "Did I create Variable ID {}? {}",
                      this.variableID,
                      this.performedInsert );
    }

	@Override
	protected Object getSaveLock()
	{
        // JFB: No need to lock, let the insert race be won/lost at DB.
        return new Object();
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
