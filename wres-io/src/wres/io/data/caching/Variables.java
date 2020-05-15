package wres.io.data.caching;

import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.data.details.VariableDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * @author Christopher Tubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public class Variables extends Cache<VariableDetails, String>
{
    private static final int MAX_DETAILS = 30;

    private static final Logger LOGGER = LoggerFactory.getLogger(Variables.class);

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Database database;

    public Variables( Database database )
    {
        this.database = database;
        this.initializeDetails();
    }

    @Override
    protected Database getDatabase()
    {
        return this.database;
    }

	@Override
	protected Object getDetailLock()
	{
        return this.detailLock;
	}

	@Override
	protected Object getKeyLock()
	{
        return this.keyLock;
    }

    /**
     * Converts all entries in the data provider into variables and adds them to the cache
     * @param data A DataProvider containing information that may be added to the cache
     */
    private void populate(DataProvider data)
    {
        if (data == null)
        {
            LOGGER.warn("The Variables cache was created with no data.");
            return;
        }

        data.consume( variable -> this.add( VariableDetails.from( variable ) ) );
    }

    /**
     * Retrieves the names of all variables that may be addressed as forecasts for the project
     * @param projectID The id of the project we're interested in
     * @param projectMember The data source member for the data (generally 'right')
     * @return A list of all of the names of variables in forecasts that may be evaluated for the project
     * @throws SQLException Thrown if an error was encountered while communicating with the database
     */
    public List<String> getAvailableVariables( final Integer projectID,
                                               final String projectMember )
            throws SQLException
    {
        String member = projectMember;

        if (!member.startsWith( "'" ))
        {
            member = "'" + member;
        }

        if (!member.endsWith( "'" ))
        {
            member += "'";
        }

        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );

        script.addLine("SELECT variable_name");
        script.addLine("FROM wres.Variable V");
        script.addLine("WHERE EXISTS (");
        script.addTab().addLine("SELECT 1");
        script.addTab().addLine("FROM wres.VariableFeature VF");
        script.addTab().addLine("WHERE V.variable_id = VF.variable_id");
        script.addTab(  2  ).addLine("AND EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.TimeSeries TS");
        script.addTab(   3   ).addLine("WHERE TS.variablefeature_id = VF.variablefeature_id");
        script.addTab(    4    ).addLine("AND EXISTS (");
        script.addTab(     5     ).addLine("SELECT 1");
        script.addTab(     5     ).addLine("FROM wres.ProjectSource PS");
        script.addTab(     5     ).addLine("WHERE PS.project_id = ", projectID);
        script.addTab(      6      ).addLine("AND PS.member = ", member);
        script.addTab(      6      ).addLine("AND TS.source_id = PS.source_id");
        script.addTab(    4    ).addLine(") AND EXISTS (");
        script.addTab(     5     ).addLine("SELECT 1");
        script.addTab(     5     ).addLine("FROM wres.TimeSeriesValue TSV");
        script.addTab(     5     ).addLine("WHERE TSV.timeseries_id = TS.timeseries_id");
        script.addTab(    4    ).addLine(")");
        script.addTab(  2  ).addLine(")");
        script.add(");");

        return script.interpret( resultSet -> resultSet.getString("variable_name") );
    }

	/**
	 * Checks to see if there are any forecasted values for a named variable
	 * tied to the project
	 * @param projectID The ID of the project to check
	 * @param projectMember The evaluation member of the project ("left", "right", or "baseline")
	 * @param variableID The ID of the variable
	 * @return Whether or not there is any forecast data for the variable within the project
	 * @throws SQLException Thrown if a database operation fails
	 */
    public boolean isValid( final Integer projectID,
                            final String projectMember,
                            final Integer variableID )
            throws SQLException
	{
		String member = projectMember;

		if (!member.startsWith( "'" ))
		{
			member = "'" + member;
		}

		if (!member.endsWith( "'" ))
		{
			member += "'";
		}

        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );

		script.addLine("SELECT EXISTS (");
		script.addTab().addLine("SELECT 1");
		script.addTab().addLine("FROM (");
		script.addTab(  2  ).addLine("SELECT TS.timeseries_id");
		script.addTab(  2  ).addLine("FROM wres.TimeSeries TS");
		script.addTab(  2  ).addLine("WHERE EXISTS (");
		script.addTab(   3   ).addLine("SELECT 1");
		script.addTab(   3   ).addLine("FROM (");
		script.addTab(    4    ).addLine("SELECT PS.source_id");
		script.addTab(    4    ).addLine("FROM wres.ProjectSource PS");
		script.addTab(    4    ).addLine("WHERE PS.project_id = ", projectID);
		script.addTab(     5     ).addLine("AND PS.member = ", member);
		script.addTab(     5     ).addLine("AND PS.source_id = TS.source_id");
		script.addTab(   3   ).addLine(") AS PS");
		script.addTab(  2  ).addLine(") AND EXISTS (");
		script.addTab(   3   ).addLine("SELECT 1");
		script.addTab(   3   ).addLine("FROM wres.VariableFeature VF");
		script.addTab(   3   ).addLine("WHERE VF.variable_id = ", variableID);
		script.addTab(    4    ).addLine("AND VF.variablefeature_id = TS.variablefeature_id");
		script.addTab(  2  ).addLine(")");
		script.addTab().addLine(") AS TS");
		script.addTab().addLine("WHERE EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.TimeSeriesValue TSV");
		script.addTab(  2  ).addLine("WHERE TSV.timeseries_id = TS.timeseries_id");
		script.addTab().addLine(")");
		script.add(");");

		return script.retrieve( "exists" );
	}

	/**
	 * Returns the ID of a variable from the cache
	 * @param variableName The short name of the variable
	 * @return The ID of the variable
	 * @throws SQLException if the ID could not be retrieved
	 */
    public Integer getVariableID(String variableName) throws SQLException {
        return this.getID(variableName);
	}

    /**
     * Returns the ID of a variable from the cache
     * @param dataSourceConfig The configuration stating what variable to use for the evaluation
     * @return The ID of the variable
     * @throws SQLException if the ID could not be retrieved
     */
    public Integer getVariableID( DataSourceConfig dataSourceConfig) throws SQLException
	{
        return this.getVariableID(dataSourceConfig.getVariable().getValue());
	}
	
	/**
	 * Returns the ID of the variable from the cache
	 * @param variableName The short name of the variable
	 * @return The ID of the variable
	 * @throws SQLException Thrown if an error was encountered while interacting with the database or storing
	 * the result in the cache
	 */
	public Integer getID(String variableName) throws SQLException
	{
		if (!getKeyIndex().containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			addElement(detail);
		}
		return this.getKeyIndex().get(variableName);
	}

    /**
     * Gets the name of the variable associated with the ID
     * @param variableId The id for the variable of interest
     * @return The name of the variable, like 'streamflow' or 'QINE'
     */
    public String getName(Integer variableId)
	{
        return this.getKey(variableId);
	}

    /**
     * Gets the name of the variable associated with the ID
     * @param variableId The id for the variable of interest
     * @return The name of the variable, like 'streamflow' or 'QINE'
     */
	public String getKey(Integer variableId)
	{
		String name = null;

		if (this.get(variableId) != null)
		{
			name = this.get(variableId).getKey();
		}

		return name;
	}

	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}

    /**
     * Loads all variables into the cache
     */
    private void initialize()
    {
        try
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.setHighPriority( true );

            script.addLine("SELECT variable_id, variable_name");
            script.add("FROM wres.Variable;");

            try (DataProvider data = script.getData())
            {
                this.populate( data );
            }
            
            LOGGER.debug( "Finished populating the Variables details." );
        }
        catch ( SQLException sqlException )
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate "
                         + "the Variable cache.", sqlException );
        }
    }
}
