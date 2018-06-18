package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.data.details.VariableDetails;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;

/**
 * @author Christopher Tubbs
 *
 * Manages the retrieval of variable information that may be shared across threads
 */
public final class Variables extends Cache<VariableDetails, String>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Variables.class);
    /**
     * The global cache of variables whose details may be accessed through static methods
     */
	private static Variables instance = null;
	private static final Object CACHE_LOCK = new Object();

	private static final Object DETAIL_LOCK = new Object();
	private static final Object KEY_LOCK = new Object();

	@Override
	protected Object getDetailLock()
	{
		return Variables.DETAIL_LOCK;
	}

	@Override
	protected Object getKeyLock()
	{
		return Variables.KEY_LOCK;
	}

	private static Variables getCache()
	{
		synchronized (CACHE_LOCK)
		{
			if ( instance == null)
			{
				instance = new Variables();
				instance.init();
			}
			return instance;
		}
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
	public static boolean isForecastValid(final Integer projectID,
                                          final String projectMember,
                                          final Integer variableID)
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

		ScriptBuilder script = new ScriptBuilder(  );

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
		script.addTab(   3   ).addLine(") AS PS");
		script.addTab(   3   ).addLine("INNER JOIN wres.ForecastSource FS");
		script.addTab(    4    ).addLine("ON FS.source_id = PS.source_id");
		script.addTab(   3   ).addLine("WHERE FS.forecast_id = TS.timeseries_id");
		script.addTab(  2  ).addLine(") AND EXISTS (");
		script.addTab(   3   ).addLine("SELECT 1");
		script.addTab(   3   ).addLine("FROM wres.VariablePosition VP");
		script.addTab(   3   ).addLine("WHERE VP.variable_id = ", variableID);
		script.addTab(    4    ).addLine("AND VP.variableposition_id = TS.variableposition_id");
		script.addTab(  2  ).addLine(")");
		script.addTab().addLine(") AS TS");
		script.addTab().addLine("WHERE EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.ForecastValue FV");
		script.addTab(  2  ).addLine("WHERE FV.timeseries_id = TS.timeseries_id");
		script.addTab().addLine(")");
		script.add(");");

		return script.retrieve( "exists" );
	}

	/**
	 * Checks to see if there are any observed values for a named variable tied
	 * to the project based on its project member
	 * @param projectID The ID of the project to check
	 * @param projectMember The evaluation member of the project ("left", "right", or "baseline")
	 * @param variableID The ID of the variable
	 * @return Whether or not there is any observed data for the variable within the project
	 * @throws SQLException Thrown if a database operation fails
	 */
	public static boolean isObservationValid(final Integer projectID,
											 final String projectMember,
											 final Integer variableID)
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

		ScriptBuilder script = new ScriptBuilder();

		script.addLine("SELECT EXISTS (");
		script.addTab().addLine("SELECT 1");
		script.addTab().addLine("FROM wres.Observation O");
		script.addTab().addLine("WHERE EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.ProjectSource PS");
		script.addTab(  2  ).addLine("WHERE PS.project_id = ", projectID);
		script.addTab(   3   ).addLine("AND PS.member = ", member);
		script.addTab(   3   ).addLine("AND PS.source_id = O.source_id");
		script.addTab().addLine(") AND EXISTS (");
		script.addTab(  2  ).addLine("SELECT 1");
		script.addTab(  2  ).addLine("FROM wres.VariablePosition VP");
		script.addTab(  2  ).addLine("WHERE VP.variable_id = ", variableID);
		script.addTab(   3   ).addLine("AND VP.variableposition_id = O.variableposition_id");
		script.addTab().addLine(")");
		script.add(");");

		return script.retrieve( "exists" );
	}
	
	/**
	 * Returns the ID of a variable from the global cache
	 * @param variableName The short name of the variable
	 * @return The ID of the variable
	 * @throws SQLException if the ID could not be retrieved
	 */
	public static Integer getVariableID(String variableName) throws SQLException {
		return getCache().getID(variableName);
	}

	public static Integer getVariableID( DataSourceConfig dataSourceConfig) throws SQLException
	{
		return Variables.getVariableID(dataSourceConfig.getVariable().getValue());
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
	 * @param variableName The short name of the variable
	 * @param measurementUnit The name of the unit of measurement for the variable
	 * @return The ID of the variable
     * @throws SQLException if the ID could not be added to the cache
	 */
	public Integer getID(String variableName, String measurementUnit) throws SQLException {
		if (!getKeyIndex().containsKey(variableName)) {
			VariableDetails detail = new VariableDetails();
			detail.setVariableName(variableName);
			detail.setMeasurementunitId( MeasurementUnits.getMeasurementUnitID(measurementUnit) );
            try
			{
                addElement(detail);
            }
            catch (SQLException e) {
                String message = "The variable '" + variableName + "' could not be added to the cache.";
                LOGGER.error(message);
                throw new SQLException(message, e);
            }
        }

		return this.getKeyIndex().get(variableName);
	}
	
	/**
	 * Returns the ID of the variable from the instance cache
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

	public static String getName(Integer variableId)
	{
		return getCache().getKey(variableId);
	}

	public static int getMeasurementUnitId(Integer variableId)
	{
		VariableDetails details = getCache().get( variableId );
		return details.getMeasurementunitId();
	}

	public String getKey(Integer variableId)
	{
		String name = null;

		if (this.get(variableId) != null)
		{
			name = this.get(variableId).getKey();
		}

		return name;
	}

	public static VariableDetails getByName(String name) throws SQLException
	{
		VariableDetails details = null;

		if (Variables.getCache().hasID( name ))
		{
			Integer id = Variables.getCache().getID( name );
			details = Variables.getCache().get( id );
		}

		return details;
	}

	@Override
	protected int getMaxDetails() {
		return 100;
	}

    @Override
    protected void init()
    {       
        synchronized(this.getKeyIndex())
        {
            this.initializeDetails();

            try
            {
            	ScriptBuilder script = new ScriptBuilder(  );
            	script.setHighPriority( true );

            	script.addLine("SELECT variable_id, variable_name, measurementunit_id");
            	script.add("FROM wres.Variable;");

            	script.consume( variable -> this.add(VariableDetails.from(variable)) );
            }
            catch ( SQLException sqlException )
            {
				// Failure to pre-populate cache should not affect primary outputs.
                LOGGER.warn( "An error was encountered when trying to populate "
                             + "the Variable cache.", sqlException );
            }
        }
    }
}
