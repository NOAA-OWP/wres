package wres.io.data.caching;

import java.sql.SQLException;
import java.util.List;

import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Helpers for data on Variables used in timeseries data.
 */

public class Variables
{
    private final Database database;

    public Variables( Database database )
    {
        this.database = database;
    }

    private Database getDatabase()
    {
        return this.database;
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
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );

        script.addLine( "SELECT DISTINCT( variable_name )" );
        script.addLine( "FROM wres.TimeSeries TS" );
        script.addLine( "WHERE EXISTS" );
        script.addLine( "(" );
        script.addTab().addLine( "SELECT 1" );
        script.addTab().addLine( "FROM wres.ProjectSource PS" );
        script.addTab().addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectID );
        script.addTab().addLine( "AND PS.member = ( ? )::operating_member" );
        script.addArgument( projectMember );
        script.addTab().addLine( "AND TS.source_id = PS.source_id" );
        script.addLine( ")" );

        return script.interpret( resultSet -> resultSet.getString("variable_name") );
    }

	/**
	 * Checks to see if there are any forecasted values for a named variable
	 * tied to the project
	 * @param projectID The ID of the project to check
	 * @param projectMember The evaluation member of the project ("left", "right", or "baseline")
	 * @param variableName The variable
	 * @return Whether or not there is any forecast data for the variable within the project
	 * @throws SQLException Thrown if a database operation fails
	 */
    public boolean isValid( final Integer projectID,
                            final String projectMember,
                            final String variableName )
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

		script.addLine( "SELECT EXISTS" );
		script.addLine( "(" );
		script.addTab().addLine("SELECT 1");
		script.addTab().addLine( "FROM wres.TimeSeries TS" );
		script.addTab().addLine( "WHERE EXISTS" );
        script.addTab().addLine( "(" );
		script.addTab( 2 ).addLine( "SELECT 1" );
		script.addTab( 2 ).addLine( "FROM" );
        script.addTab( 2 ).addLine( "(" );
		script.addTab(  3  ).addLine( "SELECT PS.source_id" );
		script.addTab(  3  ).addLine( "FROM wres.ProjectSource PS" );
		script.addTab(  3  ).addLine( "WHERE PS.project_id = ", projectID );
		script.addTab(   4   ).addLine( "AND PS.member = ", member );
		script.addTab(   4   ).addLine( "AND PS.source_id = TS.source_id" );
		script.addTab(   3   ).addLine(") AS PS");
		script.addTab( 2 ).add( ") AND TS.variable_name = '" );
		script.add( variableName );
        script.addLine( "'" );
		script.add(");");

		return script.retrieve( "exists" );
	}
}
