package wres.io.data.caching;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

import wres.io.utilities.DataProvider;
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
    public List<String> getAvailableVariables( final long projectID,
                                               final String projectMember )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );

        script.addLine( "SELECT DISTINCT( TS.variable_name )" );
        script.addLine( "FROM wres.TimeSeries TS" );
        script.addLine( "INNER JOIN wres.ProjectSource PS ON" );
        script.addTab().addLine( "TS.source_id = PS.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectID );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( projectMember );

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
	 * @throws NullPointerException If any nullable input is null
	 */
    public boolean isValid( final long projectID,
                            final String projectMember,
                            final String variableName )
            throws SQLException
	{
        Objects.requireNonNull( projectMember );
        Objects.requireNonNull( variableName );
        
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine( "SELECT 1" );
        script.addLine( "FROM wres.TimeSeries TS" );
        script.addLine( "INNER JOIN wres.ProjectSource PS ON" );
        script.addTab().addLine( "PS.source_id = TS.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectID );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( projectMember );
        script.addTab().addLine( "AND TS.variable_name = ?" );
        script.addArgument( variableName );
        script.setMaxRows( 1 );

        try ( DataProvider provider = script.getData() )
        {
            // When a row exists, next returns true. Otherwise false.
            return provider.next();
        }
	}
}
