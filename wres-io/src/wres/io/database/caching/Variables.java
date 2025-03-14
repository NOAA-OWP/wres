package wres.io.database.caching;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;

/**
 * Helpers for data on variables used in time-series data.
 */
public class Variables
{
    private final Database database;

    /**
     * Creates an instance.
     * @param database the database
     */
    public Variables( Database database )
    {
        this.database = database;
    }

    /**
     * Retrieves the names of all variables associated with a given side of data.
     * @param projectID The id of the project we're interested in
     * @param projectMember The data source member for the data (generally 'right')
     * @return A list of all names of variables that may be evaluated for the project
     * @throws SQLException Thrown if an error was encountered while communicating with the database
     */
    public List<String> getAvailableVariables( final long projectID,
                                               final String projectMember )
            throws SQLException
    {
        Database db = this.getDatabase();
        DataScripter script = new DataScripter( db );

        script.addLine( "SELECT DISTINCT( S.variable_name )" );
        script.addLine( "FROM wres.Source S" );
        script.addLine( "INNER JOIN wres.ProjectSource PS ON" );
        script.addTab().addLine( "S.source_id = PS.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectID );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( projectMember );

        // To remain open until all series have been read
        List<String> variables = new ArrayList<>();
        try ( Connection connection = this.getDatabase()
                                          .getConnection();
              DataProvider provider = script.buffer( connection ) )
        {
            while ( provider.next() )
            {
                String nextVariable = provider.getString( "variable_name" );
                variables.add( nextVariable );
            }
        }

        return Collections.unmodifiableList( variables );
    }

    /**
     * Checks to see if there are any values for a named variable tied to the project
     * @param projectID The ID of the project to check
     * @param projectMember The evaluation member of the project ("left", "right", or "baseline")
     * @param variableName The variable
     * @return Whether there is any forecast data for the variable within the project
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

        Database db = this.getDatabase();
        DataScripter script = new DataScripter( db );
        script.addLine( "SELECT 1" );
        script.addLine( "FROM wres.Source S" );
        script.addLine( "INNER JOIN wres.ProjectSource PS ON" );
        script.addTab().addLine( "PS.source_id = S.source_id" );
        script.addLine( "WHERE PS.project_id = ?" );
        script.addArgument( projectID );
        script.addTab().addLine( "AND PS.member = ?" );
        script.addArgument( projectMember );
        script.addTab().addLine( "AND S.variable_name = ?" );
        script.addArgument( variableName );
        script.setMaxRows( 1 );

        try ( DataProvider provider = script.getData() )
        {
            // When a row exists, next returns true. Otherwise false.
            return provider.next();
        }
    }

    /**
     * @return the database
     */
    private Database getDatabase()
    {
        return this.database;
    }

}
