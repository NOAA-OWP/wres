/**
 * 
 */
package data;

import java.sql.SQLException;

import data.definition.LocationDef;
import util.Database;

@Deprecated
/**
 * Interface for querying the database for information from public.ObservationLocation
 * @author Christopher Tubbs
 * @deprecated Use of any data from the public schema should be discontinued
 */
public class ObservationLocation {
	
    /**
     * Saves a location definition to the database if it isn't already in there
     * @param new_definition The new definition for a location
     * @throws Exception Thrown if the definition is determined invalid
     * @throws SQLException Thrown if the database couldn't be queried to determine if the location
     * already exists
     * @throws SQLException Thrown if the definition could not be saved to the database
     */
	public static synchronized void add(LocationDef new_definition) throws Exception
	{
		new_definition.validate();
		
		if (exists(new_definition))
		{
			return;
		}
	
		Database.execute(new_definition.add_script());
	}
	
	/**
	 * Determines if information about the location already exists in the database
	 * @param definition The definition to check for
	 * @return True if the location exists, false otherwise
	 * @throws SQLException Thrown if the query to the database fails
	 */
	private static synchronized boolean exists(LocationDef definition) throws SQLException
	{
		boolean found = false;
		
		if (definition.for_coordinates())
		{
			found = Database.getResult(String.format(find_by_latlon, 
													  definition.get_latitude(), 
													  definition.get_longitude()), "location_exists");
		}
		else if (definition.for_feature())
		{
			found = Database.getResult(String.format(find_by_feature_id, definition.get_feature_id()), "location_exists");
		}
		
		return found;
	}

	/**
	 * Definition for SQL script used to detect the existence of a location based on latitude and longitude 
	 */
	private static String find_by_latlon = "SELECT true AS location_exists\n"
										  + "FROM ObservationLocation\n"
										  + "WHERE nws_lat = %f AND nws_lon = %f;";
	
	/**
	 * Definition for SQL script used to detect the existence of a location based on its comid
	 */
	private static String find_by_feature_id = "SELECT true AS location_exists\n"
											 + "FROM ObservationLocation\n"
											 + "WHERE comid = %d;";
}
