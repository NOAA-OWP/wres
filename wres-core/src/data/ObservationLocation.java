/**
 * 
 */
package data;

import java.sql.SQLException;

import data.definition.LocationDef;
import util.Database;

/**
 * @author ctubbs
 *
 */
public class ObservationLocation {
	
	private final static String newline = System.lineSeparator(); 
	
	public static synchronized void add(LocationDef new_definition) throws Exception
	{
		new_definition.validate();
		
		if (exists(new_definition))
		{
			return;
		}
	
		Database.execute(new_definition.add_script());
	}
	
	private static synchronized boolean exists(LocationDef definition) throws SQLException
	{
		boolean found = false;
		
		if (definition.for_coordinates())
		{
			found = Database.get_result(String.format(find_by_latlon, 
													  definition.get_latitude(), 
													  definition.get_longitude()), "location_exists");
		}
		else if (definition.for_feature())
		{
			found = Database.get_result(String.format(find_by_feature_id, definition.get_feature_id()), "location_exists");
		}
		
		return found;
	}

	private static String find_by_latlon = "SELECT true AS location_exists\n"
										  + "FROM ObservationLocation\n"
										  + "WHERE nws_lat = %f AND nws_lon = %f;";
	
	private static String find_by_feature_id = "SELECT true AS location_exists\n"
											 + "FROM ObservationLocation\n"
											 + "WHERE comid = %d;";
}
