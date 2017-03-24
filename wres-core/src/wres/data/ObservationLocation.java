/**
 * 
 */
package wres.data;

import java.sql.ResultSet;
import java.sql.SQLException;
import wres.data.definition.LocationDef;
import wres.util.Database;

/**
 * @author ctubbs
 *
 */
public class ObservationLocation {
	
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
		
		ResultSet result = null;
		
		if (definition.for_coordinates())
		{
			result = Database.execute_for_result(String.format(find_by_latlon, 
											     definition.get_latitude(), 
											     definition.get_longitude()));
		}
		else if (definition.for_feature())
		{
			result = Database.execute_for_result(String.format(find_by_feature_id, 
			  								     definition.get_feature_id()));
		}
		
		found = result != null && result.getRow() > 0;	
		return found;
	}

	private static String find_by_latlon = "SELECT 1\n"
										  + "FROM ObservationLocation\n"
										  + "WHERE nws_lat = %f AND nws_lon = %f;";
	
	private static String find_by_feature_id = "SELECT 1\n"
											 + "FROM ObservationLocation\n"
											 + "WHERE comid = %d;";
}
