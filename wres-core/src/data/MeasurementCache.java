/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import data.details.MeasurementDetails;
import util.Database;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementCache extends Cache<MeasurementDetails, String> {

    /**
     *  Internal, Global cache of measurement details
     */
	private static MeasurementCache internalCache = new MeasurementCache();
	
	/**
	 * Return the ID of a unit of measurement from the global cache based on the specification of the measurement
	 * details
	 * @param detail The specification for the unit of measurement
	 * @return The ID for the unit of measurement
	 * @throws Exception Thrown if the ID could not be retrieved from the database 
	 */
	public static Integer getMeasurementUnitID(MeasurementDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	/**
	 * Returns the ID of a unit of measurement from the global cache based on the name of the measurement
	 * @param unit The name of the unit of measurement
	 * @return The ID of the unit of measurement
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getMeasurementUnitID(String unit) throws Exception {
		return internalCache.getUnitID(unit);
	}
	
	/**
	 * Returns the name of the unit of measurement from the global cache based on its ID
	 * @param measurementunitID The ID of the unit of measurement from the database
	 * @return The name of the unit of measurement
	 */
	public static String getUnit(Integer measurementunitID)
	{
		return internalCache.getKey(measurementunitID);
	}
	
	/**
	 * Returns the ID of the unit of measurement from the instance cache based on its name
	 * @param unit The name of the unit of measurement
	 * @return The ID of the unit of measurement
	 * @throws Exception Thrown if the ID of the unit of measurement couldn't be loaded from the
	 * database or stored in the cache
	 */
	public Integer getUnitID(String unit) throws Exception
	{
		if (!keyIndex.containsKey(unit))
		{
			MeasurementDetails details = new MeasurementDetails();
			details.setUnit(unit);
			addElement(details);
		}
		
		return keyIndex.get(unit);
	}

	@Override
	protected int getMaxDetails() {
		return 100;
	}

	/**
	 * Loads all pre-existing data into the global cache
	 * @throws SQLException Thrown if data couldn't be loaded from the database or stored in the global cache
	 */
	public synchronized static void initialize() throws SQLException {
		internalCache.init();
	}
	
	/**
	 * Loads all pre-existing data into the instance cache
	 * @throws SQLException Thrown if data couldn't be loaded from the database or stored in the cache
	 */
	@Override
    public synchronized void init() throws SQLException {
        Connection connection = Database.getConnection();
        Statement measurementQuery = connection.createStatement();
        measurementQuery.setFetchSize(100);
        
        String loadScript = "SELECT measurementunit_id, unit_name" + System.lineSeparator();
        loadScript += "FROM wres.measurementunit";
        
        ResultSet measurements = measurementQuery.executeQuery(loadScript);
        
        MeasurementDetails detail = null;
        
        while (measurements.next()) {
            detail = new MeasurementDetails();
            detail.setUnit(measurements.getString("unit_name"));
            detail.setID(measurements.getInt("measurementunit_id"));
            
            this.details.put(detail.getId(), detail);
            this.keyIndex.put(detail.getKey(), detail.getId());
        }
        
        measurements.close();
        measurementQuery.close();
        Database.returnConnection(connection);
	}
}
