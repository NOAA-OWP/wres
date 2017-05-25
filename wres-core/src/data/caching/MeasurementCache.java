/**
 * 
 */
package data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import data.details.MeasurementDetails;
import util.Database;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementCache extends Cache<MeasurementDetails, String> {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());
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
	 * @throws Exception 
	 */
	public static Integer getMeasurementUnitID(String unit) throws Exception
	{
		return internalCache.getID(unit);
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
	    synchronized(keyIndex)
	    {
            Connection connection = null;
            Statement measurementQuery = null;
            ResultSet measurements = null;

            try
            {
                connection = Database.getConnection();
                measurementQuery = connection.createStatement();
                measurementQuery.setFetchSize(100);

                String loadScript = "SELECT measurementunit_id, unit_name" + System.lineSeparator();
                loadScript += "FROM wres.measurementunit" + newline;
                loadScript += "LIMIT " + getMaxDetails() + ";";

                measurements = measurementQuery.executeQuery(loadScript);

                while (measurements.next()) {                
                    this.keyIndex.put(measurements.getString("unit_name"), measurements.getInt("measurementunit_id"));
                }
            }
            catch (SQLException error)
            {
                LOGGER.error("An error was encountered when trying to populate the Measurement cache. {}",
                             error);
                throw error;
            }
            finally
            {
                if (measurements != null)
                {
                    measurements.close();
                }

                if (measurementQuery != null)
                {
                    measurementQuery.close();
                }

                if (connection != null)
                {
                    Database.returnConnection(connection);
                }
            }
	    }
	}
}
