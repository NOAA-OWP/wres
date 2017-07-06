package wres.io.data.caching;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.Database;
import wres.util.Strings;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits extends Cache<MeasurementDetails, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementUnits.class);
    /**
     *  Internal, Global cache of measurement details
     */
	private static final MeasurementUnits internalCache = new MeasurementUnits();
	
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
	 * @throws SQLException
	 */
	public static Integer getMeasurementUnitID(String unit) throws SQLException
	{
		return internalCache.getID(unit.toLowerCase());
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
	 * Loads all pre-existing data into the instance cache
	 */
	@Override
    protected synchronized void init()
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
            loadScript += "FROM wres.measurementunit" + NEWLINE;
            loadScript += "LIMIT " + getMaxDetails() + ";";

            measurements = measurementQuery.executeQuery(loadScript);

            while (measurements.next())
            {
                this.keyIndex.put(measurements.getString("unit_name").toLowerCase(), measurements.getInt("measurementunit_id"));
            }
        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Measurement cache.");
            LOGGER.error(Strings.getStackTrace(error));
        }
        finally
        {
            if (measurements != null)
            {
                try
                {
                    measurements.close();
                }
                catch(SQLException e)
                {
                    LOGGER.error("An error was encountered when trying to close the resultset that loaded measurements.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (measurementQuery != null)
            {
                try
                {
                    measurementQuery.close();
                }
                catch(SQLException e)
                {
                    LOGGER.error("An error was encountered when trying to close the statement that retrieved measurement values.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
	}
}
