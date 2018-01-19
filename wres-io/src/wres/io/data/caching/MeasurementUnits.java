package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.Strings;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits extends Cache<MeasurementDetails, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementUnits.class);
    /**
     *  Internal, Global cache of measurement details
     */
	private static  MeasurementUnits instance = null;
	private static final Object CACHE_LOCK = new Object();

    private static MeasurementUnits getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance == null)
            {
                instance = new MeasurementUnits();
                instance.init();
            }
            return instance;
        }
    }
	
	/**
	 * Returns the ID of a unit of measurement from the global cache based on the name of the measurement
	 * @param unit The name of the unit of measurement
	 * @return The ID of the unit of measurement
	 * @throws SQLException Thrown if the ID could not be retrieved from the database 
	 */
	public static Integer getMeasurementUnitID(String unit) throws SQLException {
        Integer id = null;
        if (unit != null && !unit.trim().isEmpty())
        {
            id = getCache().getID(unit.toLowerCase());
        }

        if (id == null)
        {
            MeasurementDetails details = new MeasurementDetails();
            details.setUnit( unit );
            id = getCache().getID(details);
        }

	    return id;
	}

	public static String getNameByID(Integer id)
    {
        return Collections.getKeyByValue(MeasurementUnits.getCache().getKeyIndex(), id);
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
        ResultSet measurements = null;

        try
        {
            connection = Database.getHighPriorityConnection();

            String loadScript = "SELECT measurementunit_id, unit_name" + NEWLINE;
            loadScript += "FROM wres.measurementunit" + NEWLINE;
            loadScript += "LIMIT " + getMaxDetails() + ";";

            measurements = Database.getResults(connection, loadScript);

            while (measurements.next())
            {
                this.getKeyIndex().put(measurements.getString("unit_name").toLowerCase(), measurements.getInt("measurementunit_id"));
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

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
	}
}
