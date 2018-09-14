package wres.io.data.caching;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.Collections;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits extends Cache<MeasurementDetails, String>
{
    private static final int MAX_DETAILS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementUnits.class);
    /**
     *  Internal, Global cache of measurement details
     */
	private static  MeasurementUnits instance = null;
	private static final Object CACHE_LOCK = new Object();

    private static final Object DETAIL_LOCK = new Object();
    private static final Object KEY_LOCK = new Object();

    @Override
    protected Object getDetailLock()
    {
        return MeasurementUnits.DETAIL_LOCK;
    }

    @Override
    protected Object getKeyLock()
    {
        return MeasurementUnits.KEY_LOCK;
    }

    private MeasurementUnits(DataProvider data)
    {
        while (data.next())
        {
            if (!data.isNull( "unit_name" ))
            {
                this.getKeyIndex().put( data.getString( "unit_name" ), data.getInt( "measurementunit_id" ) );
            }
        }
    }

    private static MeasurementUnits getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if ( instance == null)
            {
                MeasurementUnits.initialize();
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

	static String getNameByID(Integer id)
    {
        return Collections.getKeyByValue(MeasurementUnits.getCache().getKeyIndex(), id);
    }

	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}
	
	/**
	 * Loads all pre-existing data into the instance cache
     * TODO: Return MeasurementUnits, don't implicitly set it
	 */
    private static synchronized void initialize()
	{
        try
        {
            DataScripter script = new DataScripter(  );
            script.setHighPriority( true );

            script.addLine("SELECT measurementunit_id, unit_name");
            script.addLine("FROM wres.MeasurementUnit");
            script.add("LIMIT ", MAX_DETAILS, ";");

            MeasurementUnits.instance = new MeasurementUnits( script.getData() );
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Measurement cache.",
                         error );
        }
	}
}
