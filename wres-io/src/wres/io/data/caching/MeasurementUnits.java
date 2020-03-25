package wres.io.data.caching;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits extends Cache<MeasurementDetails, String>
{
    private static final int MAX_DETAILS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger(MeasurementUnits.class);

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Database database;

    public MeasurementUnits( Database database )
    {
        this.database = database;
    }

    @Override
    protected Database getDatabase()
    {
        return this.database;
    }

    @Override
    protected Object getDetailLock()
    {
        return this.detailLock;
    }

    @Override
    protected Object getKeyLock()
    {
        return this.keyLock;
    }

    private void populate(DataProvider data)
    {
        while (data.next())
        {
            if (!data.isNull( "unit_name" ))
            {
                this.getKeyIndex().put( data.getString( "unit_name" ), data.getInt( "measurementunit_id" ) );
            }
        }
    }


	/**
	 * Returns the ID of a unit of measurement from the global cache based on the name of the measurement
	 * @param unit The name of the unit of measurement
	 * @return The ID of the unit of measurement
	 * @throws SQLException Thrown if the ID could not be retrieved from the database 
	 */
	public Integer getMeasurementUnitID(String unit) throws SQLException {
        Integer id = null;
        if (unit != null && !unit.trim().isEmpty())
        {
            id = this.getID(unit.toLowerCase());
        }

        if (id == null)
        {
            MeasurementDetails details = new MeasurementDetails();
            details.setUnit( unit );
            id = this.getID(details);
        }

	    return id;
	}

	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}
	
	/**
	 * Loads all pre-existing data into the instance cache
	 */
    private synchronized void initialize()
	{
        try
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.setHighPriority( true );

            script.addLine("SELECT measurementunit_id, unit_name");
            script.addLine("FROM wres.MeasurementUnit");
            script.add("LIMIT ", MAX_DETAILS, ";");

            try (DataProvider data = script.getData())
            {
                this.populate( data );
            }
            LOGGER.debug( "Finished populating the MeasurementUnit details." );
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Measurement cache.",
                         error );
        }
	}
}
