package wres.io.data.caching;

import java.sql.SQLException;

import wres.io.data.details.MeasurementDetails;
import wres.io.utilities.Database;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits extends Cache<MeasurementDetails, String>
{
    private static final int MAX_DETAILS = 100;

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


	/**
	 * Returns the ID of a unit of measurement from the global cache based on the name of the measurement
	 * @param unit The name of the unit of measurement
	 * @return The ID of the unit of measurement
	 * @throws SQLException Thrown if the ID could not be retrieved from the database 
	 */
	public Long getMeasurementUnitID( String unit ) throws SQLException
    {
        Long id = null;

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
	
}
