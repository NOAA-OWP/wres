package wres.io.data.details;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.Strings;

/**
 * Details defining a unit of measurement within the database (i.e. CFS (cubic feet per second),
 * M (meter), etc)
 * @author Christopher Tubbs
 */
public final class MeasurementDetails extends CachedDetail<MeasurementDetails, String>
{
    private static final Logger
            LOGGER = LoggerFactory.getLogger( MeasurementDetails.class );

	// Prevents asynchronous saving of identical measurementunits
	private static final Object MEASUREMENTUNIT_SAVE_LOCK = new Object();

	private String unit = null;
	private Integer measurementUnitID = null;

	/**
	 * Sets the name of the unit of measurement
	 * @param unit The new name of the unit of measurement
	 */
	public void setUnit(String unit)
	{
		if ( Strings.hasValue(unit) && (this.unit == null || !this.unit.equalsIgnoreCase( unit)))
		{
			this.unit = unit.toLowerCase();
			this.measurementUnitID = null;
		}
	}

	@Override
	public int compareTo(MeasurementDetails other) {
		return this.unit.compareTo(other.unit);
	}

	@Override
	public String getKey() {
		return this.unit.toLowerCase();
	}

	@Override
	public Integer getId() {
		return this.measurementUnitID;
	}

	@Override
	protected String getIDName() {
		return "measurementunit_id";
	}

	@Override
	public void setID(Integer id) {
		this.measurementUnitID = id;
	}

    @Override
    protected Logger getLogger()
    {
        return MeasurementDetails.LOGGER;
    }

    @Override
    public String toString()
    {
        return this.unit;
    }

	@Override
	protected PreparedStatement getInsertSelectStatement( Connection connection )
			throws SQLException
	{
		ScriptBuilder script = new ScriptBuilder(  );
		script.addLine("SELECT measurementunit_id");
		script.addLine("FROM wres.MeasurementUnit");
		script.addLine("WHERE unit_name = ?;");

		return script.getPreparedStatement( connection, this.unit );
	}

	@Override
	protected Object getSaveLock()
	{
		return MEASUREMENTUNIT_SAVE_LOCK;
	}
}
