package wres.io.data.details;

import java.sql.SQLException;

import wres.io.utilities.Database;
import wres.util.Internal;

/**
 * Details defining a unit of measurement within the database (i.e. CFS (cubic feet per second),
 * M (meter), etc)
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public final class MeasurementDetails extends CachedDetail<MeasurementDetails, String> {	
	private String unit = null;
	private Integer measurementunit_id = null;

	/**
	 * Sets the name of the unit of measurement
	 * @param unit The new name of the unit of measurement
	 */
	public void setUnit(String unit)
	{
		if (this.unit == null || !this.unit.equalsIgnoreCase(unit))
		{
			this.unit = unit.toLowerCase();
			this.measurementunit_id = null;
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
		return this.measurementunit_id;
	}

	@Override
	protected String getIDName() {
		return "measurementunit_id";
	}

	@Override
	public void setID(Integer id) {
		this.measurementunit_id = id;		
	}
	
	@Override
	public void save() throws SQLException
	{
	    super.save();
	    
	    Database.execute(MeasurementDetails.getUnitConversionInsertScript());
	}

	@Override
	protected String getInsertSelectStatement() {
		String script = "";

		script += "WITH new_measurementunit_id AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.MeasurementUnit (unit_name)" + NEWLINE;
		script += "		SELECT '" + unit + "'" + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.MeasurementUnit" + NEWLINE;
		script += "			WHERE unit_name = '" + unit + "'" + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING measurementunit_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT measurementunit_id" + NEWLINE;
		script += "FROM new_measurementunit_id" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT measurementunit_id" + NEWLINE;
		script += "FROM wres.MeasurementUnit" + NEWLINE;
		script += "WHERE unit_name = '" + unit + "';";

		return script;
	}
	
    private static String getUnitConversionInsertScript() {
	    String script = "";
	    
	    script += "INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)" + NEWLINE;
	    script += "SELECT measurementunit_id, measurementunit_id, 1" + NEWLINE;
	    script += "FROM wres.MeasurementUnit M" + NEWLINE;
	    script += "WHERE NOT EXISTS (" + NEWLINE;
	    script += "    SELECT 1" + NEWLINE;
	    script += "    FROM wres.UnitConversion UC" + NEWLINE;
	    script += "    WHERE UC.from_unit = M.measurementunit_id" + NEWLINE;
	    script += "        AND UC.from_unit = UC.to_unit" + NEWLINE;
	    script += ");";
	    
	    return script;
	}
}
