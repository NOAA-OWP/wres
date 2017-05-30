package wres.io.data.details;

import java.sql.SQLException;

import wres.io.utilities.Database;

/**
 * Details defining a unit of measurement within the database (i.e. CFS (cubic feet per second),
 * M (meter), etc)
 * @author Christopher Tubbs
 */
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
			this.unit = unit;
			this.measurementunit_id = null;
		}
	}
	
	/**
	 * @return The ID for this particular unit of measurement
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public int getMeasurementUnitID() throws SQLException
	{
		if (measurementunit_id == null)
		{
			save();
		}
		return measurementunit_id;
	}

	@Override
	public int compareTo(MeasurementDetails other) {
		return this.unit.compareTo(other.unit);
	}

	@Override
	public String getKey() {
		return this.unit;
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

		script += "WITH new_measurementunit_id AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.MeasurementUnit (unit_name)" + newline;
		script += "		SELECT '" + unit + "'" + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.MeasurementUnit" + newline;
		script += "			WHERE unit_name = '" + unit + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING measurementunit_id" + newline;
		script += ")" + newline;
		script += "SELECT measurementunit_id" + newline;
		script += "FROM new_measurementunit_id" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT measurementunit_id" + newline;
		script += "FROM wres.MeasurementUnit" + newline;
		script += "WHERE unit_name = '" + unit + "';";

		return script;
	}
	
    private static String getUnitConversionInsertScript() {
	    String script = "";
	    
	    script += "INSERT INTO wres.UnitConversion(from_unit, to_unit, factor)" + newline;
	    script += "SELECT measurementunit_id, measurementunit_id, 1" + newline;
	    script += "FROM wres.MeasurementUnit M" + newline;
	    script += "WHERE NOT EXISTS (" + newline;
	    script += "    SELECT 1" + newline;
	    script += "    FROM wres.UnitConversion UC" + newline;
	    script += "    WHERE UC.from_unit = M.measurementunit_id" + newline;
	    script += "        AND UC.from_unit = UC.to_unit" + newline;
	    script += ");";
	    
	    return script;
	}
}
