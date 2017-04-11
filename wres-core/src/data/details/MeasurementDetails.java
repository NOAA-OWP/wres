/**
 * 
 */
package data.details;

import java.sql.SQLException;

import util.Database;

/**
 * @author ctubbs
 *
 */
public final class MeasurementDetails {
	private final static String newline = System.lineSeparator();
	
	private String unit = null;
	private Integer measurementunit_id = null;
	
	public void set_unit(String unit)
	{
		if (this.unit == null || !this.unit.equalsIgnoreCase(unit))
		{
			this.unit = unit;
			this.measurementunit_id = null;
		}
	}
	
	public int get_measurementunit_id() throws SQLException
	{
		if (measurementunit_id == null)
		{
			save();
		}
		return measurementunit_id;
	}
	
	public void save() throws SQLException
	{
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
		
		measurementunit_id = Database.get_result(script, "measurementunit_id");
	}
}
