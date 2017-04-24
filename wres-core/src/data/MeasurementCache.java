/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import data.details.MeasurementDetails;
import util.Database;

/**
 * @author ctubbs
 *
 */
public class MeasurementCache extends Cache<MeasurementDetails, String> {

	private static MeasurementCache internalCache = new MeasurementCache();
	
	public static Integer getMeasurementUnitID(MeasurementDetails detail) throws Exception
	{
		return internalCache.getID(detail);
	}
	
	public static Integer getMeasurementUnitID(String unit) throws Exception {
		return internalCache.getUnitID(unit);
	}
	
	public static String getUnit(Integer measurementunitID)
	{
		return internalCache.getKey(measurementunitID);
	}
	
	public Integer getUnitID(String unit) throws Exception
	{
		if (!keyIndex.containsKey(unit))
		{
			MeasurementDetails details = new MeasurementDetails();
			details.set_unit(unit);
			addElement(details);
		}
		
		return keyIndex.get(unit);
	}

	@Override
	protected Integer getMaxDetails() {
		return 100;
	}

	public synchronized static void initialize() throws SQLException {
		Connection connection = Database.getConnection();
		Statement measurementQuery = connection.createStatement();
		measurementQuery.setFetchSize(100);
		
		String loadScript = "SELECT measurementunit_id, unit_name" + System.lineSeparator();
		loadScript += "FROM wres.measurementunit;";
		
		ResultSet measurements = measurementQuery.executeQuery(loadScript);
		
		MeasurementDetails detail = null;
		
		while (measurements.next()) {
			detail = new MeasurementDetails();
			detail.set_unit(measurements.getString("unit_name"));
			detail.setID(measurements.getInt("measurementunit_id"));
			
			internalCache.details.put(detail.getId(), detail);
			internalCache.keyIndex.put(detail.getKey(), detail.getId());
		}
		
		measurements.close();
		measurementQuery.close();
		Database.returnConnection(connection);
	}
}
