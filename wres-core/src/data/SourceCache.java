/**
 * 
 */
package data;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import collections.TwoTuple;
import data.details.SourceDetails;
import util.Database;

/**
 * @author ctubbs
 *
 */
public class SourceCache extends Cache<SourceDetails, TwoTuple<String, String>> {

	private static SourceCache internalCache = new SourceCache();
	
	public static Integer getSourceID(String path, String output_time) throws Exception {
		return internalCache.getID(path, output_time);
	}
	
	public static Integer getSourceID(SourceDetails detail) throws Exception {
		return internalCache.getID(detail);
	}	
	
	public Integer getID(String path, String output_time) throws Exception {
		return getID(new TwoTuple<String, String>(path, output_time));
	}
	
	public Integer getID(TwoTuple<String, String> key) throws Exception {
		if (!keyIndex.containsKey(key)) {
			addElement(new SourceDetails(key));
		}
		return this.keyIndex.get(key);
	}
	
	@Override
	protected Integer getMaxDetails() {
		return 100;
	}

	public static void initialize() throws SQLException {
		Connection connection = Database.getConnection();
		Statement sourceQuery = connection.createStatement();
		String loadScript = "SELECT source_id, path, CAST(output_time AS TEXT) AS output_time" + System.lineSeparator();
		loadScript = "FROM wres.Source;";
		
		ResultSet sources = sourceQuery.executeQuery(loadScript);
		SourceDetails detail = null;
		
		while (sources.next()) {
			detail = new SourceDetails();
			detail.setOutputTime(sources.getString("output_time"));
			detail.setSourcePath(sources.getString("path"));
			detail.setID(sources.getInt("source_id"));
			
			internalCache.details.put(detail.getId(), detail);
			internalCache.keyIndex.put(detail.getKey(), detail.getId());
		}
		
		sources.close();
		sourceQuery.close();
		Database.returnConnection(connection);
	}
	
}
