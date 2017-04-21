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
public final class FeatureDetails implements Comparable<FeatureDetails>
{
	private final static String newline = System.lineSeparator();

	private Integer variable_id = null;
	private String lid = null;
	public String station_name = null;
	private Integer feature_id = null;
	private Integer variableposition_id = null;
	
	public void set_variable_id(int variable_id)
	{
		if (this.variable_id == null || this.variable_id != variable_id)
		{
			this.variable_id = variable_id;
			this.feature_id = null;
		}
	}
	
	public void set_lid(String lid)
	{
		if (this.lid == null || !this.lid.equalsIgnoreCase(lid))
		{
			this.lid = lid;
			this.feature_id = null;
		}
	}
	
	public int get_variableposition_id() throws SQLException
	{
		if (variableposition_id == null)
		{
			save();
		}
		
		return variableposition_id;
	}
	
	private String get_station_name()
	{
		String name = null;
		
		if (station_name == null)
		{
			name = "null";
		}
		else
		{
			name = "'" + station_name + "'";
		}
		
		return name;
	}
	
	private void save() throws SQLException
	{
		String script = "";
		
		script += "WITH new_feature AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Feature (comid, lid, feature_name)" + newline;
		script += "		SELECT COALESCE((" + newline;
		script += "				SELECT MAX(feature_id) + 1" + newline;
		script += "				FROM wres.Feature" + newline;
		script += "			), 0)," + newline;
		script += "			'" + lid + "'," + newline;
		script += "			" + get_station_name() + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Feature" + newline;
		script += "			WHERE lid = '" + lid + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING comid" + newline;
		script += ")" + newline;
		script += "SELECT comid" + newline;
		script += "FROM new_feature" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT comid" + newline;
		script += "FROM wres.feature" + newline;
		script += "WHERE lid = '" + lid + "'" + newline;
		script += "LIMIT 1;";
		
		feature_id = Database.get_result(script, "comid");
		
		script = "SELECT wres.get_variableposition_id(" + feature_id + ", " + variable_id + ") AS variableposition_id;";

		variableposition_id = Database.get_result(script, "variableposition_id");
	}

	@Override
	public int compareTo(FeatureDetails other) {
		Integer id = this.feature_id;
		
		if (id == null)
		{
			id = -1;
		}

		return id.compareTo(other.feature_id);
	}

}
