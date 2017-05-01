/**
 * 
 */
package data.details;

import java.sql.SQLException;
import collections.TwoTuple;

/**
 * Details about a source of observation or forecast data
 * @author Christopher Tubbs
 */
public class SourceDetails extends CachedDetail<SourceDetails, TwoTuple<String, String>> {

	private String sourcePath = null;
	private String outputTime = null;
	private Integer sourceID = null;
	
	/**
	 * Constructor
	 */
	public SourceDetails() {
		this.setSourcePath(null);
		this.setOutputTime(null);
		this.setID(null);
	}
	
	/**
	 * Constructor
	 * @param sourcePath The path on the file system to the source file
	 * @param outputTime The time that the file was generated
	 */
	public SourceDetails(String sourcePath, String outputTime) {
		this.setSourcePath(sourcePath);
		this.setOutputTime(outputTime);
		this.setID(null);
	}
	
	/**
	 * Constructor
	 * @param key A TwoTuple containing, first, the path to the source file and, second, the time
	 * that the source file was generated
	 */
	public SourceDetails(TwoTuple<String, String> key) {
		this.setSourcePath(key.itemOne());
		this.setOutputTime(key.itemTwo());
		this.setID(null);
	}
	
	/**
	 * Sets the path to the source file
	 * @param path The path to the source file on the file system
	 */
	public void setSourcePath(String path) {
		this.sourcePath = path;
	}
	
	/**
	 * Sets the time that the file was generated
	 * @param outputTime
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
	}
	
	/**
	 * @return The ID of the information about the source in the database
	 * @throws SQLException Thrown if the data in the database could not be retrieved
	 */
	public Integer getSourceID() throws SQLException {
		if (this.sourceID == null) {
			save();
		}		
		return sourceID;
	}
	
	@Override
	public int compareTo(SourceDetails other) {
		Integer id = this.sourceID;
		
		if (id == null) {
			id = -1;
		}
		
		return id.compareTo(other.getId());
	}

	@Override
	public TwoTuple<String, String> getKey() {
		return new TwoTuple<String, String> (sourcePath, outputTime);
	}

	@Override
	public Integer getId() {
		return this.sourceID;
	}

	@Override
	protected String getIDName() {
		return "source_id";
	}

	@Override
	public void setID(Integer id) {
		this.sourceID = id;		
	}

	@Override
	protected String getInsertSelectStatement() {
		String script = "";
		script += "WITH new_source AS" + newline;
		script += "(" + newline;
		script += "		INSERT INTO wres.Source (path, output_time)" + newline;
		script += "		SELECT '" + this.sourcePath + "'," + newline;
		script += "				'" + this.outputTime + "'" + newline;
		script += "		WHERE NOT EXISTS (" + newline;
		script += "			SELECT 1" + newline;
		script += "			FROM wres.Source" + newline;
		script += "			WHERE path = '" + this.sourcePath + "'" + newline;
		script += "				AND output_time = '" + this.outputTime + "'" + newline;
		script += "		)" + newline;
		script += "		RETURNING source_id" + newline;
		script += ")" + newline;
		script += "SELECT source_id" + newline;
		script += "FROM new_source" + newline + newline;
		script += "";
		script += "UNION" + newline + newline;
		script += "";
		script += "SELECT source_id" + newline;
		script += "FROM wres.Source" + newline;
		script += "WHERE path = '" + this.sourcePath + "'" + newline;
		script += "		AND output_time = '" + this.outputTime + "';";

		return script;
	}

}
