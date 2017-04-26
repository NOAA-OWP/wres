/**
 * 
 */
package data.details;

import java.sql.SQLException;
import collections.TwoTuple;

/**
 * @author ctubbs
 *
 */
public class SourceDetails extends CachedDetail<SourceDetails, TwoTuple<String, String>> {

	private String sourcePath = null;
	private String outputTime = null;
	private Integer sourceID = null;
	
	public SourceDetails() {
		this.sourceID = null;
		this.outputTime = null;
		this.sourcePath = null;
	}
	
	public SourceDetails(String sourcePath, String outputTime) {
		this.sourcePath = sourcePath;
		this.outputTime = outputTime;
		this.sourceID = null;
	}
	
	public SourceDetails(TwoTuple<String, String> key) {
		this.sourcePath = key.itemOne();
		this.outputTime = key.itemTwo();
		this.sourceID = null;
	}
	
	public void setSourcePath(String path) {
		this.sourcePath = path;
	}
	
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
	}
	
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
