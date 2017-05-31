package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import wres.io.data.details.SourceDetails.SourceKey;
/**
 * Details about a source of observation or forecast data
 * @author Christopher Tubbs
 */
public class SourceDetails extends CachedDetail<SourceDetails, SourceKey> {

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
	public SourceDetails(SourceKey key) {
		this.setSourcePath(key.getSourcePath());
		this.setOutputTime(key.getSourceTime());
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
	public SourceKey getKey() {
		return new SourceKey(sourcePath, outputTime);
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
		script += "WITH new_source AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Source (path, output_time)" + NEWLINE;
		script += "		SELECT '" + this.sourcePath + "'," + NEWLINE;
		script += "				'" + this.outputTime + "'" + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Source" + NEWLINE;
		script += "			WHERE path = '" + this.sourcePath + "'" + NEWLINE;
		script += "				AND output_time = '" + this.outputTime + "'" + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING source_id" + NEWLINE;
		script += ")" + NEWLINE;
		script += "SELECT source_id" + NEWLINE;
		script += "FROM new_source" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
		script += "SELECT source_id" + NEWLINE;
		script += "FROM wres.Source" + NEWLINE;
		script += "WHERE path = '" + this.sourcePath + "'" + NEWLINE;
		script += "		AND output_time = '" + this.outputTime + "';";

		return script;
	}
	
	public static SourceKey createKey(String sourcePath, String sourceTime)
	{
	    return new SourceDetails().new SourceKey(sourcePath, sourceTime);
	}

	public class SourceKey implements Comparable<SourceKey>
	{
	    public SourceKey(String sourcePath, String sourceTime)
	    {
	        this.sourcePath = sourcePath;
	        this.sourceTime = sourceTime;
	    }
	    
        @Override
        public int compareTo(SourceKey other)
        {

            int equality = 0;

            if (this.getSourcePath() == null && other.getSourcePath() == null)
            {
                equality = 0;
            }
            else if (this.getSourcePath() != null && other.getSourcePath() == null)
            {
                equality = 1;
            }
            else if (this.getSourcePath() == null && other.getSourcePath() != null)
            {
                equality = -1;
            }
            else
            {
                equality = this.getSourcePath().toLowerCase().compareTo(other.getSourcePath().toLowerCase());
            }

            if (equality == 0)
            {
                if (this.getSourceTime() == null && other.getSourceTime() == null)
                {
                    equality = 0;
                }
                else if (this.getSourceTime() != null && other.getSourceTime() == null)
                {
                    equality = 1;
                }
                else if (this.getSourceTime() == null && other.getSourceTime() != null)
                {
                    equality = -1;
                }
                else
                {
                    equality = this.getSourceTime().toLowerCase().compareTo(other.getSourceTime().toLowerCase());
                }
            }

            return equality;
        }
        
        public String getSourcePath()
        {
            return this.sourcePath;
        }
        
        public String getSourceTime()
        {
            return this.sourceTime;
        }

		@Override
		public boolean equals(Object obj) {
	        boolean equivalent = false;

	        if (obj instanceof SourceKey)
            {
                equivalent = this.compareTo((SourceKey)obj) == 0;
            }

			return equivalent;
		}

		@Override
		public int hashCode() {
			return Objects.hashCode(this.sourcePath) + Objects.hashCode(this.sourceTime);
		}

		private String sourcePath;
	    private String sourceTime;
	}
}
