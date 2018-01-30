package wres.io.data.details;

import java.sql.SQLException;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.Database;
import wres.util.TimeHelper;

/**
 * Details about a source of observation or forecast data
 * @author Christopher Tubbs
 */
public class SourceDetails extends CachedDetail<SourceDetails, SourceKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger( SourceDetails.class );

	private String sourcePath = null;
	private String outputTime = null;
	private Integer lead = null;
	private Integer sourceID = null;
	private String hash = null;
	private SourceKey key = null;
	private boolean performedInsert;

	/**
	 * Constructor
	 */
	public SourceDetails() {
		this.setSourcePath(null);
		this.setOutputTime(null);
		this.setID(null);
		this.setHash( null );

	}

	/**
	 * Constructor
	 * @param key A TwoTuple containing, first, the path to the source file and, second, the time
	 * that the source file was generated
	 */
	public SourceDetails(SourceKey key) {
		this.setSourcePath(key.getSourcePath());
		this.setOutputTime(key.getSourceTime());
		this.setLead(key.getLead());
		this.setHash( key.getHash() );
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
	 * @param outputTime the time that the file was generated
	 */
	public void setOutputTime(String outputTime) {
		this.outputTime = outputTime;
	}


	public void setLead(Integer lead)
	{
		this.lead = lead;
	}

	public void setHash(String hash)
    {
        this.hash = hash;
    }

    public String getHash()
	{
		return this.hash;
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
	    if (this.key == null)
        {
            this.key = new SourceKey( this.sourcePath,
                                      this.outputTime,
                                      this.lead,
                                      this.hash );
        }
        return this.key;
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
	protected String getInsertSelectStatement() throws SQLException
	{
	    if (this.hash == null)
        {
            throw new SQLException( "Could not save '" + this.sourcePath + "'; there was no file hash." );
        }

		String script = "";
		script += "WITH new_source AS" + NEWLINE;
		script += "(" + NEWLINE;
		script += "		INSERT INTO wres.Source (path, output_time, lead, hash)" + NEWLINE;
		script += "		SELECT '" + this.sourcePath + "'," + NEWLINE;
		script += "				'" + this.outputTime + "'," + NEWLINE;
		script += "             " + this.lead + "," + NEWLINE;
		script += "             '" + this.hash + "'" + NEWLINE;
		script += "		WHERE NOT EXISTS (" + NEWLINE;
		script += "			SELECT 1" + NEWLINE;
		script += "			FROM wres.Source" + NEWLINE;
		script += "			WHERE hash = '" + this.hash + "'" + NEWLINE;
		script += "		)" + NEWLINE;
		script += "		RETURNING source_id" + NEWLINE;
		script += ")" + NEWLINE;
        script += "SELECT source_id, TRUE as wasInserted" + NEWLINE;
		script += "FROM new_source" + NEWLINE + NEWLINE;
		script += "";
		script += "UNION" + NEWLINE + NEWLINE;
		script += "";
        script += "SELECT source_id, FALSE as wasInserted" + NEWLINE;
		script += "FROM wres.Source" + NEWLINE;
		script += "WHERE hash = '" + this.hash + "';" + NEWLINE;

		return script;
	}

    @Override
    public void save() throws SQLException
    {
        String[] tablesToLock = { "wres.source" };
        Pair<Integer,Boolean> databaseResult
                = Database.getResult( this.getInsertSelectStatement(),
                                      this.getIDName(),
                                      tablesToLock );

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Did I create Source ID {}? {}",
                          databaseResult.getLeft(),
                          databaseResult.getRight() );
        }

        this.setID( databaseResult.getLeft() );
        this.performedInsert = databaseResult.getRight();
    }

    public boolean performedInsert()
    {
        return this.performedInsert;
    }

	public static SourceKey createKey(String sourcePath, String sourceTime, Integer lead, String hash)
	{
	    return new SourceKey( sourcePath, sourceTime, lead, hash);
	}

	public static class SourceKey implements Comparable<SourceKey>
	{
	    public SourceKey(String sourcePath, String sourceTime, Integer lead, String hash)
	    {
	        this.sourcePath = sourcePath;
	        this.sourceTime = sourceTime;
	        this.lead = lead;
	        this.hash = hash;
	    }
	    
        @Override
        public int compareTo(SourceKey other)
        {
			return this.hash.compareTo( other.hash );
        }
        
        public String getSourcePath()
        {
            return this.sourcePath;
        }
        
        public String getSourceTime()
        {
            return this.sourceTime;
        }

        public Integer getLead()
        {
            return this.lead;
        }

        public String getHash()
		{
			return this.hash;
		}

		@Override
		public boolean equals(Object obj) {
	        return obj != null &&
				   obj instanceof SourceKey &&
				   obj.hashCode() == this.hashCode();
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.getHash());
		}

		private final String sourcePath;
	    private final String sourceTime;
	    private final Integer lead;
	    private final String hash;
	}
}
