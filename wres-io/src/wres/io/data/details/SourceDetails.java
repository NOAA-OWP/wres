package wres.io.data.details;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Details about a source of observation or forecast data
 * @author Christopher Tubbs
 */
public class SourceDetails extends CachedDetail<SourceDetails, SourceKey>
{
	/**
	 * Prevents asynchronous saving of the same source information
	 */
	private static final Object SOURCE_SAVE_LOCK = new Object();
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceDetails.class );

	private URI sourcePath = null;
	private String outputTime = null;
	private Integer lead = null;
	private Long sourceID = null;
	private String hash = null;
	private SourceKey key = null;
	private boolean isPointData = true;
	private boolean performedInsert;

	/**
	 * Constructor
	 */
	public SourceDetails() {
		this.setSourcePath(null);
		this.setOutputTime(null);
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
	}

	public SourceDetails(final DataProvider data)
    {
        this.setSourcePath(  data.getURI("path") );
        this.setOutputTime( data.getString( "output_time" ) );
        this.setLead( data.getInt( "lead" ));
        this.setHash( data.getString("hash") );
        this.setIsPointData( data.getBoolean( "is_point_data" ) );
        this.setID( data.getLong( this.getIDName() ) );
    }

	/**
	 * Sets the path to the source file
	 * @param path The path to the source file on the file system
	 */
	public void setSourcePath( URI path ) {
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

    public void setIsPointData(boolean isPointData)
    {
        this.isPointData = isPointData;
    }

    public String getHash()
	{
		return this.hash;
	}

	public boolean getIsPointData()
    {
        return this.isPointData;
    }

    public URI getSourcePath()
    {
        return this.sourcePath;
    }

	@Override
	public int compareTo(SourceDetails other)
	{
		Long id = this.sourceID;

		if (id == null) {
			id = -1l;
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
	public Long getId() {
		return this.sourceID;
	}

	@Override
	protected String getIDName() {
		return "source_id";
	}

	@Override
	public void setID( long id ) {
		this.sourceID = id;
	}

	@Override
	protected DataScripter getInsertSelect( Database database )
	{
        DataScripter script = new DataScripter( database );

        script.setUseTransaction( true );

		script.retryOnSerializationFailure();
		script.retryOnUniqueViolation();

        script.setHighPriority( true );

        script.addLine( "INSERT INTO wres.Source ( path, output_time, lead, hash, is_point_data )" );
        script.addTab().addLine( "SELECT ?, (?)::timestamp without time zone, ?, ?, ?" );

        script.addArgument( this.sourcePath.toString() );
        script.addArgument( this.outputTime );
        script.addArgument( this.lead );
        script.addArgument( this.hash );
        script.addArgument( this.isPointData );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 ).addLine( "SELECT 1" );
        script.addTab( 2 ).addLine( "FROM wres.Source" );
        script.addTab( 2 ).addLine( "WHERE hash = ?" );

        script.addArgument( this.hash );

        script.addTab().addLine( ");" );

	    return script;
	}

	@Override
	protected Object getSaveLock()
	{
		return SourceDetails.SOURCE_SAVE_LOCK;
	}

	@Override
    public void save( Database database ) throws SQLException
    {
        DataScripter script = this.getInsertSelect( database );
        this.performedInsert = script.execute() > 0;

        if ( this.performedInsert )
        {
            this.sourceID = script.getInsertedIds()
                                  .get( 0 );
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.add( "SELECT " ).addLine( this.getIDName() );
            scriptWithId.addLine( "FROM wres.Source" );
            scriptWithId.addLine( "WHERE hash = ? " );
            scriptWithId.addArgument( this.hash );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.sourceID = data.getLong( this.getIDName() );
            }
        }

        LOGGER.trace( "Did I create Source ID {}? {}",
                      this.sourceID,
                      this.performedInsert );
    }

    @Override
    protected Logger getLogger()
    {
        return SourceDetails.LOGGER;
    }

    @Override
	public String toString()
	{
		return "Source: { path: " + this.sourcePath +
			   ", Lead: " + this.lead +
			   ", Hash: " + this.hash + " }";
	}

	public boolean performedInsert()
    {
        return this.performedInsert;
    }

	public static SourceKey createKey( URI sourcePath, String sourceTime, Integer lead, String hash )
	{
	    return new SourceKey( sourcePath, sourceTime, lead, hash);
	}

	public static class SourceKey implements Comparable<SourceKey>
	{
	    public SourceKey( URI sourcePath, String sourceTime, Integer lead, String hash )
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
        
        public URI getSourcePath()
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
	        return obj instanceof SourceKey && obj.hashCode() == this.hashCode();
		}

		@Override
		public int hashCode() {
			return Objects.hash(this.getHash());
		}

		private final URI sourcePath;
	    private final String sourceTime;
	    private final Integer lead;
	    private final String hash;

		@Override
		public String toString()
		{
			return String.format( "Path: %s, lead: %d", this.getSourcePath(), this.getLead()  );
		}
	}
}
