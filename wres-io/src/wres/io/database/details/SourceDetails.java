package wres.io.database.details;

import java.net.URI;
import java.sql.SQLException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.DataProvider;
import wres.io.database.DataScripter;
import wres.io.database.Database;

/**
 * Details about a time series or raster source of observation or forecast data.
 * @author Christopher Tubbs
 */
public class SourceDetails extends CachedDetail<SourceDetails, String>
{
    /** Prevents asynchronous saving of the same source information. */
    private static final Object SOURCE_SAVE_LOCK = new Object();
    private static final Logger LOGGER = LoggerFactory.getLogger( SourceDetails.class );

    private URI sourcePath = null;
    private Integer lead = null;
    private Long sourceId = null;
    private String hash = null;
    private boolean isPointData = true;
    private Long featureId = null;
    private Long timeScaleId = null;
    private Long measurementUnitId = null;
    private String variableName = null;
    private boolean performedInsert;

    /**
     * Creates an instance.
     */
    public SourceDetails()
    {
    }

    /**
     * Creates an instance with a hash.
     * @param hash the hash
     */
    public SourceDetails( String hash )
    {
        this.setHash( hash );
    }

    /**
     * Sets the path to the source file
     * @param path The path to the source file on the file system
     */
    public void setSourcePath( URI path )
    {
        this.sourcePath = path;
    }

    /**
     * @return the lead duration
     */
    public Integer getLead()
    {
        return this.lead;
    }

    /**
     * Sets the lead duration.
     * @param lead the lead duration
     */
    public void setLead( Integer lead )
    {
        this.lead = lead;
    }

    /**
     * Sets the hash.
     * @param hash the hash
     */
    public void setHash( String hash )
    {
        this.hash = hash;
    }

    /**
     * Sets the point data status.
     * @param isPointData whether the data is point data
     */
    public void setIsPointData( boolean isPointData )
    {
        this.isPointData = isPointData;
    }

    /**
     * @return the hash
     */
    public String getHash()
    {
        return this.hash;
    }

    /**
     * @return whether the data is point data
     */
    public boolean getIsPointData()
    {
        return this.isPointData;
    }

    /**
     * @return the source path
     */
    public URI getSourcePath()
    {
        return this.sourcePath;
    }

    /**
     * @return the feature ID
     */
    public Long getFeatureId()
    {
        return featureId;
    }

    /**
     * Sets the feature ID.
     * @param featureId the feature ID
     */
    public void setFeatureId( Long featureId )
    {
        this.featureId = featureId;
    }

    /**
     * @return the time scale ID
     */
    public Long getTimeScaleId()
    {
        return timeScaleId;
    }

    /**
     * Sets the time scale ID.
     * @param timeScaleId the time scale ID
     */
    public void setTimeScaleId( Long timeScaleId )
    {
        this.timeScaleId = timeScaleId;
    }

    /**
     * @return the measurement unit ID
     */
    public Long getMeasurementUnitId()
    {
        return measurementUnitId;
    }

    /**
     * Sets the measruement unit ID.
     * @param measurementUnitId the measruement unit ID
     */
    public void setMeasurementUnitId( Long measurementUnitId )
    {
        this.measurementUnitId = measurementUnitId;
    }

    /**
     * @return trhe variable name
     */
    public String getVariableName()
    {
        return variableName;
    }

    /**
     * Sets the variable name.
     * @param variableName the variable name
     */
    public void setVariableName( String variableName )
    {
        this.variableName = variableName;
    }

    @Override
    public int compareTo( @NotNull SourceDetails other )
    {
        Long id = this.sourceId;

        if ( id == null )
        {
            id = -1L;
        }

        return id.compareTo( other.getId() );
    }

    @Override
    public String getKey()
    {
        if ( this.getHash() == null )
        {
            throw new IllegalStateException( "There was no key, it was null." );
        }

        return this.hash;
    }

    @Override
    public Long getId()
    {
        return this.sourceId;
    }

    @Override
    protected String getIDName()
    {
        return "source_id";
    }

    @Override
    public void setID( long id )
    {
        this.sourceId = id;
    }

    @Override
    protected DataScripter getInsertSelect( Database database )
    {
        DataScripter script = new DataScripter( database );

        script.setUseTransaction( true );

        script.retryOnSerializationFailure();
        script.retryOnUniqueViolation();

        script.setHighPriority( true );

        String insertStatement =
                "INSERT INTO wres.Source ( path, lead, hash, is_point_data, feature_id, timescale_id, measurementunit_id, variable_name )";
        script.addLine( insertStatement );
        script.addTab()
              .addLine( "SELECT ?, ?, ?, ?, ?, ?, ?, ?" );

        if ( this.getSourcePath() != null )
        {
            script.addArgument( this.getSourcePath()
                                    .toString() );
        }
        else
        {
            script.addArgument( null );
        }

        script.addArgument( this.lead );
        script.addArgument( this.getHash() );
        script.addArgument( this.getIsPointData() );
        script.addArgument( this.getFeatureId() );
        script.addArgument( this.getTimeScaleId() );
        script.addArgument( this.getMeasurementUnitId() );
        script.addArgument( this.getVariableName() );

        script.addTab().addLine( "WHERE NOT EXISTS" );
        script.addTab().addLine( "(" );
        script.addTab( 2 )
              .addLine( "SELECT 1" );
        script.addTab( 2 )
              .addLine( "FROM wres.Source" );
        script.addTab( 2 )
              .addLine( "WHERE hash = ?" );

        script.addArgument( this.getHash() );

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
            this.sourceId = script.getInsertedIds()
                                  .get( 0 );
        }
        else
        {
            DataScripter scriptWithId = new DataScripter( database );
            scriptWithId.setHighPriority( true );
            scriptWithId.setUseTransaction( false );
            scriptWithId.add( "SELECT " )
                        .addLine( this.getIDName() );
            scriptWithId.addLine( "FROM wres.Source" );
            scriptWithId.addLine( "WHERE hash = ? " );
            scriptWithId.addArgument( this.hash );
            scriptWithId.setMaxRows( 1 );

            try ( DataProvider data = scriptWithId.getData() )
            {
                this.sourceId = data.getLong( this.getIDName() );
            }
        }

        LOGGER.trace( "Did I create Source Id {}? {}",
                      this.sourceId,
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

    /**
     * @return whether the insert has been performed
     */
    public boolean performedInsert()
    {
        return this.performedInsert;
    }
}
