package wres.io.data.caching;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.Collections;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class DataSources extends Cache<SourceDetails, String>
{
    private static final int MAX_DETAILS = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSources.class);

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Database database;

    public DataSources( Database database )
    {
        this.database = database;
    }

    @Override
    protected Database getDatabase()
    {
        return this.database;
    }

    @Override
    protected Object getDetailLock()
    {
        return this.detailLock;
    }

    @Override
    protected Object getKeyLock()
    {
        return this.keyLock;
    }


    void populate(final DataProvider data)
    {
        if (data == null)
        {
            LOGGER.warn("The DataSources cache was populated with no data.");
            return;
        }

        SourceDetails detail;

        while ( data.next() )
        {
            detail = new SourceDetails();
            detail.setSourcePath( URI.create( data.getString( "path" ) ) );
            detail.setHash( data.getString( "hash" ) );
            detail.setIsPointData( data.getBoolean( "is_point_data" ) );
            detail.setID( data.getLong( "source_id" ) );
            detail.setVariableName( data.getString( "variable_name" ) );
            detail.setMeasurementUnitId( data.getLong( "measurementunit_id" ) );
            detail.setTimeScaleId( data.getLong( "timescale_id" ) );
            detail.setFeatureId( data.getLong( "feature_id" ) );

            this.getKeyIndex().put( detail.getKey(), detail.getId() );
            this.getDetails().put( detail.getId(), detail );
        }
    }

	
	/**
	 * Gets the ID of source metadata from the global cache based on a file path and the date of its output
	 * @param hash the hash code for the source file
	 * @return The ID of the source in the database
	 */
	public Long getSourceID( String hash ) throws SQLException
    {
		return this.getID( hash);
	}

	public SourceDetails get( String hash ) throws SQLException
    {
        long id = this.getID( hash );
        return this.get( id );
    }

    public SourceDetails getById( Long id )
    {
        return this.get( id );
    }

    public SourceDetails getFromCacheOrDatabaseByIdThenCache( Long id )
            throws SQLException
    {
        SourceDetails foundInCache = this.get( id );
        if ( Objects.nonNull( foundInCache) )
        {
            return foundInCache;
        }

        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.setHighPriority( true );
        script.addLine( "SELECT source_id, path, lead, hash, is_point_data, feature_id, timescale_id, measurementunit_id, variable_name" );
        script.addLine( "FROM wres.Source" );
        script.addLine( "WHERE source_id = ?" );
        script.addArgument( id );
        script.setMaxRows( 1 );

        SourceDetails notFoundInCache = null;

        try ( DataProvider data = script.getData() )
        {
            while(data.next())
            {
                notFoundInCache = new SourceDetails();
                notFoundInCache.setHash( data.getString( "hash" ) );
                notFoundInCache.setLead( data.getInt( "lead" ) );
                notFoundInCache.setSourcePath( URI.create( data.getString( "path" ) ) );
                notFoundInCache.setID( data.getLong( "source_id" ) );
                notFoundInCache.setIsPointData( data.getBoolean( "is_point_data" ) );
                notFoundInCache.setMeasurementUnitId( data.getLong( "measurementunit_id" ) );
                notFoundInCache.setTimeScaleId( data.getLong( "timescale_id" ) );
                notFoundInCache.setFeatureId( data.getLong( "feature_id" ) );
                notFoundInCache.setVariableName( data.getString( "variable_name" ) );

                this.addElement( notFoundInCache );
            }
        }

        if ( Objects.isNull( notFoundInCache ) )
        {
            throw new IllegalArgumentException( "Unable to find source_id '"
                                                + id
                                                + "' in this database instance." );
        }

        return notFoundInCache;
    }

    public boolean isCached( String key )
    {
        return this.hasID( key );
    }

	public boolean hasSource(String hash) throws SQLException
    {
        return this.getExistingSource( hash ) != null;
    }

    public String getHash( long sourceId )
    {
        return Collections.getKeyByValue( this.getKeyIndex(), sourceId );
    }

    public SourceDetails getExistingSource(final String hash) throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getExistingSource");

        SourceDetails sourceDetails = null;

        if (this.hasID( hash ))
        {
            sourceDetails = this.get(
                    this.getID( hash )
            );
        }

        if (sourceDetails == null)
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.setHighPriority( true );

            script.addLine( "SELECT source_id, path, lead, hash, is_point_data, feature_id, timescale_id, measurementunit_id, variable_name" );
            script.addLine("FROM wres.Source");
            script.addLine("WHERE hash = ?");
            script.addArgument( hash );
            script.setMaxRows( 1 );

            try (DataProvider data = script.getData())
            {
                while(data.next())
                {
                    sourceDetails = new SourceDetails();
                    sourceDetails.setHash( hash );
                    sourceDetails.setLead( data.getInt( "lead" ) );
                    sourceDetails.setSourcePath( URI.create( data.getString( "path" ) ) );
                    sourceDetails.setID( data.getLong( "source_id" ) );
                    sourceDetails.setIsPointData( data.getBoolean( "is_point_data" ) );
                    sourceDetails.setMeasurementUnitId( data.getLong( "measurementunit_id" ) );
                    sourceDetails.setTimeScaleId( data.getLong( "timescale_id" ) );
                    sourceDetails.setFeatureId( data.getLong( "feature_id" ) );
                    sourceDetails.setVariableName( data.getString( "variable_name" ) );

                    this.addElement( sourceDetails );
                }
            }
        }

        return sourceDetails;
    }

    public Long getActiveSourceID( String hash )
            throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getActiveSourceID");

        Long id = null;

        if ( this.hasID( hash ) )
        {
            id = this.getID( hash );
        }

        if (id == null)
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.addLine( "SELECT source_id, path, lead, hash, is_point_data, feature_id, timescale_id, measurementunit_id, variable_name" );
            script.addLine("FROM wres.Source");
            script.addLine( "WHERE hash = ?" );
            script.addArgument( hash );
            script.setMaxRows( 1 );

            try (DataProvider data = script.getData())
            {
                SourceDetails details;

                if ( data.next() )
                {
                    details = new SourceDetails(data);

                    this.addElement( details );

                    id = data.getLong( "source_id" );
                }
            }
        }

        return id;
    }

    public void put( SourceDetails sourceDetails )
    {
        this.add( sourceDetails );
    }
	
	/**
	 * Gets the ID of source metadata from the instanced cache based on identity
	 * @param key the hash code for the source file
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	@Override
    public Long getID( String key ) throws SQLException
    {
	    if (!this.hasID(key))
	    {
	        addElement(new SourceDetails(key));
	    }

	    return super.getID(key);
	}

	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}

}
