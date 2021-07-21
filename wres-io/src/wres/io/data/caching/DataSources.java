package wres.io.data.caching;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.SourceDetails;
import wres.io.data.details.SourceDetails.SourceKey;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.Collections;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class DataSources extends Cache<SourceDetails, SourceKey>
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
            detail.setOutputTime( data.getString( "output_time" ) );
            detail.setSourcePath( URI.create( data.getString( "path" ) ) );
            detail.setHash( data.getString( "hash" ) );
            detail.setIsPointData( data.getBoolean( "is_point_data" ) );
            detail.setID( data.getInt( "source_id" ) );

            this.getKeyIndex().put( detail.getKey(), detail.getId() );
            this.getDetails().put( detail.getId(), detail );
        }
    }

	
	/**
	 * Gets the ID of source metadata from the global cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generated
	 * @param lead the lead time
	 * @param hash the hash code for the source file
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	public Long getSourceID( URI path, String outputTime, Integer lead, String hash )
            throws SQLException
    {
		return this.getID(path, outputTime, lead, hash);
	}

	public SourceDetails get( URI path, String outputTime, Integer lead, String hash )
            throws SQLException
    {
        long id = this.getID( path, outputTime, lead, hash );
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
        script.addLine( "SELECT source_id, path, output_time::text, lead, hash, is_point_data" );
        script.addLine( "FROM wres.Source" );
        script.addLine( "WHERE source_id = ?;" );
        script.addArgument( id );

        SourceDetails notFoundInCache = null;

        try ( DataProvider data = script.getData() )
        {
            while(data.next())
            {
                notFoundInCache = new SourceDetails();
                notFoundInCache.setHash( data.getString( "hash" ) );
                notFoundInCache.setLead( data.getInt( "lead" ) );
                notFoundInCache.setOutputTime( data.getString( "output_time" ) );
                notFoundInCache.setSourcePath( URI.create( data.getString( "path" ) ) );
                notFoundInCache.setID( data.getLong( "source_id" ) );
                notFoundInCache.setIsPointData( data.getBoolean( "is_point_data" ) );

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

    public boolean isCached( SourceDetails.SourceKey key )
    {
        return this.hasID( key );
    }

	public boolean hasSource(String hash) throws SQLException
    {
        return this.getExistingSource( hash ) != null;
    }

    public String getHash( long sourceId )
    {
        String hash = null;

        SourceKey key = Collections.getKeyByValue( this.getKeyIndex(), sourceId );

        if (key != null)
        {
            hash = key.getHash();
        }

        return hash;
    }

    public SourceDetails getExistingSource(final String hash) throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getExistingSource");

        SourceDetails sourceDetails = null;

        SourceKey key = new SourceKey( null, null, null, hash );

        if (this.hasID( key ))
        {
            sourceDetails = this.get(
                    this.getID( key )
            );
        }

        if (sourceDetails == null)
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.setHighPriority( true );

            script.addLine("SELECT source_id, path, output_time::text, lead, hash, is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("WHERE hash = ?;");

            script.addArgument( hash );

            try (DataProvider data = script.getData())
            {
                while(data.next())
                {
                    sourceDetails = new SourceDetails();
                    sourceDetails.setHash( hash );
                    sourceDetails.setLead( data.getInt( "lead" ) );
                    sourceDetails.setOutputTime( data.getString( "output_time" ) );
                    sourceDetails.setSourcePath( URI.create( data.getString( "path" ) ) );
                    sourceDetails.setID( data.getLong( "source_id" ) );
                    sourceDetails.setIsPointData( data.getBoolean( "is_point_data" ) );

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

        SourceKey key = new SourceKey( null, null, null, hash );

        if (this.hasID( key ))
        {
            id = this.getID( key );
        }

        if (id == null)
        {
            Database database = this.getDatabase();
            DataScripter script = new DataScripter( database );
            script.addLine("SELECT source_id, path, output_time::text, lead, hash, is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("WHERE hash = '", hash, "';");

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
	 * Gets the ID of source metadata from the instanced cache based on a file path and the date of its output
	 * @param path The path to the file on the file system
	 * @param outputTime The time in which the information was generation
	 * @param lead the lead time
	 * @param hash the hash code for the source file
	 * @return The ID of the source in the database
	 * @throws SQLException Thrown when interaction with the database failed
	 */
	Long getID( URI path, String outputTime, Integer lead, String hash ) throws SQLException
    {
		return this.getID(SourceDetails.createKey(path, outputTime, lead, hash));
	}
	
	@Override
    public Long getID(SourceKey key) throws SQLException
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
