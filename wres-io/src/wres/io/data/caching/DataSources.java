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
import wres.util.Collections;

/**
 * Caches information about the source of forecast and observation data
 * @author Christopher Tubbs
 */
public class DataSources extends Cache<SourceDetails, SourceKey>
{
    private static final int MAX_DETAILS = 10000;
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSources.class);
    private static final Object CACHE_LOCK = new Object();

    private static final Object DETAIL_LOCK = new Object();
    private static final Object KEY_LOCK = new Object();

    @Override
    protected Object getDetailLock()
    {
        return DataSources.DETAIL_LOCK;
    }

    @Override
    protected Object getKeyLock()
    {
        return DataSources.KEY_LOCK;
    }

    /**
     * Cache of basic source data
     */
	private static final DataSources INSTANCE = new DataSources();

	/**
	 * <p>Invalidates the global cache of the singleton associated with this class, {@link #INSTANCE}.
	 * 
	 * <p>See #61206.
	 */
	   
    public static void invalidateGlobalCache()
    {
        DataSources.INSTANCE.invalidate();
    }
	
	public DataSources()
    {
        this.initializeDetails();
    }

    private void populate(final DataProvider data)
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

	private static DataSources getCache()
    {
        synchronized (CACHE_LOCK)
        {
            if ( INSTANCE.isEmpty())
            {
                DataSources.initialize();
            }
            return INSTANCE;
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
	public static Integer getSourceID( URI path, String outputTime, Integer lead, String hash )
            throws SQLException
    {
		return getCache().getID(path, outputTime, lead, hash);
	}

	public static SourceDetails get( URI path, String outputTime, Integer lead, String hash )
            throws SQLException
    {
        int id = DataSources.getCache().getID( path, outputTime, lead, hash );
        return DataSources.getCache().get( id );
    }

    public static SourceDetails getById(Integer id)
    {
        return DataSources.getCache().get( id );
    }

    public static SourceDetails getFromCacheOrDatabaseByIdThenCache( Integer id )
            throws SQLException
    {
        SourceDetails foundInCache = DataSources.getCache()
                                                .get( id );
        if ( Objects.nonNull( foundInCache) )
        {
            return foundInCache;
        }

        DataScripter script = new DataScripter();
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
                notFoundInCache.setID( data.getInt( "source_id" ) );
                notFoundInCache.setIsPointData( data.getBoolean( "is_point_data" ) );

                DataSources.getCache().addElement( notFoundInCache );
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

    public static boolean isCached( SourceDetails.SourceKey key )
    {
        return DataSources.getCache()
                          .hasID( key );
    }

	public static boolean hasSource(String hash) throws SQLException
    {
        return DataSources.getExistingSource( hash ) != null;
    }

    public static String getHash(int sourceId)
    {
        String hash = null;

        SourceKey key = Collections.getKeyByValue( DataSources.getCache().getKeyIndex(), sourceId );

        if (key != null)
        {
            hash = key.getHash();
        }

        return hash;
    }

    public static SourceDetails getExistingSource(final String hash) throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getExistingSource");

        SourceDetails sourceDetails = null;

        SourceKey key = new SourceKey( null, null, null, hash );

        if (DataSources.getCache().hasID( key ))
        {
            sourceDetails = DataSources.getCache().get(
                    DataSources.getCache().getID( key )
            );
        }

        if (sourceDetails == null)
        {
            DataScripter script = new DataScripter(  );
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
                    sourceDetails.setID( data.getInt( "source_id" ) );
                    sourceDetails.setIsPointData( data.getBoolean( "is_point_data" ) );

                    DataSources.getCache().addElement( sourceDetails );
                }
            }
        }

        return sourceDetails;
    }

    public static Integer getActiveSourceID(String hash)
            throws SQLException
    {
        Objects.requireNonNull(hash, "A nonexistent hash was passed to DataSources#getActiveSourceID");

        Integer id = null;

        SourceKey key = new SourceKey( null, null, null, hash );

        if (DataSources.getCache().hasID( key ))
        {
            id = DataSources.getCache().getID( key );
        }

        if (id == null)
        {
            DataScripter script = new DataScripter(  );
            script.addLine("SELECT source_id, path, output_time::text, lead, hash, is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("WHERE hash = '", hash, "';");

            try (DataProvider data = script.getData())
            {
                SourceDetails details;

                if ( data.next() )
                {
                    details = new SourceDetails(data);

                    DataSources.getCache().addElement( details );

                    id = data.getInt( "source_id" );
                }
            }
        }

        return id;
    }

    public static void put( SourceDetails sourceDetails )
    {
        DataSources.getCache().add( sourceDetails );
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
	Integer getID( URI path, String outputTime, Integer lead, String hash ) throws SQLException
    {
		return this.getID(SourceDetails.createKey(path, outputTime, lead, hash));
	}
	
	@Override
    public Integer getID(SourceKey key) throws SQLException
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

	private static void initialize()
    {
        try
        {
            DataScripter script = new DataScripter(  );

            script.addLine("SELECT source_id,");
            script.addTab().addLine("path,");
            script.addTab().addLine("CAST(output_time AS TEXT) AS output_time,");
            script.addTab().addLine("hash,");
            script.addTab().addLine("is_point_data");
            script.addLine("FROM wres.Source");
            script.addLine("LIMIT ", MAX_DETAILS, ";");

            try (DataProvider sources = script.getData())
            {
                INSTANCE.populate( sources );
            }
            
            LOGGER.debug( "Finished populating the DataSource details." );
        }
        catch (SQLException error)
        {
            // Failure to pre-populate cache should not affect primary outputs.
            LOGGER.warn( "An error was encountered when trying to populate the Source cache.",
                         error );
        }
    }
}
