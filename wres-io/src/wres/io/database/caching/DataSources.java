package wres.io.database.caching;

import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;

import wres.datamodel.DataProvider;
import wres.io.database.details.SourceDetails;
import wres.io.database.DataScripter;
import wres.io.database.Database;

/**
 * Caches information about the source of forecast and observation data
 * @author James Brown
 * @author Christopher Tubbs
 */
public class DataSources
{
    private static final String HASH = "hash";
    private static final String PATH = "path";
    private static final String VARIABLE_NAME = "variable_name";
    private static final String TIMESCALE_ID = "timescale_id";
    private static final String SOURCE_ID = "source_id";
    private static final String MEASUREMENTUNIT_ID = "measurementunit_id";
    private static final String IS_POINT_DATA = "is_point_data";
    private static final String FEATURE_ID = "feature_id";
    private static final int MAX_DETAILS = 10000;

    private static final Logger LOGGER = LoggerFactory.getLogger( DataSources.class );

    private final Cache<String, Long> keyIndex = Caffeine.newBuilder()
                                                         .maximumSize( MAX_DETAILS )
                                                         .build();
    private final Cache<Long, SourceDetails> details = Caffeine.newBuilder()
                                                               .maximumSize( MAX_DETAILS )
                                                               .build();

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Database database;

    /**
     * Creates an instance.
     * @param database the database
     * @throws NullPointerException if the input is null
     */

    public DataSources( Database database )
    {
        Objects.requireNonNull( database );
        this.database = database;
    }

    /**
     * Creates an instance
     * @param database the database
     * @param data the data source
     * @throws NullPointerException if either input is null
     */

    public DataSources( Database database,
                        DataProvider data )
    {
        this( database );

        Objects.requireNonNull( data );

        SourceDetails detail;

        while ( data.next() )
        {
            detail = new SourceDetails();
            detail.setSourcePath( URI.create( data.getString( PATH ) ) );
            detail.setHash( data.getString( HASH ) );
            detail.setIsPointData( data.getBoolean( IS_POINT_DATA ) );
            detail.setID( data.getLong( SOURCE_ID ) );
            detail.setVariableName( data.getString( VARIABLE_NAME ) );
            detail.setMeasurementUnitId( data.getLong( MEASUREMENTUNIT_ID ) );
            detail.setTimeScaleId( data.getLong( TIMESCALE_ID ) );
            detail.setFeatureId( data.getLong( FEATURE_ID ) );

            this.getKeyIndex()
                .put( detail.getKey(),
                      detail.getId() );
            this.getDetails()
                .put( detail.getId(),
                      detail );
        }
    }

    /**
     * Gets the source details corresponding to the source hash.
     * @param hash the hash
     * @return the source details
     * @throws SQLException if the source could not be found
     * @throws NullPointerException if the input is null
     */

    public SourceDetails get( String hash ) throws SQLException
    {
        Objects.requireNonNull( hash );

        long id = this.getId( hash );
        return this.get( id );
    }

    /**
     * Gets the source details from the corresponding source identifier.
     * @param id the source identifier
     * @return the source details
     * @throws NullPointerException if the input is null
     */

    public SourceDetails getById( Long id )
    {
        Objects.requireNonNull( id );

        return this.get( id );
    }

    /**
     * Gets the ID of source metadata from the instanced cache based on identity
     * @param key the hash code for the source file
     * @return The ID of the source in the database
     * @throws SQLException Thrown when interaction with the database failed
     * @throws NullPointerException if the input is null
     */

    public Long getId( String key ) throws SQLException
    {
        Objects.requireNonNull( key );

        if ( !this.hasId( key ) )
        {
            this.addToDatabaseAndCache( new SourceDetails( key ) );
        }

        Long id;
        
        LOGGER.debug( "Getting data source with hash {} from the data sources cache.", key );

        synchronized ( this.getKeyLock() )
        {
            Cache<String, Long> keyMap = this.getKeyIndex();
            id = keyMap.getIfPresent( key );
        }

        return id;
    }

    /**
     * Gets an existing source based on the source identifier and adds to the cache if required.
     * @param id the source identifier
     * @return the source details
     * @throws SQLException if an error occurs when inspecting the database
     * @throws NullPointerException if the input is null
     */

    public SourceDetails getSource( Long id )
            throws SQLException
    {
        Objects.requireNonNull( id );

        SourceDetails foundInCache = this.get( id );
        if ( Objects.nonNull( foundInCache ) )
        {
            LOGGER.debug( "Found a data source with identifier {} in the data sources cache.", id );

            return foundInCache;
        }

        LOGGER.debug( "Looking in the database for a data source with identifier {}.", id );

        Database innerDatabase = this.getDatabase();
        DataScripter script = new DataScripter( innerDatabase );
        script.setHighPriority( true );
        script.addLine( "SELECT source_id, path, lead, hash, is_point_data, feature_id, timescale_id, "
                        + "measurementunit_id, variable_name" );
        script.addLine( "FROM wres.Source" );
        script.addLine( "WHERE source_id = ?" );
        script.addArgument( id );
        script.setMaxRows( 1 );

        SourceDetails notFoundInCache = null;

        try ( DataProvider data = script.getData() )
        {
            while ( data.next() )
            {
                notFoundInCache = new SourceDetails();
                notFoundInCache.setHash( data.getString( HASH ) );
                notFoundInCache.setLead( data.getInt( "lead" ) );
                notFoundInCache.setSourcePath( URI.create( data.getString( PATH ) ) );
                notFoundInCache.setID( data.getLong( SOURCE_ID ) );
                notFoundInCache.setIsPointData( data.getBoolean( IS_POINT_DATA ) );
                notFoundInCache.setMeasurementUnitId( data.getLong( MEASUREMENTUNIT_ID ) );
                notFoundInCache.setTimeScaleId( data.getLong( TIMESCALE_ID ) );
                notFoundInCache.setFeatureId( data.getLong( FEATURE_ID ) );
                notFoundInCache.setVariableName( data.getString( VARIABLE_NAME ) );

                this.addToDatabaseAndCache( notFoundInCache );
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

    /**
     * Gets an existing source based on the source hash and adds to the cache if required.
     * @param hash the source hash
     * @return the source details
     * @throws SQLException if an error occurs when inspecting the database
     * @throws NullPointerException if the input is null
     */

    public SourceDetails getSource( String hash ) throws SQLException
    {
        Objects.requireNonNull( hash );

        SourceDetails sourceDetails = null;

        if ( this.hasId( hash ) )
        {
            LOGGER.debug( "Found a data source with hash {} in the data sources cache.", hash );

            Long id = this.getId( hash );
            sourceDetails = this.get( id );
        }

        LOGGER.debug( "Looking in the database for a data source with hash {}.", hash );

        if ( sourceDetails == null )
        {
            Database innerDatabase = this.getDatabase();
            DataScripter script = new DataScripter( innerDatabase );
            script.setHighPriority( true );

            script.addLine( "SELECT source_id, path, lead, hash, is_point_data, feature_id, timescale_id, measurementunit_id, variable_name" );
            script.addLine( "FROM wres.Source" );
            script.addLine( "WHERE hash = ?" );
            script.addArgument( hash );
            script.setMaxRows( 1 );

            try ( DataProvider data = script.getData() )
            {
                while ( data.next() )
                {
                    sourceDetails = new SourceDetails();
                    sourceDetails.setHash( hash );
                    sourceDetails.setLead( data.getInt( "lead" ) );
                    sourceDetails.setSourcePath( URI.create( data.getString( PATH ) ) );
                    sourceDetails.setID( data.getLong( SOURCE_ID ) );
                    sourceDetails.setIsPointData( data.getBoolean( IS_POINT_DATA ) );
                    sourceDetails.setMeasurementUnitId( data.getLong( MEASUREMENTUNIT_ID ) );
                    sourceDetails.setTimeScaleId( data.getLong( TIMESCALE_ID ) );
                    sourceDetails.setFeatureId( data.getLong( FEATURE_ID ) );
                    sourceDetails.setVariableName( data.getString( VARIABLE_NAME ) );

                    this.addToDatabaseAndCache( sourceDetails );
                }
            }
        }

        return sourceDetails;
    }

    /**
     * Gets the source identifier corresponding to the source hash.
     * @param hash the hash
     * @return the source identifier
     * @throws SQLException if an error was encountered when checking the database
     */

    public Long getSourceId( String hash )
            throws SQLException
    {
        return this.getSource( hash )
                   .getId();
    }

    /**
     * @return the database
     */

    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * @return the details lock
     */

    private Object getDetailLock()
    {
        return this.detailLock;
    }

    /**
     * @return the key lock
     */

    private Object getKeyLock()
    {
        return this.keyLock;
    }

    /**
     * @return the details
     */

    private Cache<Long, SourceDetails> getDetails()
    {
        return this.details;
    }

    /**
     * <p>Invalidates the cached details.
     * 
     * <p>See #61206.
     * @param details the details whose corresponding cached record should be invalidated
     */

    public void invalidate( SourceDetails details )
    {
        synchronized ( this.getDetailLock() )
        {
            LOGGER.debug( "Invalidating the data sources cache for {}.", details );

            this.details.invalidate( details.getId() );

            synchronized ( this.getKeyLock() )
            {
                this.keyIndex.invalidate( details.getKey() );
            }
        }
    }

    /**
     * @return the key index cache
     */

    private Cache<String, Long> getKeyIndex()
    {
        return this.keyIndex;
    }

    /**
     * @param id the identifier key
     * @return the mapped value
     */
    private SourceDetails get( long id )
    {
        LOGGER.debug( "Getting data source with identifier {} from the data sources cache.", id );
        
        return this.getDetails()
                   .getIfPresent( id );
    }

    /**
     * Adds the details to the instance cache. If the details don't exist in the database, they are added.
     * <br><br>
     * Since only a limited amount of data is stored within the instanced cache, the least recently used item from the
     * instanced cache is removed if the amount surpasses the maximum allowable number of stored details
     * @param element The details to add to the instanced cache
     * @throws SQLException Thrown if the ID of the element could not be retrieved or the cache could not be
     * updated
     */
    private void addToDatabaseAndCache( SourceDetails element ) throws SQLException
    {
        LOGGER.debug( "Adding data source {} to the database.", element );

        Database innerDatabase = this.getDatabase();
        element.save( innerDatabase );
        this.addToCache( element );
    }

    /**
     * Adds the source details to the cache
     * @param element the source details to add
     */

    private void addToCache( SourceDetails element )
    {
        synchronized ( this.getKeyLock() )
        {
            LOGGER.debug( "Adding data source {} to the data sources cache.", element );

            this.getKeyIndex()
                .put( element.getKey(), element.getId() );

            SourceDetails sourceDetails = this.details.getIfPresent( element.getId() );
            if ( Objects.isNull( sourceDetails ) )
            {
                this.getDetails()
                    .put( element.getId(), element );
            }
        }
    }

    /**
     * @param key the key
     * @return whether the key is contained in the cache
     */

    private boolean hasId( String key )
    {
        boolean hasIt;

        synchronized ( this.getKeyLock() )
        {
            Cache<String, Long> keyMap = this.getKeyIndex();
            Long id = keyMap.getIfPresent( key );
            hasIt = Objects.nonNull( id );
        }

        return hasIt;
    }

}
