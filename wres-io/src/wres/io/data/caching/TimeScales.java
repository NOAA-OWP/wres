package wres.io.data.caching;

import java.sql.SQLException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.datamodel.scale.TimeScaleOuter;
import wres.io.data.details.TimeScaleDetails;
import wres.io.database.Database;

/**
 * Caches TimeScale rows
 * Based on similar Features class
 */
public class TimeScales
{
    private static final int MAX_DETAILS = 50;
    private final Database database;
    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long, TimeScaleOuter> keyToValue = Caffeine.newBuilder()
                                                                   .maximumSize( MAX_DETAILS )
                                                                   .build();
    private final Cache<TimeScaleOuter, Long> valueToKey = Caffeine.newBuilder()
                                                                   .maximumSize( MAX_DETAILS )
                                                                   .build();

    /**
     * Creates an instance.
     * @param database the database
     */
    public TimeScales( Database database )
    {
        this.database = database;
    }

    /**
     * Mark this instance as only being allowed to read from the database, in other words, not being allowed to add 
     * new time scales, but allowed to look for existing time scales. During ingest, read and create. During retrieval,
     * read only.
     */

    public void setOnlyReadFromDatabase()
    {
        this.onlyReadFromDatabase = true;
    }

    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * Gets a time-scale ID, creating one as needed.
     * @param key the key
     * @return the time-scale ID
     * @throws SQLException if the new ID could not be created
     */
    public Long getOrCreateTimeScaleId( TimeScaleOuter key ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new features, call another method!" );
        }

        Long id = this.valueToKey.getIfPresent( key );

        if ( id == null )
        {
            TimeScaleDetails timeScaleDetails = new TimeScaleDetails( key );
            timeScaleDetails.save( this.getDatabase() );
            id = timeScaleDetails.getId();

            if ( id == null )
            {
                throw new IllegalStateException( "Failed to acquire a time scale identifier for " + key + "." );
            }

            this.valueToKey.put( key, id );
            this.keyToValue.put( id, key );
        }

        return id;
    }
}
