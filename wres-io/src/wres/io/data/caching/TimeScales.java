package wres.io.data.caching;

import java.sql.SQLException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.io.data.details.TimeScaleDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Caches TimeScale rows
 * Based on similar Features class
 */
public class TimeScales
{
    private static final int MAX_DETAILS = 50;

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long,TimeScaleOuter> keyToValue = Caffeine.newBuilder()
                                                                  .maximumSize( MAX_DETAILS )
                                                                  .build();
    private final Cache<TimeScaleOuter,Long> valueToKey = Caffeine.newBuilder()
                                                                  .maximumSize( MAX_DETAILS )
                                                                  .build();

    public TimeScales( Database database )
    {
        this.database = database;
    }

    /**
     * Mark this instance as only being allowed to read from the database, in
     * other words, not being allowed to add new features, but allowed to look
     * for existing features. During ingest, read and create. During retrieval,
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
                throw new IllegalStateException( "Issue getting id from TimeScaleDetails" );
            }

            this.valueToKey.put( key, id );
            this.keyToValue.put( id, key );
        }

        return id;
    }


    /**
     * Given a db row id aka surrogate key, find the TimeScaleOuter values for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * Also keeps in cache when found. This is a read-only-from-db operation.
     * @param timeScaleId the db-instance-specific surrogate key for the feature
     * @return the existing or new TimeScaleOuter
     * @throws SQLException when communication with the database fails.
     */

    public TimeScaleOuter getTimeScaleOuter( long timeScaleId )
            throws SQLException
    {
        TimeScaleOuter value = this.keyToValue.getIfPresent( timeScaleId );

        if ( value == null )
        {
            // Not found above, gotta find it.
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
            dataScripter.addLine( "SELECT duration_ms, function_name" );
            dataScripter.addLine( "FROM wres.TimeScale" );
            dataScripter.addLine( "WHERE timescale_id = ?" );
            dataScripter.addArgument( timeScaleId );
            dataScripter.setMaxRows( 1 );
            dataScripter.setUseTransaction( false );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                Long timescaleDurationMs = dataProvider.getLong( "duration_ms" );
                String functionRaw = dataProvider.getString( "function_name" );
                TimeScaleFunction function = TimeScaleFunction.valueOf( functionRaw );
                Duration duration = Duration.ofMillis( timescaleDurationMs );

                value = TimeScaleOuter.of( duration, function );
            }

            this.keyToValue.put( timeScaleId, value );
            this.valueToKey.put( value, timeScaleId );
        }

        return value;
    }

    /**
     * Given a TimeScaleOuter with values, find the db surrogate key for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * Also keeps in cache when found. This is a read-only-from-db operation.
     * @param timeScale the feature data
     * @return the id of the feature
     * @throws SQLException when communication with the database fails.
     */

    public Long getTimeScaleId( TimeScaleOuter timeScale )
            throws SQLException
    {
        Long id = this.valueToKey.getIfPresent( timeScale );

        if ( id == null )
        {
            // Not found above, gotta find it.
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
            dataScripter.addLine( "SELECT timescale_id" );
            dataScripter.addLine( "FROM wres.TimeScale" );
            dataScripter.addLine( "WHERE function_name = ?" );
            dataScripter.addArgument( timeScale.getFunction()
                                               .toString() );
            Duration duration = timeScale.getPeriod();
            Long durationInMs = duration.toMillis();
            dataScripter.addTab().addLine( "AND duration_ms = ?" );
            dataScripter.addArgument( durationInMs );

            dataScripter.setMaxRows( 1 );
            dataScripter.setUseTransaction( false );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                id = dataProvider.getLong( "timescale_id" );
            }

            this.keyToValue.put( id, timeScale );
            this.valueToKey.put( timeScale, id );
        }

        return id;
    }
}
