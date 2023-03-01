package wres.io.database.caching;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.io.data.DataProvider;
import wres.io.database.details.MeasurementDetails;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.io.retrieval.DataAccessException;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Jesse Bickel
 * @author Christopher Tubbs
 */
public class MeasurementUnits
{
    private static final int MAX_DETAILS = 100;

    private static final Logger LOGGER = LoggerFactory.getLogger( MeasurementUnits.class );

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long, String> keyToValue = Caffeine.newBuilder()
                                                           .maximumSize( MAX_DETAILS )
                                                           .build();
    private final Cache<String, Long> valueToKey = Caffeine.newBuilder()
                                                           .maximumSize( MAX_DETAILS )
                                                           .build();

    /**
     * Creates an instance.
     * @param database the database
     */
    public MeasurementUnits( Database database )
    {
        this.database = database;
    }

    /**
     * @return the database
     */
    private Database getDatabase()
    {
        return this.database;
    }

    /**
     * Mark this instance as only being allowed to read from the database, in other words, not being allowed to add new 
     * measurement units, but allowed to look for existing measurement units. During ingest, read and create. During 
     * retrieval, read only.
     */

    public void setOnlyReadFromDatabase()
    {
        this.onlyReadFromDatabase = true;
    }

    /**
     * Gets a measurement unit ID, creating one a new one as needed.
     * @param unit the unit
     * @return the measurement unit ID
     * @throws SQLException if the unit ID could not be created
     */
    public Long getOrCreateMeasurementUnitId( String unit ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new units, call another method!" );
        }

        Long id = this.valueToKey.getIfPresent( unit );

        if ( id == null )
        {
            LOGGER.debug( "When attempting to getOrCreateMeasurementUnitId, failed to discover an identifier "
                          + "corresponding to {} in the cache. Adding to the database.",
                          unit );

            MeasurementDetails unitDetails = new MeasurementDetails();
            unitDetails.setUnit( unit );
            unitDetails.save( this.getDatabase() );
            id = unitDetails.getId();

            if ( id == null )
            {
                throw new IllegalStateException( "Failed to acquire a measurement unit identifier for name "
                                                 + unit
                                                 + "." );
            }

            this.valueToKey.put( unit, id );
            this.keyToValue.put( id, unit );
        }

        return id;
    }

    /**
     * @param id the unit ID
     * @return the unit
     */
    public String getUnit( long id )
    {
        String unit = keyToValue.getIfPresent( id );

        if ( unit == null )
        {
            Database db = this.getDatabase();
            DataScripter dataScripter = new DataScripter( db );
            dataScripter.addLine( "SELECT unit_name" );
            dataScripter.addLine( "FROM wres.MeasurementUnit" );
            dataScripter.addLine( "WHERE measurementunit_id = ?" );
            dataScripter.addArgument( id );
            dataScripter.setMaxRows( 1 );
            dataScripter.setUseTransaction( false );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                unit = dataProvider.getString( "unit_name" );
            }
            catch ( SQLException se )
            {
                throw new DataAccessException( "Could not get MeasurementUnit data.",
                                               se );
            }

            this.keyToValue.put( id, unit );
            this.valueToKey.put( unit, id );
        }

        return unit;
    }
}
