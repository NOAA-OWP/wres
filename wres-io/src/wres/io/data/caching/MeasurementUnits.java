package wres.io.data.caching;

import java.sql.SQLException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.io.data.details.MeasurementDetails;
import wres.io.retrieval.DataAccessException;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Caches details mapping units of measurements to their IDs
 * @author Christopher Tubbs
 */
public class MeasurementUnits
{
    private static final int MAX_DETAILS = 100;
    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long,String> keyToValue = Caffeine.newBuilder()
                                                          .maximumSize( MAX_DETAILS )
                                                          .build();
    private final Cache<String,Long> valueToKey = Caffeine.newBuilder()
                                                          .maximumSize( MAX_DETAILS )
                                                          .build();

    public MeasurementUnits( Database database )
    {
        this.database = database;
    }

    private Database getDatabase()
    {
        return this.database;
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


    public Long getOrCreateMeasurementUnitId( String unit ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new units, call another method!" );
        }

        Long id = this.valueToKey.getIfPresent( unit );

        if ( id == null )
        {
            MeasurementDetails unitDetails = new MeasurementDetails();
            unitDetails.setUnit( unit );
            unitDetails.save( this.getDatabase() );
            id = unitDetails.getId();

            if ( id == null )
            {
                throw new IllegalStateException( "Issue getting id from FeatureDetails" );
            }

            this.valueToKey.put( unit, id );
            this.keyToValue.put( id, unit );
        }

        return id;
    }


    public String getUnit( long id )
    {
        String unit = keyToValue.getIfPresent( id );

        if ( unit == null )
        {
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
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
