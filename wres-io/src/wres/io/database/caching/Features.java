package wres.io.database.caching;

import java.sql.SQLException;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.Feature;
import wres.io.data.DataProvider;
import wres.io.database.details.FeatureDetails;
import wres.io.database.DataScripter;
import wres.io.database.Database;
import wres.statistics.generated.Geometry;

/**
 * Caches details about Features
 * @author Jesse Bickel
 * @author Christopher Tubbs
 * @author James Brown
 */
public class Features
{
    private static final int MAX_DETAILS = 5000;

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long, Feature> keyToValue = Caffeine.newBuilder()
                                                               .maximumSize( MAX_DETAILS )
                                                               .build();
    private final Cache<Feature, Long> valueToKey = Caffeine.newBuilder()
                                                               .maximumSize( MAX_DETAILS )
                                                               .build();

    /**
     * Creates an instance.
     * @param database the database
     */

    public Features( Database database )
    {
        this.database = database;
    }

    /**
     * Mark this instance as only being allowed to read from the database, in other words, not being allowed to add new 
     * features, but allowed to look for existing features. During ingest, read and create. During retrieval, read only.
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
     * Returns a feature identifier for the input, creating one as needed.
     * @param key the key
     * @return the feature identifier
     * @throws SQLException if the identifier could not be created
     */

    public Long getOrCreateFeatureId( Feature key ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new features, call another method!" );
        }

        Long id = this.valueToKey.getIfPresent( key );

        if ( id == null )
        {
            FeatureDetails featureDetails = new FeatureDetails( key );
            featureDetails.save( this.getDatabase() );
            id = featureDetails.getId();

            if ( id == null )
            {
                throw new IllegalStateException( "Issue getting id from FeatureDetails" );
            }

            this.valueToKey.put( key, id );
            this.keyToValue.put( id, key );
        }

        return id;
    }


    /**
     * Given a db row id aka surrogate key, find the FeatureKey values for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * Also keeps in cache when found. This is a read-only-from-db operation.
     * @param featureId the db-instance-specific surrogate key for the feature
     * @return the existing or new FeatureKey
     * @throws SQLException when communication with the database fails.
     */

    public Feature getFeatureKey( long featureId )
            throws SQLException
    {
        Feature value = this.keyToValue.getIfPresent( featureId );

        if ( value == null )
        {
            // Not found above, gotta find it.
            Database db = this.getDatabase();
            DataScripter dataScripter = new DataScripter( db );
            dataScripter.addLine( "SELECT name, description, srid, wkt" );
            dataScripter.addLine( "FROM wres.Feature" );
            dataScripter.addLine( "WHERE feature_id = ?" );
            dataScripter.addArgument( featureId );
            dataScripter.setMaxRows( 1 );
            dataScripter.setUseTransaction( false );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                String name = dataProvider.getString( "name" );
                String description = dataProvider.getString( "description" );
                Integer srid = dataProvider.getInt( "srid" );
                String wkt = dataProvider.getString( "wkt" );
                Geometry geometry = MessageFactory.getGeometry( name, description, srid, wkt );
                value = Feature.of( geometry );
            }

            this.keyToValue.put( featureId, value );
            this.valueToKey.put( value, featureId );
        }

        return value;
    }

    /**
     * Given a FeatureKey with values, find the db surrogate key for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * Also keeps in cache when found. This is a read-only-from-db operation.
     * @param featureKey the feature data
     * @return the id of the feature
     * @throws SQLException when communication with the database fails.
     */

    public Long getFeatureId( Feature featureKey )
            throws SQLException
    {
        Long id = this.valueToKey.getIfPresent( featureKey );

        if ( id == null )
        {
            // Not found above, gotta find it.
            Database db = this.getDatabase();
            DataScripter dataScripter = new DataScripter( db );
            dataScripter.addLine( "SELECT feature_id" );
            dataScripter.addLine( "FROM wres.Feature" );
            dataScripter.addLine( "WHERE name = ?" );
            dataScripter.addArgument( featureKey.getName() );
            dataScripter.addTab().addLine( "AND description = ?" );
            dataScripter.addArgument( featureKey.getDescription() );
            dataScripter.addTab().addLine( "AND srid = ?" );
            dataScripter.addArgument( featureKey.getSrid() );
            dataScripter.addTab().addLine( "AND wkt = ?" );
            dataScripter.addArgument( featureKey.getWkt() );
            dataScripter.setMaxRows( 1 );
            dataScripter.setUseTransaction( false );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                id = dataProvider.getLong( "feature_id" );
            }

            this.keyToValue.put( id, featureKey );
            this.valueToKey.put( featureKey, id );
        }

        return id;
    }

}
