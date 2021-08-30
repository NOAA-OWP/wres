package wres.io.data.caching;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import ucar.nc2.NetcdfFile;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.space.FeatureTuple;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
public class Features
{
    private static final Logger LOGGER = LoggerFactory.getLogger( Features.class );
    
    private static final int MAX_DETAILS = 5000;

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long,FeatureKey> keyToValue = Caffeine.newBuilder()
                                                              .maximumSize( MAX_DETAILS )
                                                              .build();
    private final Cache<FeatureKey,Long> valueToKey = Caffeine.newBuilder()
                                                              .maximumSize( MAX_DETAILS )
                                                              .build();
    /** Gridded features.*/
    private final GriddedFeatures.Builder griddedFeatures;

    public Features( Database database )
    {
        this.database = database;
        this.griddedFeatures = null;
    }

    /**
     * @param database the database
     * @param gridFilters the filters for gridded features, if any
     */
    public Features( Database database, List<UnnamedFeature> gridFilters )
    {
        Objects.requireNonNull( database );
        Objects.requireNonNull( gridFilters );
        
        this.database = database;
        if( ! gridFilters.isEmpty() )
        {
            LOGGER.debug( "Instantiating features for non-gridded features." );
            this.griddedFeatures = new GriddedFeatures.Builder( gridFilters );
        }
        else
        {
            this.griddedFeatures = null;
        }
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


	public Long getOrCreateFeatureId( FeatureKey key ) throws SQLException
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

    public FeatureKey getFeatureKey( long featureId )
            throws SQLException
    {
        FeatureKey value = this.keyToValue.getIfPresent( featureId );

        if ( value == null )
        {
            // Not found above, gotta find it.
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
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
                value = new FeatureKey( name, description, srid, wkt );
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

    public Long getFeatureId( FeatureKey featureKey )
            throws SQLException
    {
        Long id = this.valueToKey.getIfPresent( featureKey );

        if ( id == null )
        {
            // Not found above, gotta find it.
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
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

    /**
     * @param source the source grid with features to add
     * @throws NullPointerException if the source is null
     * @throws IOException if the source could not be read for any reason, other than nullity
     * @throws UnsupportedOperationException if the cache was not initialized with gridded features
     */
    public void addGriddedFeatures( NetcdfFile source ) throws IOException
    {
        if ( Objects.isNull( this.griddedFeatures ) )
        {
            throw new UnsupportedOperationException( "This cache has not been initialized with gridded features." );
        }
        
        Objects.requireNonNull( source );
        
        this.griddedFeatures.addFeatures( source );
    }
    
    /**
     * @return the gridded features
     * @throws UnsupportedOperationException if the cache was not initialized with gridded features
     */

    public Set<FeatureTuple> getGriddedFeatures()
    {
        if ( Objects.isNull( this.griddedFeatures ) )
        {
            throw new UnsupportedOperationException( "This cache has not been initialized with gridded features." );
        }

        GriddedFeatures features = this.griddedFeatures.build();
        return features.get();
    }

    /**
     * Get a comma separated description of a list of features.
     *
     * @param features the list of features to describe, nonnull
     * @return a description of all features
     * @throws NullPointerException when list of features is null
     */
    public static String getFeaturesDescription( List<FeatureTuple> features )
    {
        Objects.requireNonNull( features );
        StringJoiner result = new StringJoiner( ", ", "( ", " )" );

        for ( FeatureTuple feature : features )
        {
            result.add( feature.toStringShort() );
        }

        return result.toString();
    }
}
