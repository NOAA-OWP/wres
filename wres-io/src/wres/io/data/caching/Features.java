package wres.io.data.caching;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import wres.config.generated.Polygon;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UnnamedFeature;
import wres.datamodel.FeatureTuple;
import wres.datamodel.FeatureKey;
import wres.io.data.details.FeatureDetails;
import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.NotImplementedException;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
public class Features
{
    private static final int MAX_DETAILS = 5000;

    private final Database database;

    private volatile boolean onlyReadFromDatabase = false;
    private final Cache<Long,FeatureKey> keyToValue = Caffeine.newBuilder()
                                                              .maximumSize( MAX_DETAILS )
                                                              .build();
    private final Cache<FeatureKey,Long> valueToKey = Caffeine.newBuilder()
                                                              .maximumSize( MAX_DETAILS )
                                                              .build();

    public Features( Database database )
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


	public Long getOrCreateFeatureId( FeatureKey key ) throws SQLException
    {
        if ( this.onlyReadFromDatabase )
        {
            throw new IllegalStateException( "This instance now allows no new features, call another method!" );
        }

        Long id = valueToKey.getIfPresent( key );

        if ( id == null )
        {
            FeatureDetails featureDetails = new FeatureDetails( key );
            featureDetails.save( this.getDatabase() );
            id = featureDetails.getId();

            if ( id == null )
            {
                throw new IllegalStateException( "Issue getting id from FeatureDetails" );
            }

            valueToKey.put( key, id );
            keyToValue.put( id, key );
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
        FeatureKey value = keyToValue.getIfPresent( featureId );

        if ( value == null )
        {
            // Not found above, gotta find it.
            Database database = this.getDatabase();
            DataScripter dataScripter = new DataScripter( database );
            dataScripter.addLine( "SELECT name, description, srid, wkt" );
            dataScripter.addLine( "FROM wres.Feature" );
            dataScripter.addLine( "WHERE feature_id = ?" );
            dataScripter.addArgument( featureId );
            dataScripter.setUseTransaction( false );
            dataScripter.setMaxRows( 1 );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                String name = dataProvider.getString( "name" );
                String description = dataProvider.getString( "description" );
                Integer srid = dataProvider.getInt( "srid" );
                String wkt = dataProvider.getString( "wkt" );
                value = new FeatureKey( name, description, srid, wkt );
            }

            keyToValue.put( featureId, value );
            valueToKey.put( value, featureId );
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
        Long id = valueToKey.getIfPresent( featureKey );

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
            dataScripter.setUseTransaction( false );
            dataScripter.setMaxRows( 1 );
            dataScripter.setHighPriority( true );

            try ( DataProvider dataProvider = dataScripter.getData() )
            {
                id = dataProvider.getLong( "feature_id" );
            }

            keyToValue.put( id, featureKey );
            valueToKey.put( featureKey, id );
        }

        return id;
    }


    public Set<FeatureTuple> getGriddedDetails( Project details )
            throws SQLException
    {
        Set<FeatureTuple> features;

        if (details.getProjectConfig().getPair().getGridSelection().size() > 0)
        {
            features = this.getSpecifiedGriddedFeatures( details.getProjectConfig() );
        }
        else
        {
            features = Features.getAllGriddedFeatures( details.getProjectConfig() );
        }

        return features;
    }

    private static Set<FeatureTuple> getAllGriddedFeatures( ProjectConfig projectConfig )
            throws SQLException
    {
        // We need a new solution for decomposing gridded features; we can't
        // and shouldn't hold ~17,000,000 of these objects in memory at once
        throw new NotImplementedException( "The retrieval of all gridded features has not been implemented yet." );

    }

    /**
     * Creates a list of features to retrieve from gridded data.
     * <p>
     *     Retrieving data is currently reliant on a WKT. RFCs and bounding
     *     boxes should be implemented later.
     * </p>
     * @param projectConfig The project configuration that holds the specifications for what gridded features to use
     * @return The list of feature details to use for evaluation
     * @throws SQLException Thrown if an issue is encountered while running the script in the database
     */

    private Set<FeatureTuple> getSpecifiedGriddedFeatures( ProjectConfig projectConfig )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );

        script.addLine("SELECT geographic_coordinate[0] AS longitude,");
        script.addTab().addLine("geographic_coordinate[1] AS latitude,");
        script.addTab().addLine("(rpad(LEAST(x_position, 21473)::VARCHAR, 4, '1') || lpad(y_position::VARCHAR, 4, '0'))::INT AS feature_id,");
        script.addTab().addLine("*");
        script.addLine("FROM wres.NetcdfCoordinate");
        script.addLine("WHERE");

        boolean geometryAdded = false;

        for ( UnnamedFeature feature : projectConfig.getPair().getGridSelection() )
        {
            if (feature.getCircle() != null)
            {
                if (geometryAdded)
                {
                    script.addTab().add("OR ( ");
                }
                else
                {
                    geometryAdded = true;
                    script.addTab().add("( ");
                }

                script.add("geographic_coordinate <@ CIRCLE '( ( ",
                           feature.getCircle().getLongitude(),
                           ", ",
                           feature.getCircle().getLatitude(),
                           "), ",
                           feature.getCircle().getDiameter(),
                           ") )'");

                script.addLine(" )");
            }

            if (feature.getPolygon() != null)
            {
                if (geometryAdded)
                {
                    script.addTab().add("OR ( ");
                }
                else
                {
                    geometryAdded = true;
                    script.addTab().add("( ");
                }

                String shape = "POLYGON";

                if (feature.getPolygon().getPoint().size() == 2)
                {
                    shape = "BOX";
                }

                StringJoiner pointJoiner = new StringJoiner( "), (",
                                                             "geographic_coordinate <@ " + shape + " '( (",
                                                             ") )'" );

                for ( Polygon.Point point : feature.getPolygon().getPoint())
                {
                    pointJoiner.add(point.getLongitude() + ", " + point.getLatitude());
                }

                script.addLine(pointJoiner.toString(), " )");
            }
        }

        return this.getUnnamedFeaturesFromDatabase( script );
    }

    private Set<FeatureTuple> getUnnamedFeaturesFromDatabase( DataScripter scripter )
            throws SQLException
    {
        Set<FeatureTuple> featureTuples = new HashSet<>();

        try ( DataProvider dataProvider = scripter.getData() )
        {
            while ( dataProvider.next() )
            {
                double x = dataProvider.getDouble( "longitude" );
                double y = dataProvider.getDouble( "latitude" );
                StringJoiner wktBuilder =
                        new StringJoiner( " " );
                wktBuilder.add( "POINT(" );
                wktBuilder.add( Double.toString( x ) );
                wktBuilder.add( Double.toString( y ) );
                wktBuilder.add( ")" );
                String wkt = wktBuilder.toString();
                FeatureKey featureKey = new FeatureKey( Features.getGriddedNameFromLonLat( x, y ),
                                                        Features.getGriddedDescriptionFromLonLat( x, y ),
                                                        4326,
                                                        wkt );
                FeatureTuple featureTuple =
                        new FeatureTuple( featureKey, featureKey, featureKey );
                featureTuples.add( featureTuple );
            }
        }

        return Collections.unmodifiableSet( featureTuples );
    }


    /**
     * Creates a float-rounded shorthand name for given lon, lat values.
     *
     * TODO: Use full precision, not truncated to float values
     *
     * This name will be used for filenames, the other for the data itself. The
     * goal is to keep scenario650 benchmarks intact, however inconsistent.
     *
     * @param x The longitude value
     * @param y The latitude value
     * @return Name with E or W and N or S.
     * @throws IllegalArgumentException When longitude or latitude out of range.
     */
    private static String getGriddedDescriptionFromLonLat( double x, double y )
    {
        Features.validateLonLat( x, y );

        String name;

        if ( x < 0 )
        {
            name = Math.abs( (float) x ) + "W_";
        }
        else
        {
            name = (float) x + "E_";
        }

        if ( y < 0 )
        {
            name += Math.abs( (float) y ) + "S";
        }
        else
        {
            name += (float) y + "N";
        }

        return name;
    }

    /**
     * Creates a float-rounded shorthand name for given lon, lat values.
     *
     * TODO: Use full precision, not truncated to float values
     *
     * This name will be used for data itself, the other for filenames. The
     * goal is to keep scenario650 benchmarks intact, however inconsistent.
     * @param x The longitude value
     * @param y The latitude value
     * @return Name with E or W and N or S.
     * @throws IllegalArgumentException When longitude or latitude out of range.
     */
    private static String getGriddedNameFromLonLat( double x, double y )
    {
        Features.validateLonLat( x, y );
        return (float) x + " " + (float) y;
    }

    /**
     * @throws IllegalArgumentException When longitude or latitude out of range.
     */
    private static void validateLonLat( double x, double y )
    {
        if ( x < -180.0 || x > 180.0 )
        {
            throw new IllegalArgumentException( "Expected longitude x between -180.0 and 180.0, got "
                                                + x );
        }

        if ( y < -90.0 || y > 90.0 )
        {
            throw new IllegalArgumentException( "Expected latitude y between -90.0 and 90.0, got "
                                                + y );
        }
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
