package wres.io.data.caching;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Features extends Cache<FeatureDetails, FeatureKey>
{
    private static final int MAX_DETAILS = 5000;

    private static final Logger LOGGER = LoggerFactory.getLogger(Features.class);

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Database database;

    public Features( Database database )
    {
        this.database = database;
        this.initializeDetails();
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

    @Override
    protected Database getDatabase()
    {
        return this.database;
    }


	/**
	 * Returns the ID of a Feature from the global cache based on a full Feature specification
	 * @param detail The full specification for a Feature
	 * @return The ID for the specified feature
	 * @throws SQLException Thrown if the ID could not be retrieved from the Database
	 */
    public Long getFeatureID( FeatureDetails detail )
            throws SQLException
	{
	    LOGGER.trace("getFeatureID - args {}", detail);
		return this.getID(detail);
	}

	public Long getFeatureID( FeatureKey key ) throws SQLException
    {
        LOGGER.trace( "getFeatureID with FeatureKey arg {}", key );
        Long result;

        synchronized ( this.keyLock )
        {
            if ( !this.hasID( key ) )
            {
                LOGGER.trace( "getFeatureID with FeatureKey arg {} NOT in cache",
                              key );
                FeatureDetails featureDetails = new FeatureDetails( key );
                this.addElement( featureDetails );
                Long fakeResultForLRUPurposes = this.getID( key );
                result = featureDetails.getId();
            }
            else
            {
                LOGGER.trace( "getFeatureID with FeatureKey arg {} FOUND in cache",
                              key );
                result = this.getID( key );
            }
        }

        LOGGER.trace( "getFeatureId with FeatureKey arg {} returning result {}",
                      key, result );
        return result;
    }

    /**
     * Given a db row id, find the FeatureDetails object for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * @param featureId the db-instance-specific surrogate key for the feature
     * @return the existing or new FeatureDetails
     * @throws SQLException when communication with the database fails.
     */

    public FeatureKey getFeatureKey( int featureId )
            throws SQLException
    {
        LOGGER.trace( "getFeatureKey called with {}", featureId );
        for ( Map.Entry<FeatureKey,Long> cacheEntry :
                this
                        .getKeyIndex()
                        .entrySet() )
        {
            if ( cacheEntry.getValue() == featureId )
            {
                LOGGER.trace( "getFeatureKey found in cache id {}: {}",
                              featureId, cacheEntry );
                return cacheEntry.getKey();
            }
        }

        LOGGER.trace( "getFeatureKey is going to reach out to db for key {}",
                      featureId );
        // Not found above, gotta find it.
        Database database = this.getDatabase();
        DataScripter dataScripter = new DataScripter( database );
        dataScripter.addLine( "SELECT name, description, srid, wkt" );
        dataScripter.addLine( "FROM wres.Feature" );
        dataScripter.addLine( "WHERE feature_id = ?" );
        dataScripter.addArgument( featureId );

        try ( DataProvider dataProvider = dataScripter.getData() )
        {
            String name = dataProvider.getString( "name" );
            String description = dataProvider.getString( "description" );
            Integer srid = dataProvider.getInt( "srid" );
            String wkt = dataProvider.getString( "wkt" );
            return new FeatureKey( name, description, srid, wkt );
        }
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
            LOGGER.info( "{}", dataProvider.getColumnNames() );
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
                LOGGER.debug( "Added gridded feature: {}", featureKey );
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

    @Override
    public boolean hasID( FeatureKey key )
    {
        LOGGER.trace( "hasID( FeatureKey={} )", key );
        synchronized ( this.getKeyLock() )
        {
            LOGGER.trace( "hasID( FeatureKey={} ) key.hasPrimaryKey() was false, matching on partial key...",
                          key );

            // Otherwise, scan the keys for a match on a partial key
            boolean hasIt = false;

            for ( FeatureKey featureKey : this.getKeyIndex().keySet() )
            {
                hasIt = featureKey.equals( key );
                if ( hasIt )
                {
                    break;
                }
            }

            LOGGER.trace( "hasID( FeatureKey={} ) key.hasPrimaryKey() was false, hasIt={}",
                          key, hasIt );
            return hasIt;
        }
    }

    @Override
    Long getID( FeatureKey key )
    {
        LOGGER.trace( "getID( FeatureKey={} )", key );
        synchronized ( this.getKeyLock() )
        {
            LOGGER.trace( "getID( FeatureKey={} ) key.hasPrimaryKey() was false, matching on partial key...",
                          key );
            // Otherwise, scan the keys for a match on a partial key
            Long id = null;
            boolean foundIt = false;

            for ( Map.Entry<FeatureKey, Long> keyId : this.getKeyIndex().entrySet() )
            {
                if ( keyId.getKey().equals( key ) )
                {
                    id = keyId.getValue();
                    foundIt = true;
                    break;
                }
            }

            if ( foundIt && id == null )
            {
                LOGGER.debug( "The key of {} was found but the ID couldn't be retrieved.", key );
            }

            LOGGER.trace( "getID( FeatureKey={} ) key.hasPrimaryKey() was false, foundIt={}",
                          key, foundIt );
            return id;
        }
    }


	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
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
