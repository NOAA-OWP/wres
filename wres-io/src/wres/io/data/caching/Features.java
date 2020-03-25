package wres.io.data.caching;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.details.FeatureDetails;
import wres.io.project.Project;
import wres.io.utilities.DataProvider;
import wres.io.utilities.DataScripter;
import wres.io.utilities.Database;
import wres.util.NotImplementedException;
import wres.util.Strings;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
public class Features extends Cache<FeatureDetails, FeatureDetails.FeatureKey>
{
    private static final int MAX_DETAILS = 5000;

    private static final Logger LOGGER = LoggerFactory.getLogger(Features.class);

    private final Object detailLock = new Object();
    private final Object keyLock = new Object();
    private final Object positionLock = new Object();
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


    @Override
    Map<FeatureDetails.FeatureKey, Integer> getKeyIndex()
    {
        if (this.keyIndex == null)
        {
            // Fuzzy selection is required, so a LinkedHashMap cannot be used here
            this.keyIndex = new TreeMap<>(  );
        }
        return this.keyIndex;
    }

    public boolean lidExists( String lid) throws SQLException
	{
		LOGGER.trace( "Checking if a location named '{}' has been defined...", lid );

		boolean exists = this.hasID( FeatureDetails.keyOfLid( lid ) );

		if (!exists)
		{
		    Database database = this.getDatabase();
		    DataScripter script = new DataScripter( database );
		    script.addLine("SELECT EXISTS (");
		    script.addTab().addLine("SELECT 1");
		    script.addTab().addLine("FROM wres.Feature");
		    script.addTab().addLine("WHERE lid = '", lid, "'");
		    script.add(");");

			exists = script.retrieve( "exists" );
		}

		return exists;
	}

	/**
	 * Returns the ID of a Feature from the global cache based on a full Feature specification
	 * @param detail The full specification for a Feature
	 * @return The ID for the specified feature
	 * @throws SQLException Thrown if the ID could not be retrieved from the Database
	 */
    public Integer getFeatureID( FeatureDetails detail )
            throws SQLException
	{
	    LOGGER.trace("getFeatureID - args {}", detail);
		return this.getID(detail);
	}

	private Integer getFeatureID( FeatureDetails.FeatureKey key) throws SQLException
    {
        LOGGER.trace( "getFeatureID with FeatureKey arg {}", key );
        Integer result;

        synchronized ( this.keyLock )
        {
            if ( !this.hasID( key ) )
            {
                LOGGER.trace( "getFeatureID with FeatureKey arg {} NOT in cache",
                              key );
                FeatureDetails featureDetails = new FeatureDetails( key );
                this.addElement( featureDetails );
                Integer fakeResultForLRUPurposes = this.getID( key );
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

    private Integer getFeatureID( Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        LOGGER.trace( "getFeatureID with 4-args: comid={}, lid={}, gageID={}, huc={}",
                      comid, lid, gageID, huc );
        return this.getFeatureID( new FeatureDetails.FeatureKey( comid, lid, gageID, huc, null, null ));
    }

    private Integer getFeatureID( Feature feature ) throws SQLException
    {
        LOGGER.trace( "getFeatureID with Feature: {}", feature );
        FeatureDetails details = new FeatureDetails(  );
        details.setLid( feature.getLocationId() );

        if (feature.getComid() != null)
        {
            details.setComid( feature.getComid().intValue() );
        }

        details.setGageID( feature.getGageId() );
        details.setHuc( feature.getHuc() );
        details.setFeatureName( feature.getName() );

        return getFeatureID( details );
    }


    /**
     * Given a db row id, find the FeatureDetails object for it.
     * Tries to find in the cache and if it is not there, reaches to the db.
     * @param featureId the db-instance-specific surrogate key for the feature
     * @return the existing or new FeatureDetails
     * @throws SQLException when communication with the database fails.
     */

    public FeatureDetails.FeatureKey getFeatureKey( int featureId )
            throws SQLException
    {
        LOGGER.trace( "getFeatureKey called with {}", featureId );
        for ( Map.Entry<FeatureDetails.FeatureKey,Integer> cacheEntry :
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
        dataScripter.addLine( "SELECT lid, comid, huc, gage_id, longitude, latitude" );
        dataScripter.addLine( "FROM wres.Feature" );
        dataScripter.addLine( "WHERE feature_id = ?" );
        dataScripter.addArgument( featureId );

        try ( DataProvider dataProvider = dataScripter.getData() )
        {
            String lid = dataProvider.getString( "lid" );
            Integer comid = dataProvider.getInt( "comid" );
            String gageId = dataProvider.getString( "gage_id" );
            String huc = dataProvider.getString( "huc" );
            Double longitude = dataProvider.getDouble( "longitude" );
            Double latitude = dataProvider.getDouble( "latitude" );
            return new FeatureDetails.FeatureKey( comid, lid, gageId, huc, longitude, latitude );
        }
    }


    public FeatureDetails getDetails(Feature feature) throws SQLException
    {
        LOGGER.trace( "getDetails with Feature: {}", feature );
        Integer id = this.getFeatureID( feature );

        if (id == null)
        {
            throw new NullPointerException(
                    "No feature ID could be found for the feature described as: location id: '" +
                    feature.getLocationId() +
                    "', gage id: '" +
                    feature.getGageId() +
                    "', comid: '" +
                    feature.getComid());
        }

        return this.get( id );
    }

    private Set<FeatureDetails> getUnspecifiedDetails( ProjectConfig projectConfig )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE lid != ''");
        script.addTab().addLine("AND lid IS NOT NULL");

        if (ConfigHelper.usesNetCDFData( projectConfig ))
        {
            script.addTab().addLine("AND comid > 0");
            script.addTab().addLine("AND comid IS NOT NULL");
        }

        script.addLine("ORDER BY feature_id");

        // A set is used to avoid duplications
        Set<FeatureDetails> features = new HashSet<>(  );
        script.consume( featureRow -> features.add( new FeatureDetails(  featureRow  )) );

        return features;
    }

    public Set<FeatureDetails> getAllDetails(ProjectConfig projectConfig) throws SQLException
    {
        Set<FeatureDetails> features;

        if (projectConfig.getPair().getFeature() == null || projectConfig.getPair().getFeature().isEmpty())
        {
            features = this.getUnspecifiedDetails( projectConfig );
        }
        else
        {
            features = this.getSpecifiedDetails(projectConfig);
        }

        return features;
    }

    private Set<FeatureDetails> getSpecifiedDetails( ProjectConfig projectConfig ) throws SQLException
    {
        Set<FeatureDetails> features = new HashSet<>();

        for ( Feature feature : projectConfig.getPair().getFeature() )
        {
            for ( FeatureDetails details : this.getAllDetails( feature ) )
            {
                features.add( details );
            }
        }

        return features;
    }

    private Set<FeatureDetails> getAllDetails(Feature feature)
            throws SQLException
    {
        Set<FeatureDetails> details = new HashSet<>(  );

        if (Strings.hasValue( feature.getHuc() ))
        {
            details.addAll( this.getDetailsByHUC( feature.getHuc()) );
        }

        if (feature.getCoordinate() != null)
        {
            details.addAll(
                    this.getDetailsByCoordinates(
                            feature.getCoordinate().getLongitude(),
                            feature.getCoordinate().getLatitude(),
                            feature.getCoordinate().getRange()
                    )
            );
        }

        if (Strings.hasValue(feature.getRfc()))
        {
            details.addAll(this.getDetailsByRegion( feature.getRfc().toUpperCase()));
        }

        if (Strings.hasValue( feature.getState() ))
        {
            details.addAll(this.getDetailsByState( feature.getState().toUpperCase() ));
        }

        if (Strings.hasValue( feature.getLocationId() ))
        {
            details.add( this.getDetailsByLID( feature.getLocationId().toUpperCase() ) );
        }

        if (Strings.hasValue( feature.getGageId() ))
        {
            details.add( this.getDetailsByGageID( feature.getGageId() ) );
        }

        if (feature.getComid() != null)
        {
            details.add (this.getDetails( feature.getComid().intValue(),
                                              null,
                                              null,
                                              null ));
        }

        if (feature.getPolygon() != null || feature.getCircle() != null)
        {
            details.addAll(this.getDetailsByGeometry( feature ));
        }

        for (FeatureDetails detail : details)
        {
            detail.setAliases( feature.getAlias() );
        }

        return details;
    }

    public Set<FeatureDetails> getGriddedDetails( Project details )
            throws SQLException
    {
        Set<FeatureDetails> features;

        if (details.getProjectConfig().getPair().getFeature().size() > 0)
        {
            features = this.getSpecifiedGriddedFeatures( details.getProjectConfig() );
        }
        else
        {
            features = this.getAllGriddedFeatures( details.getProjectConfig() );
        }

        return features;
    }

    private static Set<FeatureDetails> getAllGriddedFeatures( ProjectConfig projectConfig )
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
    private Set<FeatureDetails> getSpecifiedGriddedFeatures( ProjectConfig projectConfig )
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

        for (Feature feature : projectConfig.getPair().getFeature())
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

        return this.getDetailsFromDatabase( script );
    }

    private FeatureDetails getDetails(Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        LOGGER.trace( "getDetails with 4-args: comid={}, lid={}, gageID={}, huc={}",
                      comid, lid, gageID, huc );
        Integer id = this.getFeatureID( comid, lid, gageID, huc );
        if (id == null)
        {
            throw new NullPointerException(
                    "No feature ID could be found for the feature described as: location id: '" +
                    lid +
                    "', gage id: '" +
                    gageID +
                    "', comid: '" +
                    comid );
        }
        return this.get( id );
    }

    private FeatureDetails getDetailsByLID(String lid) throws SQLException
    {
        LOGGER.trace( "getDetailsByLID lid={}", lid );
        return this.getDetails( null, lid, null, null );
    }

    public FeatureDetails getDetailsByGageID(String gageID) throws SQLException
    {
        LOGGER.trace( "getDetailsByGageID lid={}", gageID );
        return this.getDetails( null, null, gageID, null );
    }

    private Set<FeatureDetails> getDetailsByCoordinates( Float longitude,
                                                         Float latitude,
                                                         Float range )
            throws SQLException
    {
        Double radianLatitude = Math.toRadians( latitude );

        // This is the approximate distance between longitudinal degrees at
        // the equator
        final Double distanceAtEquator = 111321.0;

        // This is an approximation
        Double distanceOfOneDegree = Math.cos(radianLatitude) * distanceAtEquator;

        // We take the max between the approximate distance and 0.00005 because
        // 0.00005 serves as a decent epsilon for database distance comparison.
        // If the distance is much smaller than that, float point error
        // can exclude the requested location, even if the coordinates were
        // spot on.
        Double rangeInDegrees = Math.max( range / distanceOfOneDegree, 0.00005);

        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("WITH feature_and_distance AS");
        script.addLine("(");
        script.addTab().addLine("SELECT SQRT((", latitude, " - latitude)^2 + (", longitude, " - longitude)^2) AS distance");
        script.addTab().addLine("FROM wres.Feature");
        script.addTab().addLine("WHERE NOT (");
        script.addTab(  2  ).addLine("longitude IS NULL OR latitude IS NULL");
        script.addTab().addLine(")");
        script.addLine(")");
        script.addLine("SELECT *");
        script.addLine("FROM feature_and_distance");
        script.addLine("WHERE distance <= ", rangeInDegrees);
        script.addLine("ORDER BY feature_id;");

        return this.getDetailsFromDatabase( script );
    }

    private Set<FeatureDetails> getDetailsByGeometry( Feature feature )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature F");
        script.addLine("WHERE F.latitude IS NOT NULL");
        script.addTab().addLine("AND (");
        boolean geometryAdded = false;

        if (feature.getCircle() != null)
        {
            geometryAdded = true;
            script.addLine("( POINT(F.longitude, F.latitude) <@ CIRCLE '((",
                           feature.getCircle().getLongitude(),
                           ", ",
                           feature.getCircle().getLatitude(),
                           "), ",
                           feature.getCircle().getDiameter(),
                           ")' )");
        }

        if (feature.getPolygon() != null)
        {
            if ( geometryAdded )
            {
                script.addTab( 2 ).add( "OR (" );
            }
            else
            {
                script.addTab( 2 ).add( "(" );
            }

            String shape = "POLYGON";

            if (feature.getPolygon().getPoint().size() == 2)
            {
                shape = "BOX";
            }

            StringJoiner pointJoiner = new StringJoiner( "), (",
                                                         " POINT(F.longitude, F.latitude) <@ " + shape + " '((",
                                                         "))'" );

            for ( Polygon.Point point : feature.getPolygon().getPoint())
            {
                pointJoiner.add( point.getLongitude() + ", " + point.getLatitude() );
            }

            script.addLine(pointJoiner.toString(), ")");
        }

        script.addTab().add(");");

        return this.getDetailsFromDatabase( script );
    }

    private Set<FeatureDetails> getDetailsByHUC( String huc )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE huc LIKE '", huc, "%'");
        script.addLine("ORDER BY feature_id;");

        return this.getDetailsFromDatabase( script );
    }

    private Set<FeatureDetails> getDetailsByRegion( String region )
            throws SQLException
    {
        LOGGER.trace( "getDetailsByRegion region={}", region );
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE region = '", region, "'");
        script.addLine("ORDER BY feature_id;");

        return this.getDetailsFromDatabase( script );
    }

    private Set<FeatureDetails> getDetailsByState( String state )
            throws SQLException
    {
        Database database = this.getDatabase();
        DataScripter script = new DataScripter( database );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE state = '", state, "'");
        script.addLine("ORDER BY feature_id;");

        return this.getDetailsFromDatabase( script );
    }

    private Set<FeatureDetails> getDetailsFromDatabase( DataScripter script )
            throws SQLException
    {
        LOGGER.trace( "getDetailsFromDatabase( script={} )", script );
        List<FeatureDetails> features = script.interpret( FeatureDetails::new );

        features.forEach( this::add );

        LOGGER.trace( "getDetailsFromDatabase( script={} ) found features: {}",
                      script, features );
        return new HashSet<>( features );
    }

    public Integer getVariableFeatureIDByLID(String lid, Integer variableID)
            throws SQLException
    {
        LOGGER.trace( "getVariableFeatureIDByLID( lid={}, variableID={} )", lid, variableID );
        FeatureDetails featureDetails = this.getDetailsByLID( lid );
        return this.getVariableFeatureByFeature( featureDetails, variableID );
    }

    @Override
    public boolean hasID( FeatureDetails.FeatureKey key )
    {
        LOGGER.trace( "hasID( FeatureDetails.FeatureKey={} )", key );
        synchronized ( this.getKeyLock() )
        {
            if (key.hasPrimaryKey())
            {
                LOGGER.trace( "hasID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was true",
                              key );
                // If the primary key for the FeatureKey has the current primary key (lid at the time of writing),
                // use the standard search
                return this.getKeyIndex().containsKey( key );
            }
            else
            {
                LOGGER.trace( "hasID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was false, matching on partial key...",
                              key );

                // Otherwise, scan the keys for a match on a partial key
                boolean hasIt = false;

                for ( FeatureDetails.FeatureKey featureKey : this.getKeyIndex().keySet() )
                {
                    hasIt = featureKey.equals( key );
                    if ( hasIt )
                    {
                        break;
                    }
                }

                LOGGER.trace( "hasID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was false, hasIt={}",
                              key, hasIt );
                return hasIt;
            }
        }
    }

    @Override
    Integer getID( FeatureDetails.FeatureKey key ) throws SQLException
    {
        LOGGER.trace( "getID( FeatureDetails.FeatureKey={} )", key );
        synchronized ( this.getKeyLock() )
        {
            if (key.hasPrimaryKey())
            {
                LOGGER.trace( "getID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was true",
                              key );
                // If the primary key for the FeatureKey has the current primary key (lid at the time of writing),
                // use the standard search
                return this.getKeyIndex().get( key );
            }
            else
            {
                LOGGER.trace( "getID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was false, matching on partial key...",
                              key );
                // Otherwise, scan the keys for a match on a partial key
                Integer id = null;
                boolean foundIt = false;

                for ( Map.Entry<FeatureDetails.FeatureKey, Integer> keyId : this.getKeyIndex().entrySet() )
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

                LOGGER.trace( "getID( FeatureDetails.FeatureKey={} ) key.hasPrimaryKey() was false, foundIt={}",
                              key, foundIt );
                return id;
            }
        }
    }

    public Integer getVariableFeatureID( Feature feature, Integer variableID)
            throws SQLException
    {
        FeatureDetails featureDetails = this.getDetails( feature );
        return this.getVariableFeatureByFeature( featureDetails, variableID );
    }

    public Integer getVariableFeatureByFeature(FeatureDetails featureDetails, Integer variableId) throws SQLException
    {
        LOGGER.trace( "getVariableFeatureByFeature( FeatureDetails={}, variableId={} )",
                      featureDetails, variableId );
        synchronized ( this.positionLock )
        {
            if (!this.getKeyIndex().containsKey( featureDetails.getKey() ))
            {
                LOGGER.trace( "getVariableFeatureByFeature( FeatureDetails={}, variableId={} ) key not found, adding new featureDetails.",
                              featureDetails, variableId );
                this.add( featureDetails );
            }

            Integer id = featureDetails.getVariableFeatureID( variableId );

            if (id == null)
            {
                LOGGER.trace( "getVariableFeatureByFeature( FeatureDetails={}, variableId={} ) getVariableFeatureID returned null, scripting...",
                              featureDetails, variableId );
                Database database = this.getDatabase();
                DataScripter script = new DataScripter( database );
                script.setHighPriority( true );

                script.retryOnSerializationFailure();
                script.retryOnUniqueViolation();

                script.setUseTransaction( true );
                script.addTab().addLine("INSERT INTO wres.VariableFeature (variable_id, feature_id)");
                script.addTab().addLine( "SELECT ?, ?" );
                script.addArgument( variableId );
                script.addArgument( featureDetails.getId() );
                script.addTab().addLine("WHERE NOT EXISTS (");
                script.addTab(  2  ).addLine("SELECT 1");
                script.addTab(  2  ).addLine("FROM wres.VariableFeature VF");
                script.addTab(  2  ).addLine( "WHERE VF.variable_id = ?" );
                script.addArgument( variableId );
                script.addTab(   3   ).addLine( "AND VF.feature_id = ?" );
                script.addArgument( featureDetails.getId() );
                script.addTab().addLine(")");
                int rowsModified = script.execute();

                if ( rowsModified == 1 )
                {
                    id = script.getInsertedIds()
                               .get( 0 )
                               .intValue();
                }
                else if ( rowsModified < 1 )
                {
                    DataScripter scriptExistingId = new DataScripter( database );
                    scriptExistingId.addLine( "SELECT variablefeature_id" );
                    scriptExistingId.addLine( "FROM wres.VariableFeature VF" );
                    scriptExistingId.addLine( "WHERE VF.variable_id = ?" );
                    scriptExistingId.addArgument( variableId );
                    scriptExistingId.addTab().addLine( "AND VF.feature_id = ?;" );
                    scriptExistingId.addArgument( featureDetails.getId() );
                    id = scriptExistingId.retrieve( "variablefeature_id" );
                }
                else
                {
                    throw new IllegalStateException( "Expected only 0 or 1 row modified by "
                                                     + script );
                }

                featureDetails.addVariableFeature( variableId, id );
            }

            return id;
        }
    }

	@Override
	protected int getMaxDetails() {
		return MAX_DETAILS;
	}
}
