package wres.io.data.caching;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Circle;
import wres.config.generated.Feature;
import wres.config.generated.Polygon;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.details.FeatureDetails;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.util.NotImplementedException;
import wres.util.Strings;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
public class Features extends Cache<FeatureDetails, FeatureDetails.FeatureKey>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Features.class);
    private static final Object CACHE_LOCK = new Object();

    private static final Object DETAIL_LOCK = new Object();
    private static final Object KEY_LOCK = new Object();

    @Override
    protected Object getDetailLock()
    {
        return Features.DETAIL_LOCK;
    }

    @Override
    protected Object getKeyLock()
    {
        return Features.KEY_LOCK;
    }

    /**
     *  Global cache for all Features
     */
	private static Features instance = null;

	private static Features getCache ()
	{
		synchronized (CACHE_LOCK)
		{
			if ( instance == null)
			{
                instance = new Features();
				instance.init();
			}
			return instance;
		}
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

    public static boolean lidExists( String lid) throws SQLException
	{
		LOGGER.trace( "Checking if a location named '{}' has been defined...", lid );

		boolean exists = Features.getCache().hasID( FeatureDetails.keyOfLid( lid ) );

		if (!exists)
		{
			String script = "SELECT EXISTS (" + NEWLINE +
							"		SELECT 1" + NEWLINE +
							"		FROM wres.Feature" + NEWLINE +
							"		WHERE lid = '" + lid + "'" + NEWLINE
							+
							");";

			exists = Database.getResult( script, "exists");
		}

		return exists;
	}
	
	/**
	 * Returns the ID of a Feature from the global cache based on a full Feature specification
	 * @param detail The full specification for a Feature
	 * @return The ID for the specified feature
	 * @throws SQLException Thrown if the ID could not be retrieved from the Database
	 */
    private static Integer getFeatureID(FeatureDetails detail) throws SQLException
	{
	    LOGGER.trace("getFeatureID - args {}", detail);
		return getCache().getID(detail);
	}

	private static Integer getFeatureID( FeatureDetails.FeatureKey key) throws SQLException
    {
        if (!Features.getCache().hasID( key ))
        {
            Features.getCache().addElement( new FeatureDetails( key ) );
        }

        return Features.getCache().getID( key );
    }

    private static Integer getFeatureID( Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        return Features.getFeatureID( new FeatureDetails.FeatureKey( comid, lid, gageID, huc, null, null ));
    }

    public static Integer getFeatureID(Feature feature) throws SQLException
    {
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

    private static FeatureDetails getDetails(Feature feature) throws SQLException
    {
        Integer id = Features.getFeatureID( feature );
        return Features.getCache().get( id );
    }

    private static Set<FeatureDetails> getUnspecifiedDetails( ProjectConfig projectConfig )
            throws SQLException
    {
        // A set is used to avoid duplications
        Set<FeatureDetails> features = new HashSet<>(  );

        ScriptBuilder script = new ScriptBuilder(  );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE lid != ''");
        script.addTab().addLine("AND lid IS NOT NULL");

        if (ConfigHelper.usesNetCDFData( projectConfig ))
        {
            script.addTab().addLine("AND comid != -999");
            script.addTab().addLine("AND nwm_index IS NOT NULL");
        }

        if (ConfigHelper.usesUSGSData( projectConfig ))
        {
            script.addTab(  ).addLine("AND character_length(gage_id) >= 0");
        }

        script.addLine("ORDER BY feature_id");

        script.consume( featureRow -> features.add( new FeatureDetails(  featureRow  )) );

        return features;
    }

    public static Set<FeatureDetails> getAllDetails(ProjectConfig projectConfig) throws SQLException
    {
        Set<FeatureDetails> features;

        if (projectConfig.getPair().getFeature() == null || projectConfig.getPair().getFeature().isEmpty())
        {
            features = Features.getUnspecifiedDetails( projectConfig );
        }
        else
        {
            features = Features.getSpecifiedDetails(projectConfig);
        }

        return features;
    }

    private static Set<FeatureDetails> getSpecifiedDetails( ProjectConfig projectConfig ) throws SQLException
    {
        Set<FeatureDetails> features = new HashSet<>();
        boolean hasNetCDF = ConfigHelper.usesNetCDFData( projectConfig );
        boolean hasUSGS = ConfigHelper.usesUSGSData( projectConfig );

        for (Feature feature : projectConfig.getPair().getFeature())
        {
            for (FeatureDetails details : Features.getAllDetails( feature))
            {
                if (shouldAddFeature( details, hasUSGS, hasNetCDF ))
                {
                    features.add( details );
                }
                else
                {
                    String message = "";
                    if ((hasNetCDF && details.getNwmIndex() == null) &&
                        (hasUSGS && !Strings.hasValue( details.getGageID() )))
                    {
                        message = "Since this project uses both USGS and NetCDF " +
                                  "data, the location {} can't be used for " +
                                  "evaluation because there is not enough " +
                                  "information available to connect it to both " +
                                  "USGS and NetCDF sources.";
                    }
                    else if (hasNetCDF && details.getNwmIndex() == null)
                    {
                        message = "Since this project uses NetCDF data, the " +
                                  "location {} cannot be used for " +
                                  "evaluation because there is not enough " +
                                  "information available to link it to a NetCDF " +
                                  "source file.";
                    }
                    else if (hasUSGS &&
                             !(Strings.hasValue( details.getGageID() ) || details.getGageID().length() < 8))
                    {
                        message = "Since this project uses USGS data, the " +
                                  "location {} cannot be used for " +
                                  "evaluation because there is not enough " +
                                  "information available to link it to a valid " +
                                  "USGS location.";
                    }

                    LOGGER.debug(message, details.toString());
                }
            }
        }

        return features;
    }

    private static boolean shouldAddFeature(FeatureDetails feature, boolean usesUSGS, boolean usesNetCDF)
    {
        // If we aren't using NetCDF or USGS data, we don't need to worry
        // about identifiers on the locations
        if (!(usesNetCDF || usesUSGS))
        {
            return true;
        }
        // If we are using both NetCDF and USGS data, we need both Gage IDs
        // and indexes for NetCDF files to be able to load any sort of
        // data for evaluation
        else if ((usesNetCDF && feature.getComid() != -999) &&
                 (usesUSGS && Strings.hasValue( feature.getGageID() )))
        {
            // gage ids must have 8 or more characters
            return feature.getGageID().length() >= 8;
        }
        // If we are using NetCDF data, we need indexes to determine what
        // data to retrieve
        else if (usesNetCDF && feature.getComid() != -999 && !usesUSGS)
        {
            return true;
        }
        // If we are using USGS data, we need a gageID or we won't be
        // able to retrieve data. Gages must have at least 8 digits
        else if (usesUSGS && Strings.hasValue( feature.getGageID() ) &&
                 feature.getGageID().length() >= 8 && !usesNetCDF)
        {
            return true;
        }

        return false;
    }

    private static Set<FeatureDetails> getAllDetails(Feature feature)
            throws SQLException
    {
        Set<FeatureDetails> details = new HashSet<>(  );

        if (Strings.hasValue( feature.getHuc() ))
        {
            details.addAll( Features.getDetailsByHUC( feature.getHuc()) );
        }

        if (feature.getCoordinate() != null)
        {
            details.addAll(
                    Features.getDetailsByCoordinates(
                            feature.getCoordinate().getLongitude(),
                            feature.getCoordinate().getLatitude(),
                            feature.getCoordinate().getRange()
                    )
            );
        }

        if (Strings.hasValue(feature.getRfc()))
        {
            details.addAll(Features.getDetailsByRFC( feature.getRfc().toUpperCase()));
        }

        if (Strings.hasValue( feature.getLocationId() ))
        {
            details.add( Features.getDetailsByLID( feature.getLocationId() ) );
        }

        if (Strings.hasValue( feature.getGageId() ))
        {
            details.add( Features.getDetailsByGageID( feature.getGageId() ) );
        }

        if (feature.getComid() != null)
        {
            details.add (Features.getDetails( feature.getComid().intValue(),
                                              null,
                                              null,
                                              null ));
        }

        if (feature.getPolygon() != null || feature.getCircle() != null)
        {
            details.addAll(Features.getDetailsByGeometry( feature ));
        }

        for (FeatureDetails detail : details)
        {
            detail.setAliases( feature.getAlias() );
        }

        return details;
    }

    public static List<FeatureDetails> getGriddedDetails(ProjectDetails details)
            throws SQLException
    {
        List<FeatureDetails> features;

        if (details.getProjectConfig().getPair().getFeature().size() > 0)
        {
            features = Features.getSpecifiedGriddedFeatures( details.getProjectConfig() );
        }
        else
        {
            features = Features.getAllGriddedFeatures( details.getProjectConfig() );
        }

        return features;
    }

    private static List<FeatureDetails> getAllGriddedFeatures(ProjectConfig projectConfig) throws SQLException
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
     * @param projectConfig
     * @return
     * @throws SQLException
     */
    private static List<FeatureDetails> getSpecifiedGriddedFeatures(ProjectConfig projectConfig) throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("SELECT geographic_coordinate[0]::real AS longitude,");
        script.addTab().addLine("geographic_coordinate[1]::real AS latitude,");
        script.addTab().addLine("row_number() OVER (ORDER BY x_position, y_position)::int AS feature_id,");
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

                StringJoiner pointJoiner = new StringJoiner( "), (",
                                                             "geographic_coordinate <@ POLYGON '( (",
                                                             ") )'" );

                for ( Polygon.Point point : feature.getPolygon().getPoint())
                {
                    pointJoiner.add(point.getLongitude() + ", " + point.getLatitude());
                }



                script.addLine(pointJoiner.toString(), " )");
            }
        }

        return Features.getDetailsFromDatabase( script );
    }

    private static FeatureDetails getDetails(Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        Integer id = Features.getFeatureID( comid, lid, gageID, huc );
        return Features.getCache().get( id );
    }

    private static FeatureDetails getDetailsByLID(String lid) throws SQLException
    {
        return Features.getDetails( null, lid, null, null );
    }

    public static FeatureDetails getDetailsByGageID(String gageID) throws SQLException
    {
        return Features.getDetails( null, null, gageID, null );
    }

    private static List<FeatureDetails> getDetailsByCoordinates(final Float longitude, final Float latitude, final Float range)
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

        ScriptBuilder script = new ScriptBuilder(  );
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

        return Features.getDetailsFromDatabase( script );
    }

    private static List<FeatureDetails> getDetailsByGeometry( Feature feature ) throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

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
                           ") )");
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

            StringJoiner pointJoiner = new StringJoiner( "), (",
                                                         " POINT(F.longitude, F.latitude) <@ polygon '((",
                                                         "))'" );

            for ( Polygon.Point point : feature.getPolygon().getPoint())
            {
                pointJoiner.add( point.getLongitude() + ", " + point.getLatitude() );
            }

            script.addLine(pointJoiner.toString(), ")");
        }

        script.addTab().add(");");

        return Features.getDetailsFromDatabase( script );
    }

    private static List<FeatureDetails> getDetailsByHUC(String huc)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE huc LIKE '", huc, "%'");
        script.addLine("ORDER BY feature_id;");

        return Features.getDetailsFromDatabase( script );
    }

    private static List<FeatureDetails> getDetailsByRFC(String rfc)
            throws SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );
        script.addLine("SELECT *");
        script.addLine("FROM wres.Feature");
        script.addLine("WHERE rfc = '", rfc, "'");
        script.addLine("ORDER BY feature_id;");

        return Features.getDetailsFromDatabase( script );
    }

    private static List<FeatureDetails> getDetailsFromDatabase(ScriptBuilder script) throws SQLException
    {
        List<FeatureDetails> features = script.interpret( FeatureDetails::new );

        features.forEach( Features.getCache()::add );

        return features;
    }

    public static Integer getVariablePositionIDByLID(String lid, Integer variableID)
            throws SQLException
    {
        return Features.getDetailsByLID( lid ).getVariablePositionID( variableID );
    }

    public static Integer getVariablePositionID(Feature feature, Integer variableID)
            throws SQLException
    {
        return Features.getDetails( feature ).getVariablePositionID( variableID );
    }
	
	@Override
	protected int getMaxDetails() {
		return 5000;
	}

	/**
	 * Loads all pre-existing Features into the instanced cache
	 */
	@Override
    public synchronized void init()
	{
        LOGGER.trace("The features cache is being created");
        getDetails();
	}
}
