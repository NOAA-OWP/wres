package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.Format;
import wres.config.generated.ProjectConfig;
import wres.io.data.details.FeatureDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class Features extends Cache<FeatureDetails, FeatureDetails.FeatureKey>
{

    private static final Logger LOGGER = LoggerFactory.getLogger(Features.class);
    private static final Object CACHE_LOCK = new Object();

    /**
     *  Global cache for all Features
     */
	private static Features INTERNAL_CACHE = null;

	private static Features getCache ()
	{
		synchronized (CACHE_LOCK)
		{
			if (INTERNAL_CACHE == null)
			{
				INTERNAL_CACHE = new Features();
				INTERNAL_CACHE.init();
			}
			return INTERNAL_CACHE;
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

	public static boolean gageIDExists(String gageID) throws SQLException
    {
        LOGGER.trace( "Checking if a gage with an id of {} has been defined...", gageID);

        boolean exists = Features.getCache().hasID( FeatureDetails.keyOfGageID( gageID ) );

        if (!exists)
        {
            String script = "SELECT EXISTS (" + NEWLINE +
                            "    SELECT 1" + NEWLINE +
                            "    FROM wres.Feature" + NEWLINE +
                            "    WHERE gage_id = '" + gageID + "'" + NEWLINE +
                            ");";

            exists = Database.getResult(script, "exists");
        }

        return exists;
    }
	
	/**
	 * Returns the ID of a Feature from the global cache based on a full Feature specification
	 * @param detail The full specification for a Feature
	 * @return The ID for the specified feature
	 * @throws SQLException Thrown if the ID could not be retrieved from the Database
	 */
	public static Integer getFeatureID(FeatureDetails detail) throws SQLException
	{
	    LOGGER.trace("getFeatureID - args {}", detail);
		return getCache().getID(detail);
	}

	public static Integer getFeatureID( FeatureDetails.FeatureKey key) throws SQLException
    {
        if (!Features.getCache().hasID( key ))
        {
            Features.getCache().addElement( new FeatureDetails( key ) );
        }

        return Features.getCache().getID( key );
    }

    public static Integer getFeatureID( Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        return Features.getFeatureID( new FeatureDetails.FeatureKey( comid, lid, gageID, huc ));
    }

    public static Integer getFeatureIDByLocation(String lid, String featureName)
            throws SQLException
    {
        LOGGER.trace("getFeatureID - args {} ; {}", lid, featureName);
        FeatureDetails detail = new FeatureDetails();
        detail.setLid( lid);
        detail.setFeatureName( featureName );
        return getFeatureID(detail);
    }

    public static Integer getFeatureIDByGageID(String gageID)
            throws SQLException
    {
        return getFeatureID( FeatureDetails.keyOfGageID( gageID ) );
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

    public static FeatureDetails getDetails(Feature feature) throws SQLException
    {
        Integer ID = Features.getFeatureID( feature );
        return Features.getCache().get( ID );
    }

    public static Set<FeatureDetails> getAllDetails( ProjectConfig projectConfig ) throws SQLException
    {
        Set<FeatureDetails> features = new HashSet<>(  );
        boolean hasNetCDF = false;
        boolean hasUSGS = false;

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getLeft().getSource())
        {
            hasNetCDF = hasNetCDF || source.getFormat() == Format.NET_CDF;
            hasUSGS = hasUSGS || source.getFormat() == Format.USGS;

            if (hasUSGS && hasNetCDF)
            {
                break;
            }
        }

        for ( DataSourceConfig.Source source : projectConfig.getInputs().getRight().getSource())
        {
            hasNetCDF = hasNetCDF || source.getFormat() == Format.NET_CDF;
            hasUSGS = hasUSGS || source.getFormat() == Format.USGS;

            if (hasUSGS && hasNetCDF)
            {
                break;
            }
        }

        if (projectConfig.getInputs().getBaseline() != null)
        {
            for ( DataSourceConfig.Source source : projectConfig.getInputs()
                                                                .getBaseline()
                                                                .getSource() )
            {
                hasNetCDF = hasNetCDF || source.getFormat() == Format.NET_CDF;
                hasUSGS = hasUSGS || source.getFormat() == Format.USGS;

                if ( hasUSGS && hasNetCDF )
                {
                    break;
                }
            }
        }

        for (Feature feature : projectConfig.getPair().getFeature())
        {
            for (FeatureDetails details : Features.getAllDetails( feature ))
            {
                // If we aren't using NetCDF or USGS data, we don't need to worry
                // about identifiers on the locations
                if (!(hasNetCDF || hasUSGS))
                {
                    features.add( details );
                }
                // If we are using both NetCDF and USGS data, we need both Gage IDs
                // and indexes for NetCDF files to be able to load any sort of
                // data for evaluation
                else if ((hasNetCDF && details.getNwmIndex() != null) &&
                         (hasUSGS && Strings.hasValue( details.getGageID() )))
                {
                    features.add( details );
                }
                // If we are using NetCDF data, we need indexes to determine what
                // data to retrieve
                else if (hasNetCDF && details.getNwmIndex() != null)
                {
                    features.add( details );
                }
                // If we are using USGS data, we need a gageID or we won't be
                // able to retrieve data
                else if (hasUSGS && Strings.hasValue( details.getGageID() ))
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
                    else if (hasUSGS && !Strings.hasValue( details.getGageID() ))
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

    public static Set<FeatureDetails> getAllDetails(Feature feature) throws SQLException
    {
        Set<FeatureDetails> details = new HashSet<>(  );

        if (Strings.hasValue( feature.getHuc() ))
        {
            details.addAll( Features.getDetailsByHUC( feature.getHuc() ) );
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
            details.addAll(Features.getDetailsByRFC( feature.getRfc().toUpperCase() ));
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

        for (FeatureDetails detail : details)
        {
            detail.setAliases( feature.getAlias() );
        }

        return details;
    }

    public static FeatureDetails getDetails(Integer comid, String lid, String gageID, String huc)
            throws SQLException
    {
        Integer ID = Features.getFeatureID( comid, lid, gageID, huc );
        return Features.getCache().get( ID );
    }

    public static FeatureDetails getDetailsByLID(String lid) throws SQLException
    {
        return Features.getDetails( null, lid, null, null );
    }

    public static FeatureDetails getDetailsByGageID(String gageID) throws SQLException
    {
        return Features.getDetails( null, null, gageID, null );
    }

    public static List<FeatureDetails> getDetailsByCoordinates(Float longitude, Float latitude, Float range)
            throws SQLException
    {
        Double radianLatitude = Math.toRadians( latitude );

        // This is the approximate distance between longitudinal degrees at
        // the equator
        Double distanceAtEquator = 111321.0;

        // This is an approximation
        Double distanceOfOneDegree = Math.cos(radianLatitude) * distanceAtEquator;

        // We take the max between the approximate distance and 0.00005 because
        // 0.00005 serves as a decent epsilon for database distance comparison.
        // If the distance is much smaller than that, float point error
        // can exclude the requested location, even if the coordinates were
        // spot on.
        Double rangeInDegrees = Math.max( range / distanceOfOneDegree, 0.00005);

        // TODO: Convert to database function
        String script = "";
        script += "WITH feature_and_distance AS" + NEWLINE;
        script += "(" + NEWLINE;
        script += "    SELECT SQRT((" + latitude + " - latitude)^2 + (" + longitude + " - longitude)^2) AS distance, *" + NEWLINE;
        script += "    FROM wres.Feature" + NEWLINE;
        script += ")" + NEWLINE;
        script += "SELECT *" + NEWLINE;
        script += "FROM feature_and_distance" + NEWLINE;
        script += "WHERE distance <= " + rangeInDegrees + ";";

        Connection connection = null;
        ResultSet closestFeatures = null;
        List<FeatureDetails> features = new ArrayList<>(  );

        try
        {
            connection = Database.getHighPriorityConnection();
            closestFeatures = Database.getResults( connection, script );

            while (closestFeatures.next())
            {
                FeatureDetails details = new FeatureDetails( closestFeatures );
                features.add( details );
                Features.getCache().add( details );
            }
        }
        finally
        {
            if (closestFeatures != null)
            {
                closestFeatures.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }

        return features;


    }

    public static List<FeatureDetails> getDetailsByHUC(String huc)
            throws SQLException
    {
        String script = "";

        script += "SELECT *" + NEWLINE;
        script += "FROM wres.Feature" + NEWLINE;
        script += "WHERE huc LIKE '" + huc + "%';";

        Connection connection = null;
        ResultSet hucFeatures = null;
        List<FeatureDetails> features = new ArrayList<>(  );

        try
        {
            connection = Database.getHighPriorityConnection();
            hucFeatures = Database.getResults( connection, script );

            while (hucFeatures.next())
            {
                FeatureDetails details = new FeatureDetails( hucFeatures );
                features.add( details );
                Features.getCache().add( details );
            }
        }
        finally
        {
            if (hucFeatures != null)
            {
                hucFeatures.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }

        return features;
    }

    public static List<FeatureDetails> getDetailsByRFC(String rfc)
            throws SQLException
    {
        String script = "";
        script += "SELECT *" + NEWLINE;
        script += "FROM wres.Feature" + NEWLINE;
        script += "WHERE rfc = '" + rfc + "';";

        Connection connection = null;
        ResultSet rfcFeatures = null;
        List<FeatureDetails> features = new ArrayList<>(  );

        try
        {
            connection = Database.getHighPriorityConnection();
            rfcFeatures = Database.getResults( connection, script );

            while (rfcFeatures.next())
            {
                FeatureDetails details = new FeatureDetails( rfcFeatures );
                features.add( details );
                Features.getCache().add( details );
            }
        }
        finally
        {
            if (rfcFeatures != null)
            {
                rfcFeatures.close();
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection( connection );
            }
        }

        return features;
    }

    public static Integer getVariablePositionID(Integer comid, String lid, String gageID, String huc, Integer variableID)
            throws SQLException
    {
        return Features.getDetails( comid, lid, gageID, huc ).getVariablePositionID( variableID );
    }

    public static Integer getVariablePositionIDByLID(String lid, Integer variableID)
            throws SQLException
    {
        return Features.getDetailsByLID( lid ).getVariablePositionID( variableID );
    }

    public static Integer getVariablePositionIDByGageID(String gageID, Integer variableID)
            throws SQLException
    {
        return Features.getDetailsByGageID( gageID ).getVariablePositionID( variableID );
    }

    public static Integer getVariablePositionID(Feature feature, Integer variableID)
            throws SQLException
    {
        return Features.getDetails( feature ).getVariablePositionID( variableID );
    }
	
	@Override
	protected int getMaxDetails() {
		return 1000;
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
