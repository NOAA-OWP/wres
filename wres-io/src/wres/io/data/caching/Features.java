package wres.io.data.caching;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.data.details.FeatureDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.Strings;

/**
 * Caches details about Features
 * @author Christopher Tubbs
 */
@Internal(exclusivePackage = "wres.io")
public class Features extends Cache<FeatureDetails, String>
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

	public static boolean exists(String locationId) throws SQLException
	{
		LOGGER.trace( "Checking if a location named '{}' has been defined...", locationId );
		boolean exists = getCache().hasID( locationId );

		if (!exists)
		{
            String script = "SELECT EXISTS (" + NEWLINE +
                            "		SELECT 1" + NEWLINE +
                            "		FROM wres.Feature" + NEWLINE +
                            "		WHERE lid = '" + locationId + "'" + NEWLINE
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
	public static Integer getFeatureID(FeatureDetails detail) throws SQLException {
	    LOGGER.trace("getFeatureID - args {}", detail);
		return getCache().getID(detail);
	}
	
	/**
	 * Returns the ID of a Feature from the global cache based on its LID and name
	 * @param lid The location ID of the Feature
	 * @param stationName The name of the feature
	 * @return The ID of the Feature
	 * @throws SQLException Thrown if the ID could not be loaded from the database
	 */
	public static Integer getFeatureID(String lid, String stationName) throws SQLException {
        LOGGER.trace("getFeatureID - args {} ; {}", lid, stationName);
		FeatureDetails detail = new FeatureDetails();
		detail.setLID(lid);
		detail.station_name = stationName;
		return getFeatureID(detail);
	}
	
	/**
	 * Returns the ID of the variable position that a Feature in the global cache is tied to in the Database
	 * @param lid The location ID of the feature to look for
	 * @param stationName The name of the Feature to look for
	 * @param variableID The ID of the variable to look for
	 * @return The ID of the found variable position
	 * @throws SQLException Thrown if the variable position could not be loaded from the database
	 */
	public static Integer getVariablePositionID(String lid, String stationName, Integer variableID) throws SQLException {
        LOGGER.trace("getVariablePositionID - ars {} ; {} ; {}", lid, stationName, variableID);
        return getCache().getVarPosID(lid, stationName, variableID);
	}
    
    /**
     * Returns the ID of the variable position from the instanced cache that a Feature is tied to in the Database
     * @param lid The location ID of the feature to look for
     * @param stationName The name of the Feature to look for
     * @param variableID The ID of the variable to look for
     * @return The ID of the found variable position
     * @throws SQLException Thrown if the variable position could not be loaded from the database
     */
	private Integer getVarPosID(String lid, String stationName, Integer variableID) throws SQLException {
        LOGGER.trace("getVarPosID - args {} ; {} ; {}", lid, stationName, variableID);
		if (!this.getKeyIndex().containsKey(lid))
		{
			FeatureDetails detail = new FeatureDetails();
			detail.setLID(lid);
			detail.station_name = stationName;
			getID(detail);
		}

		FeatureDetails detail;

		try
		{
	        detail = this.getDetails().get(this.getKeyIndex().get(lid));
		}
		catch (NullPointerException error)
		{
		    LOGGER.error("");
			LOGGER.error("A variable position could not be retrieved with the parameters of:");
			LOGGER.error("\tLID: " + lid);
			LOGGER.error("\tStation Name: " + stationName);
			LOGGER.error("\tVariableID: " + variableID);
			LOGGER.error("");
			LOGGER.error(Strings.getStackTrace(error));
			LOGGER.error("");
            throw error;
		}

		return detail.getVariablePositionID(variableID);
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
