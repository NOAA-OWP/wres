package wres.io.data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.EnsembleCondition;
import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.io.utilities.Database;
import wres.util.Collections;
import wres.util.NetCDF;
import wres.util.Strings;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class Ensembles extends Cache<EnsembleDetails, EnsembleKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ensembles.class);
    private static final Object CACHE_LOCK = new Object();

    /**
     *  Internal cache that will store a global collection of details whose details may be accessed through static methods
     */
	private static Ensembles instance = null;

	private static Ensembles getCache()
	{
		synchronized (CACHE_LOCK)
		{
			if ( instance == null)
			{
				instance = new Ensembles();
				instance.init();
			}
			return instance;
		}
	}

	public static Integer getEnsembleID( NetCDF.Ensemble ensemble )
            throws SQLException
    {
		return Ensembles.getCache().getID( ensemble.getName(),
                                           ensemble.getTMinus(),
                                           ensemble.getQualifier() );
	}

	public static Integer getEnsembleID( EnsembleCondition ensemble)
			throws SQLException
	{
		return Ensembles.getCache().getID( ensemble.getName(), ensemble.getMemberId(), ensemble.getQualifier() );
	}
	
	/**
	 * Returns the ID of an Ensemble from the global cache based on the combination of its name, member ID, and qualifier
	 * @param name The name of the Ensemble to retrieve
	 * @param memberID The Member ID of the Ensemble to retrieve
	 * @param qualifierID The qualifier of the Ensemble to retrieve
	 * @return The ID of the Ensemble
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(String name, String memberID, String qualifierID) throws SQLException {
		return getCache().getID(name, memberID, qualifierID);
	}
	
	@Override
	protected int getMaxDetails() {
		return 500;
	}
    
    /**
     * Returns the ID of an ensemble from the instanced cache based on its name
     * @param name The name of the ensemble whose ID to retrieve
     * @return The ID of the Ensemble in question
     * @throws SQLException Thrown if the ID could not be retrieved from the database
     */
	public Integer getID(String name) throws SQLException
	{
		return getID(EnsembleDetails.createKey(name, null, null));
	}

	/**
     * Returns the ID of an Ensemble from the instanced cache based on the combination of 
     * its name, member ID, and qualifier
     * @param name The name of the Ensemble to retrieve
     * @param memberID The Member ID of the Ensemble to retrieve
     * @param qualifierID The qualifier of the Ensemble to retrieve
     * @return The ID of the Ensemble
     * @throws SQLException Thrown if the ID could not be retrieved from the database
     */
	public Integer getID(String name, String memberID, String qualifierID) throws SQLException {
		return getID(EnsembleDetails.createKey(name, qualifierID, memberID));
	}
	
	@Override
	public Integer getID(EnsembleDetails detail) throws SQLException {
		return getID(detail.getKey());
	}

	/**
	 * Performs fuzzy matching to find the ID of the closest existing ID. If one
	 * doesn't exist, one is created.  Priority is given via: name, index, then qualifier
	 * @param grouping The key containing some permutation of name, index, and qualifier
	 * @return The ID representing the ensemble in the database
	 * @throws SQLException
	 */
	@Override
	public Integer getID(final EnsembleKey grouping) throws SQLException
	{
		// Maps keys to the number of similarities between them and the passed in grouping
		Map<Integer, List<EnsembleKey>> possibleKeys;

		// The closest existing key to what we are trying to retrieve
		EnsembleKey mostSimilar = null;

        Integer id = getKeyIndex().get(grouping);
		
		// If no identical groupings are found and the grouping isn't full, attempt to find a similar one
		if (id == null && getKeyIndex().size() > 0)
		{
			possibleKeys = this.getSimilarKeys( grouping );

			if (possibleKeys.containsKey( 3 ))
            {
                mostSimilar = possibleKeys.get(3).get( 0 );
            }
			else if (possibleKeys.containsKey(2))
			{
			    mostSimilar = this.getSecondDegreeMatch( grouping, possibleKeys.get( 2 ) );
			}
			else if (possibleKeys.containsKey(1)) {
				
				mostSimilar = Collections.find (
                        possibleKeys.get(1),
						(EnsembleKey key) ->
								key.getEnsembleName()
								   .equalsIgnoreCase( grouping.getEnsembleName())
				);
			}
		}
		
		// If a similar key wasn't found, insert a new element based on the grouping
		if (id == null && mostSimilar == null)
		{
			EnsembleDetails detail = new EnsembleDetails();
			detail.setEnsembleName(grouping.getEnsembleName());
			detail.setEnsembleMemberID(grouping.getMemberIndex());
			detail.setQualifierID(grouping.getQualifierID());
			addElement(detail);
            id = getKeyIndex().get(grouping);
		}
		else if (id == null)
		{
		    id = getKeyIndex().get(mostSimilar);
		}

		return id;
	}

	private Map<Integer, List<EnsembleKey>> getSimilarKeys(EnsembleKey originalKey)
    {
		// Maps keys to the number of similarities between them and the passed in grouping
		Map<Integer, List<EnsembleKey>> possibleKeys = new TreeMap<>();

		for (EnsembleKey key : getKeyIndex().keySet())
		{
			Integer similarity = originalKey.getSimilarity(key);

			if (similarity == 3)
			{
			    possibleKeys = new TreeMap<>(  );
			    possibleKeys.put( 3, new ArrayList<>() );
			    possibleKeys.get(3).add( key );
				break;
			}
			if (similarity > 0)
			{
				if (!possibleKeys.containsKey(similarity))
				{
					possibleKeys.put(similarity, new ArrayList<>());
				}

				possibleKeys.get(similarity).add(key);
			}
		}

		return possibleKeys;
    }

    private EnsembleKey getSecondDegreeMatch(EnsembleKey key, List<EnsembleKey> similarKeys)
    {
        EnsembleKey match = null;

        for (EnsembleKey similarKey : similarKeys)
        {
            if ( similarKey.getEnsembleName().equalsIgnoreCase( key.getEnsembleName() ) &&
                 similarKey.getMemberIndex().equalsIgnoreCase( key.getMemberIndex() ))
            {
                match = similarKey;
            }
        }

        return match;
    }
	
	/**
	 * Loads all pre-existing Ensembles into the instanced cache
	 */
	@Override
    protected synchronized void init()
	{
        Connection connection = null;
        Statement ensembleQuery = null;
        ResultSet ensembles = null;
        try
        {
            connection = Database.getHighPriorityConnection();
            ensembleQuery = connection.createStatement();
            
            String loadScript = "SELECT ensemble_id, ensemble_name, qualifier_id, ensemblemember_id" + NEWLINE;
            loadScript += "FROM wres.ensemble" + NEWLINE;
            loadScript += "LIMIT " + getMaxDetails();
            
            ensembles = ensembleQuery.executeQuery(loadScript);
            
            EnsembleDetails detail;
            
            while (ensembles.next()) {
                detail = new EnsembleDetails();
                detail.setEnsembleName(ensembles.getString("ensemble_name"));
                detail.setEnsembleMemberID(String.valueOf(ensembles.getInt("ensemblemember_id")));
                detail.setQualifierID(ensembles.getString("qualifier_id"));
                detail.setID(ensembles.getInt("ensemble_id"));
                
                this.add(detail.getKey(), detail.getId());
            }
        }
        catch (SQLException error)
        {
            LOGGER.error("An error was encountered when trying to populate the Ensemble cache.");
            LOGGER.error(Strings.getStackTrace(error));
        }
        finally
        {
            if (ensembles != null)
            {
                try
                {
                    ensembles.close();
                }
                catch(SQLException e)
                {
                    LOGGER.error("An error was encountered when trying to close the result set containing ensemble information.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (ensembleQuery != null)
            {
                try
                {
                    ensembleQuery.close();
                }
                catch(SQLException e)
                {
                    LOGGER.error("An error was encountered when trying to close the query that loaded ensemble information.");
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }

            if (connection != null)
            {
                Database.returnHighPriorityConnection(connection);
            }
        }
	}
}
