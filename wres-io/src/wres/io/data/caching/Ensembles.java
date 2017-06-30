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

import wres.io.data.details.EnsembleDetails;
import wres.io.data.details.EnsembleDetails.EnsembleKey;
import wres.util.Collections;
import wres.io.utilities.Database;
import wres.io.utilities.Debug;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class Ensembles extends Cache<EnsembleDetails, EnsembleKey> {

    private static final Logger LOGGER = LoggerFactory.getLogger(Ensembles.class);

    /**
     *  Internal cache that will store a global collection of details whose details may be accessed through static methods
     */
	private static Ensembles internalCache = new Ensembles();
	
	/**
	 * Returns the ID of an Ensemble from the global cache
	 * @param detail Specifications about an ensemble to retrieve
	 * @return The ID of the specified ensemble
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(EnsembleDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	/**
	 * Returns the ID of an ensemble from the global cache on its name
	 * @param name The name of the ensemble whose ID to retrieve
	 * @return The ID of the Ensemble in question
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(String name) throws Exception {
		return internalCache.getID(name);
	}
	
	/**
	 * Returns the ID of an Ensemble from the global cache based on its name and member ID
	 * @param name The name of the Ensemble to retrieve
	 * @param memberID The Member ID of the Ensemble to retrieve
	 * @return The ID of the Ensemble
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(String name, String memberID) throws Exception {
		return internalCache.getID(name, memberID);
	}
	
	/**
	 * Returns the ID of an Ensemble from the global cache based on the combination of its name, member ID, and qualifier
	 * @param name The name of the Ensemble to retrieve
	 * @param memberID The Member ID of the Ensemble to retrieve
	 * @param qualifierID The qualifier of the Ensemble to retrieve
	 * @return The ID of the Ensemble
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(String name, String memberID, String qualifierID) throws Exception {
		return internalCache.getID(name, memberID, qualifierID);
	}
	
	/**
	 * Returns the ID of the Ensemble from the global cache based on its cached key
	 * @param grouping The key for the Ensemble in the cache
	 * @return The ID of the Ensemble
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public static Integer getEnsembleID(EnsembleKey grouping) throws Exception
	{
		return internalCache.getID(grouping);
	}
	
	@Override
	protected int getMaxDetails() {
		return 500;
	}
    
    /**
     * Returns the ID of an ensemble from the instanced cache based on its name
     * @param name The name of the ensemble whose ID to retrieve
     * @return The ID of the Ensemble in question
     * @throws Exception Thrown if the ID could not be retrieved from the database
     */
	public Integer getID(String name) throws Exception
	{
		return getID(EnsembleDetails.createKey(name, null, null));
	}
    
    /**
     * Returns the ID of an Ensemble from the instanced cache based on its name and member ID
     * @param name The name of the Ensemble to retrieve
     * @param memberID The Member ID of the Ensemble to retrieve
     * @return The ID of the Ensemble
     * @throws Exception Thrown if the ID could not be retrieved from the database
     */
	public Integer getID(String name, String memberID) throws Exception
	{
		return getID(EnsembleDetails.createKey(name, null, memberID));
	}
    
    /**
     * Returns the ID of an Ensemble from the instanced cache based on the combination of 
     * its name, member ID, and qualifier
     * @param name The name of the Ensemble to retrieve
     * @param memberID The Member ID of the Ensemble to retrieve
     * @param qualifierID The qualifier of the Ensemble to retrieve
     * @return The ID of the Ensemble
     * @throws Exception Thrown if the ID could not be retrieved from the database
     */
	public Integer getID(String name, String memberID, String qualifierID) throws Exception {
		return getID(EnsembleDetails.createKey(name, qualifierID, memberID));
	}
	
	@Override
	public Integer getID(EnsembleDetails detail) throws Exception {
		return getID(detail.getKey());
	}

	@Override
	public synchronized Integer getID(final EnsembleKey grouping) throws Exception
	{
		// Maps keys to the number of similarities between them and the passed in grouping
		Map<Integer, List<EnsembleKey>> possibleKeys = new TreeMap<>();
		
		// Listing of keys with the same amount of similarities
		List<EnsembleKey> similarKeys = null;		

		// The closest existing key to what we are trying to retrieve
		EnsembleKey mostSimilar = null;
        Integer ID = keyIndex.get(grouping);
		
		// If no identical groupings are found and the grouping isn't full, attempt to find a similar one
		if (keyIndex.size() > 0)
		{
			for (EnsembleKey key : keyIndex.keySet())
			{
			    ID = keyIndex.get(grouping);
	            if (ID != null)
	            {
	                break;
	            }
	            
				Integer similarity = grouping.getSimilarity(key);
				
				if (similarity == 3)
				{
					mostSimilar = grouping;
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
			
			if (possibleKeys.containsKey(2))
			{
				similarKeys = possibleKeys.get(2);
				
				mostSimilar = Collections.find(similarKeys, (EnsembleKey key) -> {
					return key.getEnsembleName().equalsIgnoreCase(grouping.getEnsembleName()) && key.getMemberIndex().equalsIgnoreCase(grouping.getMemberIndex());
				});
			}
			else if (possibleKeys.containsKey(1)) {
				similarKeys = possibleKeys.get(1);
				
				mostSimilar = Collections.find(similarKeys, (EnsembleKey key) -> {
					return key.getEnsembleName().equalsIgnoreCase(grouping.getEnsembleName());
				});
				
				if (mostSimilar == null)
				{
					mostSimilar = Collections.find(similarKeys, (EnsembleKey key) -> {
						return key.getMemberIndex().equalsIgnoreCase(grouping.getMemberIndex());
					});
				}
			}
		}
		
		// If a similar key wasn't found, insert a new element based on the grouping
		if (ID == null && mostSimilar == null)
		{
			mostSimilar = grouping;
			EnsembleDetails detail = new EnsembleDetails();
			detail.setEnsembleName(grouping.getEnsembleName());
			detail.setEnsembleMemberID(grouping.getMemberIndex());
			detail.qualifierID = grouping.getQualifierID();
			addElement(detail);
		}
		
		if (mostSimilar != null)
		{
		    ID = keyIndex.get(mostSimilar);
		}

		return ID;
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
            connection = Database.getConnection();
            ensembleQuery = connection.createStatement();
            
            String loadScript = "SELECT ensemble_id, ensemble_name, qualifier_id, ensemblemember_id" + NEWLINE;
            loadScript += "FROM wres.ensemble" + NEWLINE;
            loadScript += "LIMIT " + getMaxDetails();
            
            ensembles = ensembleQuery.executeQuery(loadScript);
            
            EnsembleDetails detail = null;
            
            while (ensembles.next()) {
                detail = new EnsembleDetails();
                detail.setEnsembleName(ensembles.getString("ensemble_name"));
                detail.setEnsembleMemberID(String.valueOf(ensembles.getInt("ensemblemember_id")));
                detail.qualifierID = ensembles.getString("qualifier_id");
                detail.setID(ensembles.getInt("ensemble_id"));
                
                this.add(detail.getKey(), detail.getId());
            }
        }
        catch (SQLException error)
        {
            Debug.error(LOGGER, "An error was encountered when trying to populate the Ensemble cache.");
            Debug.error(LOGGER, error);
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
                    Debug.error(LOGGER, "An error was encountered when trying to close the result set containing ensemble information.");
                    Debug.error(LOGGER, e);
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
                    Debug.error(LOGGER, "An error was encountered when trying to close the query that loaded ensemble information.");
                    Debug.error(LOGGER, e);
                }
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
	}
}
