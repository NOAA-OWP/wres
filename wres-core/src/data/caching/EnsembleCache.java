/**
 * 
 */
package data.caching;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import collections.Triplet;
import data.details.EnsembleDetails;
import util.Database;
import util.Utilities;

/**
 * Cached details about Ensembles from the database
 * @author Christopher Tubbs
 */
public class EnsembleCache extends Cache<EnsembleDetails, Triplet<String, String, String>> {

    private final Logger LOGGER = LoggerFactory.getLogger(getClass());

    /**
     *  Internal cache that will store a global collection of details whose details may be accessed through static methods
     */
	private static EnsembleCache internalCache = new EnsembleCache();
	
	private EnsembleCache()
	{
	    super();
        try
        {
            this.init();
        }
        catch (SQLException se)
        {
            LOGGER.error("Could not init EnsembleCache due to {}", se);
        }
	}
	
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
	public static Integer getEnsembleID(Triplet<String, String, String> grouping) throws Exception
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
		return getID(new Triplet<String, String, String>(name, null, null));
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
		return getID(new Triplet<String, String, String>(name, memberID, null));
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
		return getID(new Triplet<String, String, String>(name, memberID, qualifierID));
	}
	
	@Override
	public Integer getID(EnsembleDetails detail) throws Exception {
		return getID(detail.getKey());
	}

	@Override
	/**
	 * Attempts to find the correct ID from the instanced cache based on the given triplet 
	 * (item one = name, item two = member id, item three = qualifier). Since the source might not 
	 * always supply all three values, we have to attempt to find the loaded key that is closest to the one
	 * requested. If there aren't any values similar enough to match, a new ensemble is added.
	 */
	public synchronized Integer getID(final Triplet<String, String, String> grouping) throws Exception
	{
		// Maps keys to the number of similarities between them and the passed in grouping
		Map<Byte, ArrayList<Triplet<String, String, String>>> possibleKeys = new TreeMap<Byte, ArrayList<Triplet<String, String, String>>>();
		
		// Listing of keys with the same amount of similarities
		ArrayList<Triplet<String, String, String>> similarKeys = null;		

		// The closest existing key to what we are trying to retrieve
		Triplet<String, String, String> mostSimilar = null;
        Integer ID = keyIndex.get(grouping);
		
		// If no identical groupings are found and the grouping isn't full, attempt to find a similar one
		if (keyIndex.size() > 0)// && mostSimilar == null && !grouping.isFull())
		{
			for (Triplet<String, String, String> key : keyIndex.keySet())
			{
			    ID = keyIndex.get(grouping);
	            if (ID != null)
	            {
	                break;
	            }
	            
				byte similarity = keySimilarity(grouping, key);
				if (similarity == 3)
				{
					mostSimilar = grouping;
					break;
				}
				if (similarity > 0)
				{					
					if (!possibleKeys.containsKey(similarity))
					{
						possibleKeys.put(similarity, new ArrayList<Triplet<String, String, String>>());
					}
					
					possibleKeys.get(similarity).add(key);
				}
			}
			
			if (possibleKeys.containsKey(2))
			{
				similarKeys = possibleKeys.get(2);
				
				mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
					return key.getItemOne() == grouping.getItemOne() && key.getItemTwo() == grouping.getItemTwo();
				});
				
				if (mostSimilar == null)
				{
					mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
						return key.getItemOne() == grouping.getItemOne() && key.getItemThree() == grouping.getItemThree();
					});
				}
				
				if (mostSimilar == null)
				{
					mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
						return key.getItemTwo() == grouping.getItemTwo();
					});
				}
			}
			else if (possibleKeys.containsKey(1)) {
				similarKeys = possibleKeys.get(1);
				
				mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
					return key.getItemOne() == grouping.getItemOne();
				});
				
				if (mostSimilar == null)
				{
					mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
						return key.getItemTwo() == grouping.getItemTwo();
					});
				}
				
				if (mostSimilar == null)
				{
					mostSimilar = Utilities.find(similarKeys, (Triplet<String, String, String> key) -> {
						return key.getItemThree() == grouping.getItemThree();
					});
				}
			}
		}
		
		// If a similar key wasn't found, insert a new element based on the grouping
		if (ID == null && mostSimilar == null)
		{
			mostSimilar = grouping;
			EnsembleDetails detail = new EnsembleDetails();
			detail.setEnsembleName(grouping.getItemOne());
			detail.setEnsembleMemberID(grouping.getItemTwo());
			detail.qualifierID = grouping.getItemThree();
			addElement(detail);
		}
		
		if (mostSimilar != null)
		{
		    ID = keyIndex.get(mostSimilar);
		}

		return ID;
	}
	
	/**
	 * Determines if two keys for the ensemble are similar and counts the number of similarities
	 * 
	 * NOTE: Casing is ignored
	 * 
	 * @param possibleMatch A key that might match up to another key
	 * @param target The key to compare the possible match to
	 * @return 3 if all positions match, 2 if two of the positions match, 1 if only one position matches and
	 *  -1 if the key is definitely not a match 
	 */
	private static byte keySimilarity(Triplet<String, String, String> possibleMatch, Triplet<String, String, String> target)
	{
		if (possibleMatch.equals(target))
		{
			return 3;
		}
		else if (possibleMatch.isEmpty())
		{
			return -1;
		}
		
		byte similarity = 0;
		
		if (possibleMatch.getItemOne() != null && target.getItemOne() != null)
		{
			if( possibleMatch.getItemOne().equalsIgnoreCase(target.getItemOne()))
			{
				similarity++;
			}
			else
			{
				similarity = -1;
			}
		}
		
		if (target.getItemTwo() != null && possibleMatch.getItemTwo() != null && similarity >= 0)
		{
			if (possibleMatch.getItemTwo().equalsIgnoreCase(target.getItemTwo()))
			{
				similarity++;
			}
			else
			{
				similarity = -1;
			}
		}
		
		if (possibleMatch.getItemThree() != null && target.getItemThree() != null && similarity >= 0)
		{
			if (possibleMatch.getItemThree().equalsIgnoreCase(target.getItemThree()))
			{
				similarity++;
			}
			else
			{
				similarity = -1;
			}
		}
		
		return similarity;
	}

	/**
	 * Loads all pre-existing Ensembles into the global cache
	 * @throws SQLException Thrown if the values could not be loaded from the database or added to the cache
	 */
	public synchronized static void initialize() throws SQLException
	{
	    internalCache.init();
	}
	
	/**
	 * Loads all pre-existing Ensembles into the instanced cache
	 */
	@Override
    public synchronized void init() throws SQLException
	{
        Connection connection = null;
        Statement ensembleQuery = null;
        ResultSet ensembles = null;
        try
        {
            connection = Database.getConnection();
            ensembleQuery = connection.createStatement();
            
            String loadScript = "SELECT ensemble_id, ensemble_name, qualifier_id, ensemblemember_id" + newline;
            loadScript += "FROM wres.ensemble" + newline;
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
            LOGGER.error("An error was encountered when trying to populate the Ensemble cache. {}",
                         error);
            throw error;
        }
        finally
        {
            if (ensembles != null)
            {
                ensembles.close();
            }

            if (ensembleQuery != null)
            {
                ensembleQuery.close();
            }

            if (connection != null)
            {
                Database.returnConnection(connection);
            }
        }
	}
}
