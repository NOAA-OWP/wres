/**
 * 
 */
package data;

import collections.Triplet;
import data.details.EnsembleDetails;

/**
 * @author ctubbs
 *
 */
public class EnsembleCache extends Cache<EnsembleDetails, Triplet<String, String, String>> {

	private static EnsembleCache internalCache = new EnsembleCache();
	
	public static Integer getEnsembleID(EnsembleDetails detail) throws Exception {
		return internalCache.getID(detail);
	}
	
	@Override
	protected Integer getMaxDetails() {
		return 500;
	}
	
	public Integer getID(String name)
	{
		return getID(new Triplet<String, String, String>(name, null, null));
	}
	
	public Integer getID(String name, String memberID)
	{
		return getID(new Triplet<String, String, String>(name, memberID, null));
	}

	public Integer getID(Triplet<String, String, String> grouping)
	{
		/**
		 *  TODO: This needs to perform a fuzzy search; it needs to group on itemOne (name),
		 *  itemTwo(member id), and itemThree(qualifier). If any are null, it needs to try and
		 *  find the closest approximation. 
		 */
		
		return 0;
	}
}
