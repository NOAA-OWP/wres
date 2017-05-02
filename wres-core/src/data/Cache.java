/**
 * 
 */
package data;

import java.sql.SQLException;
import java.util.concurrent.ConcurrentSkipListMap;

import collections.RecentUseList;
import data.details.CachedDetail;

/**
 * An collection of details about concepts stored within the database
 * @author Christopher Tubbs
 * @param <T> The type of detail within the database
 * @param <U> The key for the type within the database
 */
abstract class Cache<T extends CachedDetail<T, U>, U extends Comparable<U>> {
    protected static final String newline = System.lineSeparator();
    
	protected ConcurrentSkipListMap<Integer, T> details = new ConcurrentSkipListMap<Integer, T>();
	protected ConcurrentSkipListMap<U, Integer> keyIndex = new ConcurrentSkipListMap<U, Integer>();
	protected RecentUseList<Integer> recentlyUsedIDs = new RecentUseList<Integer>();

	/**
	 * @return The maximum number of details that may be cached at any given time
	 */
	protected abstract int getMaxDetails();

	 /**
     * Loads all preexisting data into the cache
     * @throws SQLException Thrown if data couldn't be loaded from the database or stored in the cache
     */
	protected abstract void init() throws SQLException;
	
	/**
	 * Removes all items from the details and keys caches for the instance
	 */
	protected synchronized void clearCache() {
        details.clear();
        keyIndex.clear();
        recentlyUsedIDs.clear();
	}
	
	/**
	 * Returns the ID of the set of details in the database. If the ID for the details is not
	 * present in the instance cache, it is added.
	 * @param detail The definition of the object in the database to get information for
	 * @return The ID for the details in the database
	 * @throws Exception Thrown if the ID could not be retrieved from the database
	 */
	public Integer getID(T detail) throws Exception
	{
		if (!keyIndex.containsKey(detail.getKey())) {
			addElement(detail);
		}
		
		return detail.getId();
	}
	
	/**
	 * Retrieves the key for details in the instance cache based on its ID from the database
	 * @param id The ID of the key to retrieve
	 * @return The key tied to the ID. Returns <b>null</b> if the key doesn't exist within the cache
	 */
	public U getKey(int id) {
		U key = null;
		
		if (details.containsKey(id)) {
			key = details.get(id).getKey();
		}

		return key;
	}
	
	/**
	 * Adds the details to the instance cache. If the details don't exist in the database, they are added.
	 * <br><br>
	 * Since only a limited amount of data is stored within the instanced cache, the least recently used item from the
	 * instanced cache is removed if the amount surpasses the maximum allowable number of stored details
	 * @param element The details to add to the instanced cache
	 * @throws Exception Thrown if the ID of the element could not be retrieved or the cache could not be
	 * updated
	 */
	public void addElement(T element) throws Exception {
		element.save();
		recentlyUsedIDs.add(element.getId());

		details.put(element.getId(), element);
		keyIndex.put(element.getKey(), element.getId());
		
		if (recentlyUsedIDs.size() >= getMaxDetails()) {
			int lastID = recentlyUsedIDs.drop_last();
			T last = details.get(lastID);
			details.remove(lastID);
			keyIndex.remove(last.getKey());
		}
	}	
	
	/**
	 * Returns the ID of the cached details based on its key
	 * @param key A key used to index details
	 * @return The ID of a specific set of details
	 * @throws Exception Thrown if the ID could not be retrieved
	 */
	public Integer getID(U key) throws Exception
	{
		Integer id = null;
		
		if (keyIndex.containsKey(key)) {
			id = keyIndex.get(key);
		}
		
		return id;
	}
}
