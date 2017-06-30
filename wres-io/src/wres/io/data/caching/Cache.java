package wres.io.data.caching;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import wres.io.data.details.CachedDetail;
import wres.util.Collections;

/**
 * An collection of details about concepts stored within the database
 * @author Christopher Tubbs
 * @param <T> The type of detail within the database
 * @param <U> The key for the type within the database
 */
abstract class Cache<T extends CachedDetail<T, U>, U extends Comparable<U>> {
    protected static final String NEWLINE = System.lineSeparator();
    
    // Guarded by lock
	protected final LinkedHashMap<U, Integer> keyIndex;
	protected ConcurrentSkipListMap<Integer, T> details;

	public Cache() {
	    keyIndex = new LinkedHashMap<U, Integer>(getMaxDetails(), 0.75F, true){
	       
	        @Override
	        protected boolean removeEldestEntry(java.util.Map.Entry<U, Integer> eldest)
	        {
	            boolean remove = size() > getMaxDetails();
	            
	            if (details != null && remove)
	            {
	                details.remove(eldest.getValue());
	            }
	            
	            return remove;
	        }
	    };
	    
	    this.init();
	}
	/**
	 * @return The maximum number of details that may be cached at any given time
	 */
	protected abstract int getMaxDetails();

	 /**
     * Loads all preexisting data into the cache
     */
	protected abstract void init();
	
	/**
	 * Removes all items from the details and keys caches for the instance
	 */
    void clearCache () {
	    synchronized(keyIndex)
	    {
	        keyIndex.clear();
	    }
	}

	T get (int id)
	{
		T detail = null;

		if (this.details != null)
		{
			detail = this.details.get(id);
		}

		return detail;
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
		U key = detail.getKey();
		if (!hasID(key)) {
			addElement(detail);
		}
		
		return getID(key);
	}
	
	/**
	 * Retrieves the key for details in the instance cache based on its ID from the database
	 * @param id The ID of the key to retrieve
	 * @return The key tied to the ID. Returns <b>null</b> if the key doesn't exist within the cache
	 */
    U getKey (int id) {
		U key;
		
		synchronized (keyIndex)
		{
		    key = Collections.getKeyByValue(keyIndex, id);
		}

		return key;
	}
	
	/**
	 * Adds the details to the instance cache. If the details don't exist in the database, they are added.
	 * <br><br>
	 * Since only a limited amount of data is stored within the instanced cache, the least recently used item from the
	 * instanced cache is removed if the amount surpasses the maximum allowable number of stored details
	 * @param element The details to add to the instanced cache
	 * @throws SQLException Thrown if the ID of the element could not be retrieved or the cache could not be
	 * updated
	 */
	public void addElement(T element) throws SQLException
	{
		element.save();
		add(element.getKey(), element.getId());
		if (this.details != null && !this.details.containsKey(element.getId()))
		{
		    this.details.put(element.getId(), element);
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
		
		synchronized (keyIndex)
		{
    		if (keyIndex.containsKey(key)) {
    			id = keyIndex.get(key);
    		}
		}
		
		return id;
	}
	
	boolean hasID (U key)
	{
	    boolean hasIt;
	    
	    synchronized (keyIndex)
	    {
	        hasIt = keyIndex.containsKey(key);
	    }
	    
	    return hasIt;
	}
	
	protected void add(U key, Integer id)
	{
	    synchronized (keyIndex) {
	        keyIndex.put(key, id);
	    }
	}
}
