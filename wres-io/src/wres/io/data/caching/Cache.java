package wres.io.data.caching;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import wres.io.data.details.CachedDetail;
import wres.io.utilities.LRUMap;

/**
 * An collection of details about concepts stored within the database
 * @author Christopher Tubbs
 * @param <T> The type of detail within the database
 * @param <U> The key for the type within the database
 */
abstract class Cache<T extends CachedDetail<T, U>, U extends Comparable<U>> {
    static final String NEWLINE = System.lineSeparator();

	Map<U, Integer> keyIndex;
	private ConcurrentMap<Integer, T> details;
	private static final Object DETAIL_LOCK = new Object();
	private static final Object KEY_LOCK = new Object();

	Map<U, Integer> getKeyIndex()
    {
        if (keyIndex == null)
        {
        	keyIndex = new LRUMap<>( this.getMaxDetails(), eldest -> {
        		if (this.details != null)
				{
					details.remove( eldest.getValue() );
				}
			} );
        }

        return this.keyIndex;
    }

	final ConcurrentMap<Integer, T> getDetails()
	{
		this.initializeDetails();
		return this.details;
	}

	protected void initializeDetails()
	{
		synchronized ( DETAIL_LOCK )
		{
			if (this.details == null)
			{
				this.details = new ConcurrentHashMap<>( this.getMaxDetails() );
			}
		}
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
	    synchronized(KEY_LOCK)
	    {
	        this.getKeyIndex().clear();
	    }
	}

	T get (int id)
	{
		return this.getDetails().get(id);
	}
	
	/**
	 * Returns the ID of the set of details in the database. If the ID for the details is not
	 * present in the instance cache, it is added.
	 * @param detail The definition of the object in the database to get information for
	 * @return The ID for the details in the database
	 * @throws SQLException Thrown if the ID could not be retrieved from the database
	 */
	Integer getID( T detail ) throws SQLException
	{
		U key = detail.getKey();
		if (!hasID(key)) {
			addElement(detail);
		}
		
		return getID(key);
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
	void addElement( T element ) throws SQLException
	{
		element.save();
		add(element);
	}
	
	/**
	 * Returns the ID of the cached details based on its key
	 * @param key A key used to index details
	 * @return The ID of a specific set of details
	 * @throws SQLException Thrown if the ID could not be retrieved
	 */
	Integer getID( U key ) throws SQLException
    {
		Integer id = null;
		
		synchronized (KEY_LOCK)
		{
    		if (this.getKeyIndex().containsKey(key))
    		{
    			id = this.getKeyIndex().get(key);
    		}
		}
		
		return id;
	}
	
	public boolean hasID (U key)
	{
	    boolean hasIt;
	    
	    synchronized (KEY_LOCK)
	    {
	        hasIt = this.getKeyIndex().containsKey(key);
	    }
	    
	    return hasIt;
	}

	void add( T element )
    {
        synchronized (KEY_LOCK)
        {
            this.getKeyIndex().put(element.getKey(), element.getId());

            if (this.details != null && !this.details.containsKey(element.getId()))
            {
                this.getDetails().put(element.getId(), element);
            }
        }
    }
	
	void add( U key, Integer id )
	{
	    synchronized (KEY_LOCK)
        {
	        this.getKeyIndex().put(key, id);
	    }
	}
}
