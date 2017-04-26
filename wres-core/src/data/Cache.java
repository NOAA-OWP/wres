/**
 * 
 */
package data;

import java.util.concurrent.ConcurrentSkipListMap;

import collections.RecentUseList;
import data.details.CachedDetail;

/**
 * @author ctubbs
 * @param <V>
 * @param <U>
 *
 */
abstract class Cache<T extends CachedDetail<T, U>, U extends Comparable<U>> {
	protected ConcurrentSkipListMap<Integer, T> details = new ConcurrentSkipListMap<Integer, T>();
	protected ConcurrentSkipListMap<U, Integer> keyIndex = new ConcurrentSkipListMap<U, Integer>();
	protected RecentUseList<Integer> recentlyUsedIDs = new RecentUseList<Integer>();
	protected abstract Integer getMaxDetails();
	
	public Integer getID(T detail) throws Exception
	{
		if (!keyIndex.containsKey(detail.getKey()))
		{
			addElement(detail);
		}
		
		return detail.getId();
	}
	
	public U getKey(int id) {
		U key = null;
		
		if (details.containsKey(id))
		{
			key = details.get(id).getKey();
		}

		return key;
	}
	
	public void addElement(T element) throws Exception {
		element.save();
		recentlyUsedIDs.add(element.getId());

		details.put(element.getId(), element);
		keyIndex.put(element.getKey(), element.getId());
		
		if (recentlyUsedIDs.size() >= getMaxDetails())
		{
			int lastID = recentlyUsedIDs.drop_last();
			T last = details.get(lastID);
			details.remove(lastID);
			keyIndex.remove(last.getKey());
		}
	}	
	
	public Integer getID(U key) throws Exception
	{
		Integer id = null;
		
		if (keyIndex.containsKey(key))
		{
			id = keyIndex.get(key);
		}
		
		return id;
	}
}
