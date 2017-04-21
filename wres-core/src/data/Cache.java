/**
 * 
 */
package data;

import java.util.concurrent.ConcurrentHashMap;

import collections.RecentUseList;
import data.details.Detail;

/**
 * @author ctubbs
 * @param <V>
 * @param <U>
 *
 */
//abstract class Cache<T extends Detail<T>, V extends Comparable<V>, U> {
abstract class Cache<T extends Detail<T, U>, U extends Comparable<U>> {
	protected ConcurrentHashMap<Integer, T> details = new ConcurrentHashMap<Integer, T>();
	protected ConcurrentHashMap<U, Integer> keyIndex = new ConcurrentHashMap<U, Integer>();
	protected RecentUseList<Integer> recentlyUsedIDs = new RecentUseList<Integer>();
	protected abstract Integer getMaxDetails();
	
	public Integer getID(T detail) throws Exception
	{
		if (!keyIndex.contains(detail.getKey()))
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
		if (recentlyUsedIDs.size() >= getMaxDetails())
		{
			int lastID = recentlyUsedIDs.drop_last();
			T last = details.get(lastID);
			details.remove(lastID);
			keyIndex.remove(last.getKey());
		}
	}	
}
