/**
 * 
 */
package util;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import config.SystemConfig;

/**
 * @author ctubbs
 *
 */
public abstract class ObjectPool<T> {

	private long expirationTime;
	
	private Hashtable<T, Long> locked;
	private Hashtable<T, Long> unlocked;
	/**
	 * 
	 */
	public ObjectPool() {
		expirationTime = SystemConfig.pool_object_lifespan();
		locked = new Hashtable<T, Long>();
		unlocked = new Hashtable<T, Long>();
	}
	
	protected final int object_count()
	{
		return locked.size() + unlocked.size();
	}
	
	protected synchronized final void for_all(BiConsumer<T, Long> all_func)
	{
		unlocked.forEach(all_func);
	}
	
	protected abstract T create();
	
	public abstract boolean validate(T o);
	
	public abstract void expire(T o);
	
	public synchronized T check_out()
	{
		long now = System.currentTimeMillis();
		T t;
		
		if (unlocked.size() > 0)
		{
			Enumeration<T> e = unlocked.keys();
			while (e.hasMoreElements())
			{
				t = e.nextElement();
				if ((now - unlocked.get(t)) > expirationTime)
				{
					unlocked.remove(t);
					expire(t);
					t = null;
				}
				else
				{
					if (validate(t))
					{
						unlocked.remove(t);
						locked.put(t,  now);
						return (t);
					}
					else
					{
						unlocked.remove(t);
						expire(t);
						t = null;
					}
				}
			}
		}
		
		t = create();
		locked.put(t, now);
		
		return (t);
	}
	
	public synchronized void checkIn(T t)
	{
		locked.remove(t);
		unlocked.put(t,  System.currentTimeMillis());
	}

}
