/**
 * 
 */
package wres.util;

import java.util.Enumeration;
import java.util.Hashtable;

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
		expirationTime = 30000;
		locked = new Hashtable<T, Long>();
		unlocked = new Hashtable<T, Long>();
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
					//System.err.println("Object from pool expired...");
				}
				else
				{
					if (validate(t))
					{
						unlocked.remove(t);
						locked.put(t,  now);
						//System.err.println("Object from pool reused...");
						return (t);
					}
					else
					{
						unlocked.remove(t);
						expire(t);
						//System.err.println("Invalid object removed from pool...");
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
		//System.err.println("Object from pool returned.");
	}

}
