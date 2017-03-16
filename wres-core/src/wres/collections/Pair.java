/**
 * 
 */
package wres.collections;

/**
 * @author ctubbs
 *
 */
public class Pair<T, U> {

	/**
	 * 
	 */
	public Pair() {}
	
	public Pair(T item_one, U item_two)
	{
		this.item_one = item_one;
		this.item_two = item_two;
	}
	
	public T item_one = null;
	public U item_two = null;

}
