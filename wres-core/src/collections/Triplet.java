/**
 * 
 */
package collections;

/**
 * A mutable grouping of three values
 */
public class Triplet<T, U, V> {

	/**
	 * Creates a grouping of three null values
	 */
	public Triplet() {}
	
	/**
	 * Creates a grouping of three values
	 * @param item_one The first item in the grouping
	 * @param item_two The second item in the grouping
	 * @param item_three The third item in the grouping
	 */
	public Triplet(T item_one, U item_two, V item_three)
	{
		this.item_one = item_one;
		this.item_two = item_two;
		this.item_three = item_three;
	}
	
	// The first item in the grouping
	public T item_one = null;
	
	// The second item in the grouping
	public U item_two = null;
	
	// The third item in the grouping
	public V item_three = null;
}
