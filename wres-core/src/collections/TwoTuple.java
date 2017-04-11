/**
 * 
 */
package collections;

/**
 * An immutable pairing of two values
 */
public final class TwoTuple<T, U> {

	/**
	 * Creates the immutable pair of two values
	 */
	public TwoTuple(T item_one, U item_two) {
		this.item_one = item_one;
		this.item_two = item_two;
	}
	
	/**
	 * Returns the first value
	 * @return The first value
	 */
	public T get_item_one()
	{
		return item_one;
	}
	
	/**
	 * Returns the second value
	 * @return The second value
	 */
	public U get_item_two()
	{
		return item_two;
	}
	
	private T item_one;
	private U item_two;
}
