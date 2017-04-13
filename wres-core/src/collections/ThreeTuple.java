/**
 * 
 */
package collections;

/**
 * An immutable pairing of three values
 */
public class ThreeTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>> implements Comparable<ThreeTuple<T, U, V>>{

	/**
	 * Creates the immutable triplet of two values
	 */
	public ThreeTuple(T item_one, U item_two, V item_three) {
		this.item_one = item_one;
		this.item_two = item_two;
		this.item_three = item_three;
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
	
	/**
	 * Returns the third value
	 * @return The third value
	 */
	public V get_item_three()
	{
		return item_three;
	}

	@Override
	public int compareTo(ThreeTuple<T, U, V> o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private final T item_one;
	private final U item_two;
	private final V item_three;
}
