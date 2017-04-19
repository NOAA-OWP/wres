/**
 * 
 */
package collections;

/**
 * An immutable pairing of two values
 */
public class TwoTuple<T extends Comparable<T>, U extends Comparable<U>> implements Comparable<TwoTuple<T, U>>{

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
	public T itemOne()
	{
		return item_one;
	}
	
	/**
	 * Returns the second value
	 * @return The second value
	 */
	public U itemTwo()
	{
		return item_two;
	}

	@Override
	public int compareTo(TwoTuple<T, U> arg0) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	private final T item_one;
	private final U item_two;
}
