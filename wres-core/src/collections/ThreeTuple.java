/**
 * 
 */
package collections;

/**
 * An immutable pairing of three values
 */
public class ThreeTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>> implements Group<ThreeTuple<T, U, V>>{

	/**
	 * Creates the immutable triplet of two values
	 */
	public ThreeTuple(T itemOne, U itemTwo, V itemThree) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
		this.itemThree = itemThree;
	}
	
	/**
	 * Returns the first value
	 * @return The first value
	 */
	public T getItemOne()
	{
		return itemOne;
	}
	
	/**
	 * Returns the second value
	 * @return The second value
	 */
	public U getItemTwo()
	{
		return itemTwo;
	}
	
	/**
	 * Returns the third value
	 * @return The third value
	 */
	public V getItemThree()
	{
		return itemThree;
	}

	@Override
	public int compareTo(ThreeTuple<T, U, V> other) {
		int comparison = -1;
		comparison = other.itemOne.compareTo(this.itemOne);
		
		if (comparison == 0)
		{
			comparison = other.itemTwo.compareTo(itemTwo);
		}
		
		if (comparison == 0)
		{
			comparison = other.itemThree.compareTo(itemThree);
		}
		
		return comparison;
	}
	
	private final T itemOne;
	private final U itemTwo;
	private final V itemThree;
	
	@Override
	public boolean isFull() {
		return itemOne != null && itemTwo != null && itemThree != null;
	}

	@Override
	public boolean isEmpty() {
		return itemOne == null && itemTwo == null && itemThree == null;
	}

	@Override
	public ThreeTuple<T, U, V> copy() {
		return new ThreeTuple<T, U, V>(itemOne, itemTwo, itemThree);
	}
}
