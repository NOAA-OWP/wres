/**
 * 
 */
package collections;

/**
 * An immutable pairing of two values
 */
public class TwoTuple<T extends Comparable<T>, U extends Comparable<U>> implements Group<TwoTuple<T, U>>{

	/**
	 * Creates the immutable pair of two values
	 */
	public TwoTuple(T itemOne, U itemTwo) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
	}
	
	/**
	 * Returns the first value
	 * @return The first value
	 */
	public T itemOne()
	{
		return itemOne;
	}
	
	/**
	 * Returns the second value
	 * @return The second value
	 */
	public U itemTwo() {
		return itemTwo;
	}

	@Override
	public int compareTo(TwoTuple<T, U> other) {
		int comparison = -1;
		comparison = other.itemOne.compareTo(this.itemOne);
		
		if (comparison == 0)
		{
			comparison = other.itemTwo.compareTo(itemTwo);
		}
		
		return comparison;
	}
	
	private final T itemOne;
	private final U itemTwo;
	
	@Override
	public boolean isFull() {
		return itemTwo != null && itemOne != null;
	}

	@Override
	public boolean isEmpty() {
		return itemTwo == null && itemOne == null;
	}

	@Override
	public TwoTuple<T, U> copy() {
		return new TwoTuple<T, U>(itemOne, itemTwo);
	}
}
