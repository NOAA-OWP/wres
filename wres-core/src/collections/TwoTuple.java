/**
 * 
 */
package collections;

/**
 * An immutable pairing of two values
 * 
 * @author Christopher Tubbs
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

    @Override
    public float similarity(TwoTuple<T, U> other)
    {
        byte similarity = 0;
        
        if ((this.itemOne() == null && other.itemOne() == null) ||
            (this.itemOne() != null && this.itemOne().equals(other.itemOne()))) {
            similarity++;
        }
        
        if ((this.itemTwo() == null && other.itemTwo() == null) || 
            (this.itemTwo() != null && this.itemTwo().equals(other.itemTwo()))) {
            similarity++;
        }

        return similarity / 2F;
    }
}
