/**
 * 
 */
package collections;

/**
 * @author ctubbs
 *
 */
public class FourTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>, W extends Comparable<W>> implements Group<FourTuple<T, U, V, W>>{

	/**
	 * 
	 */
	public FourTuple(T itemOne, U itemTwo, V itemThree, W itemFour) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
		this.itemThree = itemThree;
		this.itemFour = itemFour;
	}

	@Override
	public int compareTo(FourTuple<T, U, V, W> other) {
		int comparison = -1;
		comparison = other.itemOne.compareTo(this.itemOne);
		
		if (comparison == 0) {
			comparison = other.itemTwo.compareTo(itemTwo);
		}
		
		if (comparison == 0) {
			comparison = other.itemThree.compareTo(itemThree);
		}
		
		if (comparison == 0) {
			comparison = other.itemFour.compareTo(itemFour);
		}
		
		return comparison;
	}
	
	public T getItemOne()
	{
		return this.itemOne;
	}
	
	public U getItemTwo()
	{
		return this.itemTwo;
	}
	
	public V getItemThree()
	{
		return this.itemThree;
	}
	
	public W getItemFour()
	{
		return this.itemFour;
	}

	private final T itemOne;
	private final U itemTwo;
	private final V itemThree;
	private final W itemFour;
	
	@Override
	public boolean isFull() {
		return itemOne != null && itemTwo != null && itemThree != null && itemFour != null;
	}

	@Override
	public boolean isEmpty() {
		return itemOne == null && itemTwo == null && itemThree == null && itemFour == null;
	}

	@Override
	public FourTuple<T, U, V, W> copy() {
		return new FourTuple<T, U, V, W>(itemOne, itemTwo, itemThree, itemFour);
	}
}
