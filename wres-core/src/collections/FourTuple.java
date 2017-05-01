/**
 * 
 */
package collections;

/**
 * @author Christopher Tubbs
 * Immutable grouping of 4 comparable objects
 */
public class FourTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>, W extends Comparable<W>> implements Group<FourTuple<T, U, V, W>>{

	/**
	 * Creates the tuple containing the four passed in objects
	 */
	public FourTuple(T itemOne, U itemTwo, V itemThree, W itemFour) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
		this.itemThree = itemThree;
		this.itemFour = itemFour;
	}

	@Override
	/**
	 * Compares in the order of: itemOne, itemTwo, itemThree, itemFour
	 */
	public int compareTo(FourTuple<T, U, V, W> other) {
		int comparison = this.itemOne().compareTo(other.itemOne());
		
		if (comparison == 0) {
			comparison = this.itemTwo().compareTo(other.itemTwo());
		}
		
		if (comparison == 0) {
			comparison = this.itemThree().compareTo(other.itemThree());
		}
		
		if (comparison == 0) {
			comparison = this.itemFour().compareTo(other.itemFour());
		}
		
		return comparison;
	}
	
	public T itemOne() {
		return this.itemOne;
	}
	
	public U itemTwo() {
		return this.itemTwo;
	}
	
	public V itemThree() {
		return this.itemThree;
	}
	
	public W itemFour() {
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

    @Override
    public float similarity(FourTuple<T, U, V, W> other) {
        byte similarity = 0;
        
        if ((this.itemOne() == null && other.itemOne() == null) ||
            (this.itemOne() != null && this.itemOne().equals(other.itemOne()))) {
            similarity++;
        }
        
        if ((this.itemTwo() == null && other.itemTwo() == null) || 
            (this.itemTwo() != null && this.itemTwo().equals(other.itemTwo()))) {
            similarity++;
        }
        
        if ((this.itemThree() == null && other.itemThree() == null) ||
            (this.itemThree().equals(other.itemThree()))) {
            similarity++;
        }
        
        if ((this.itemFour() == null && other.itemFour() == null) || 
            (this.itemFour().equals(other.itemFour()))) {
            similarity++;
        }

        return similarity / 4F;
    }
}
