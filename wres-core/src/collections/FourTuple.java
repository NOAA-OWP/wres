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
		int comparison = 0;
        
        if (this.getItemOne() == null && other.getItemOne() != null) {
            comparison = -1;
        } else if (this.getItemOne() != null && other.getItemOne() == null) {
            comparison = 1;
        } else if (this.getItemOne() != null && other.getItemOne() != null){
            comparison = this.getItemOne().compareTo(other.getItemOne());
        }
		
		if (comparison == 0) {      
            if (this.getItemTwo() == null && other.getItemTwo() != null) {
                comparison = -1;
            } else if (this.getItemTwo() != null && other.getItemTwo() == null) {
                comparison = 1;
            } else if (this.getItemTwo() != null && other.getItemTwo() != null){
                comparison = this.getItemTwo().compareTo(other.getItemTwo());
            }
		}
		
		if (comparison == 0) {      
            if (this.getItemThree() == null && other.getItemThree() != null) {
                comparison = -1;
            } else if (this.getItemThree() != null && other.getItemThree() == null) {
                comparison = 1;
            } else if (this.getItemThree() != null && other.getItemThree() != null){
                comparison = this.getItemOne().compareTo(other.getItemOne());
            }
		}
		
		if (comparison == 0) {      
            if (this.getItemFour() == null && other.getItemFour() != null) {
                comparison = -1;
            } else if (this.getItemFour() != null && other.getItemFour() == null) {
                comparison = 1;
            } else if (this.getItemFour() != null && other.getItemFour() != null){
                comparison = this.getItemFour().compareTo(other.getItemFour());
            }
		}
		
		return comparison;
	}
	
	/**
	 * @return The first item
	 */
	public T getItemOne() {
		return this.itemOne;
	}
	
	/**
	 * @return The second item
	 */
	public U getItemTwo() {
		return this.itemTwo;
	}
	
	/**
	 * @return The third item
	 */
	public V getItemThree() {
		return this.itemThree;
	}
	
	/**
	 * @return The fourth item
	 */
	public W getItemFour() {
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
        
        if ((this.getItemOne() == null && other.getItemOne() == null) ||
            (this.getItemOne() != null && this.getItemOne().equals(other.getItemOne()))) {
            similarity++;
        }
        
        if ((this.getItemTwo() == null && other.getItemTwo() == null) || 
            (this.getItemTwo() != null && this.getItemTwo().equals(other.getItemTwo()))) {
            similarity++;
        }
        
        if ((this.getItemThree() == null && other.getItemThree() == null) ||
            (this.getItemThree().equals(other.getItemThree()))) {
            similarity++;
        }
        
        if ((this.getItemFour() == null && other.getItemFour() == null) || 
            (this.getItemFour().equals(other.getItemFour()))) {
            similarity++;
        }

        return similarity / 4F;
    }
    
    @Override
    public String toString()
    {
        return "(" + String.valueOf(this.getItemOne()) + ", " + 
                     String.valueOf(this.getItemTwo()) + ", " + 
                     String.valueOf(this.getItemThree()) + ", " + 
                     String.valueOf(this.getItemFour()) + ")";
    }
}
