/**
 * 
 */
package collections;

/**
 * A mutable grouping of three values
 * 
 * @author Christopher Tubbs
 */
public class Triplet<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>> implements Group<Triplet<T, U, V>> {
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
		this.itemOne = item_one;
		this.itemTwo = item_two;
		this.itemThree = item_three;
	}
    
    /**
     * @return The first value
     */
    public T getItemOne()
    {
        return this.itemOne;
    }
    
    /**
     * @return The second value
     */
    public U getItemTwo()
    {
        return this.itemTwo;
    }
    
    /**
     * @return The third value
     */
    public V getItemThree()
    {
        return this.itemThree;
    }
    
    /**
     * Updates the value of the first item
     * @param itemOne The new value
     */
    public void setItemOne(T itemOne) {
        this.itemOne = getItemOne();
    }
    
    /**
     * Updates the value of the second item
     * @param itemTwo The new value
     */
    public void setItemTwo(U itemTwo) {
        this.itemTwo = itemTwo;
    }
    
    /**
     * Updates the value of the third item
     * @param itemThree The new value
     */
    public void setItemThree(V itemThree) {
        this.itemThree = itemThree;
    }

	@Override
	public int compareTo(Triplet<T, U, V> other) {
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
                comparison = this.getItemThree().compareTo(other.getItemThree());
            }
        }
        
        return comparison;
	}
	
	// The first item in the grouping
	private T itemOne = null;
	
	// The second item in the grouping
	private U itemTwo = null;
	
	// The third item in the grouping
	private V itemThree = null;
	
	@Override
    public boolean isFull()
	{
		return this.itemOne != null && this.itemTwo != null && this.itemThree != null;
	}

	@Override
	public boolean isEmpty() {
		return this.itemOne == null && this.itemTwo == null && this.itemThree == null;
	}

	@Override
	public Triplet<T, U, V> copy() {
		// TODO Auto-generated method stub
		return new Triplet<T, U, V>(itemOne, itemTwo, itemThree);
	}

    @Override
    public float similarity(Triplet<T, U, V> other)
    {
        byte similarity = 0;
        
        if ((this.itemOne == null && other.itemOne == null) ||
            (this.itemOne != null && this.itemOne.equals(other.itemOne))) {
            similarity++;
        }
        
        if ((this.itemTwo == null && other.itemTwo == null) ||
            (this.itemTwo != null && this.itemTwo.equals(other.itemTwo))) {
            similarity++;
        }
        
        if ((this.itemThree == null && other.itemTwo == null) ||
            (this.itemThree != null && this.itemThree.equals(other.itemThree))) {
            similarity++;
        }
        
        return similarity / 3.0F;
    }
}
