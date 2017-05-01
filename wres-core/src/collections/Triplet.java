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

	@Override
	public int compareTo(Triplet<T, U, V> other) {
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
	
	// The first item in the grouping
	public T itemOne = null;
	
	// The second item in the grouping
	public U itemTwo = null;
	
	// The third item in the grouping
	public V itemThree = null;
	
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
