/**
 * 
 */
package collections;

/**
 * A mutable grouping of three values
 */
public class Triplet<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>> implements Comparable<Triplet<T, U, V>> {

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
		this.item_one = item_one;
		this.item_two = item_two;
		this.item_three = item_three;
	}

	@Override
	public int compareTo(Triplet<T, U, V> other) {
		int comparison = -1;
		comparison = other.item_one.compareTo(this.item_one);
		
		if (comparison == 0)
		{
			comparison = other.item_two.compareTo(item_two);
		}
		
		if (comparison == 0)
		{
			comparison = other.item_three.compareTo(item_three);
		}
		return comparison;
	}
	
	// The first item in the grouping
	public T item_one = null;
	
	// The second item in the grouping
	public U item_two = null;
	
	// The third item in the grouping
	public V item_three = null;
}
