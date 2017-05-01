/**
 * 
 */
package collections;

/**
 * @author Christopher Tubbs
 * Represents a grouping of one or more objects of different types
 */
public interface Group<T> extends Comparable<T> {
	
	/**
	 * Determines if there is a value for each position
	 * @return True if no values are null
	 */
	public boolean isFull();
	
	/**
	 * Determines if all values have no assigned value
	 * @return True if no values have been set
	 */
	public boolean isEmpty();
	
	/**
	 * Creates a new grouping with identical values
	 * @return A new grouping with identical values
	 */
	public T copy();
	
	/**
	 * Returns the percentage of how similar the two groups are
	 * @param other The other group
	 * @return The percentage of similarity. Having a similarity of 1.0 indicates
	 * Equivalence
	 */
	public float similarity(T other);
}
