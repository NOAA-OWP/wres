/**
 * 
 */
package collections;

/**
 * A mutable grouping of two values
 * 
 * @author ctubbs
 *
 */
public class Pair<T extends Comparable<T>, U extends Comparable<U>> implements Comparable<Pair<T, U>> {

	/**
	 * Creates a pair of null values
	 */
	public Pair() {}

	/**
	 * Creates a pair containing the two passed in values
	 * 
	 * @param item_one
	 * @param item_two
	 */
	public Pair(T item_one, U item_two)
	{
		this.item_one = item_one;
		this.item_two = item_two;
	}
	
	public T item_one = null;
	public U item_two = null;

	@Override
	/**
	 * Returns 0 if the values in each pair are equal
	 * Returns 1 if the first's first value is greater than the second's first value or the first values are equal and the first's second is greater than the second's
	 * Returns -1 if the first's first value is less than the second's or the first values are equal but the second's second value is greater than the firsts
	 */
	public int compareTo(Pair<T, U> other) {
		int comparison = -1;
		comparison = other.item_one.compareTo(this.item_one);
		
		if (comparison == 0) {
			comparison = other.item_two.compareTo(item_two);
		}
		
		return comparison;
	}
}
