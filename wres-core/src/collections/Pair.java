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
public class Pair<T extends Comparable<T>, U extends Comparable<U>> implements Group<Pair<T, U>> {

	/**
	 * Creates a pair of null values
	 */
	public Pair() {}

	/**
	 * Creates a pair containing the two passed in values
	 * 
	 * @param itemOne
	 * @param itemTwo
	 */
	public Pair(T itemOne, U itemTwo) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
	}
	
	public T itemOne = null;
	public U itemTwo = null;

	@Override
	/**
	 * Returns 0 if the values in each pair are equal
	 * Returns 1 if the first's first value is greater than the second's first value or the first values are equal and the first's second is greater than the second's
	 * Returns -1 if the first's first value is less than the second's or the first values are equal but the second's second value is greater than the firsts
	 */
	public int compareTo(Pair<T, U> other) {
		int comparison = -1;
		comparison = other.itemOne.compareTo(this.itemOne);
		
		if (comparison == 0) {
			comparison = other.itemTwo.compareTo(itemTwo);
		}
		
		return comparison;
	}

	@Override
	public boolean isFull() {
		return itemOne != null && itemTwo != null;
	}

	@Override
	public boolean isEmpty() {
		return itemOne == null && itemTwo == null;
	}

	@Override
	public Pair<T, U> copy() {
		return new Pair<T, U>(itemOne, itemTwo);
	}
}
