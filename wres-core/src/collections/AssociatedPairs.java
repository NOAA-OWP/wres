/**
 * 
 */
package collections;

import java.util.TreeMap;

/**
 * A map that will map a pair of two comparable values to another value.
 * <br/><br/>
 * Useful for accessing values based on two values instead of one.
 * 
 * @author Christopher Tubbs
 *
 */
public class AssociatedPairs<U extends Comparable<U>, V extends Comparable<V>, W> extends TreeMap<Pair<U, V>, W> {

	/**
	 * The serialization version id of this interation of the class
	 */
	private static final long serialVersionUID = 1538678551612228282L;

	/**
	 * Returns the mapped value based on the two values within the key
	 * 
	 * @param itemOne The value in the first position in the pair
	 * @param itemTwo The value in the second position in the pair
	 * @return The value mapped to item one and item two
	 */
	public W get(U itemOne, V itemTwo) {
		if (indexer == null) {
			indexer = new Pair<U, V>();
		}
		
		indexer.setItemOne(itemOne);
		indexer.setItemTwo(itemTwo);
		
		return this.get(indexer);
	}

	/**
	 * Places a value into the map based on the keys item_one and item_two
	 * 
	 * @param itemOne The first key to map the value to
	 * @param itemTwo The second key to map the value to
	 * @param value The value that will be stored and indexed
	 */
	public void put(U itemOne, V itemTwo, W value) {
		this.put(new Pair<U, V>(itemOne, itemTwo), value);
	}
	
	/**
	 * Background pair used to find values without having to instantiate new keys on get operations
	 */
	private Pair<U, V> indexer = null;
}
