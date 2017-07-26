package grouping;

import java.util.TreeMap;
import grouping.AssociatedPairs.Key;

/**
 * A map that will map a pair of two comparable values to another value.
 * <br><br>
 * Useful for accessing values based on two values instead of one.
 * 
 * @author Christopher Tubbs
 *
 */
@Deprecated
public class AssociatedPairs<U extends Comparable<U>, V extends Comparable<V>, W> extends TreeMap<Key, W> {

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
			indexer = new Key();
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
		this.put(new Key(itemOne, itemTwo), value);
	}
	
	/**
	 * Background pair used to find values without having to instantiate new keys on get operations
	 */
	private Key indexer = null;
	
	public class Key implements Comparable<Key>
	{
	    public Key() {}
	    
        public Key(U itemOne, V itemTwo)
        {
            this.itemOne = itemOne;
            this.itemTwo = itemTwo;
        }

        @Override
        public int compareTo(AssociatedPairs<U, V, W>.Key other)
        {
            int equality = this.getItemOne().compareTo(other.getItemOne());
            if (equality == 0)
            {
                equality = this.getItemTwo().compareTo(other.getItemTwo());
            }
            return equality;
        }
        
        public U getItemOne()
        {
            return this.itemOne;
        }
        
        public V getItemTwo()
        {
            return this.itemTwo;
        }
        
        public void setItemOne(U itemOne)
        {
            this.itemOne = itemOne;
        }
        
        public void setItemTwo(V itemTwo)
        {
            this.itemTwo = itemTwo;
        }
        
	    private U itemOne;
	    private V itemTwo;
	}
}
