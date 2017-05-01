/**
 * 
 */
package collections;

/**
 * An immutable pairing of three values
 * 
 * @author Christopher Tubbs
 */
public class ThreeTuple<T extends Comparable<T>, U extends Comparable<U>, V extends Comparable<V>> implements Group<ThreeTuple<T, U, V>>{

	/**
	 * Creates the immutable triplet of two values
	 */
	public ThreeTuple(T itemOne, U itemTwo, V itemThree) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
		this.itemThree = itemThree;
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

	@Override
	public int compareTo(ThreeTuple<T, U, V> other) {
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
	
	private final T itemOne;
	private final U itemTwo;
	private final V itemThree;
	
	@Override
	public boolean isFull() {
		return itemOne != null && itemTwo != null && itemThree != null;
	}

	@Override
	public boolean isEmpty() {
		return itemOne == null && itemTwo == null && itemThree == null;
	}

	@Override
	public ThreeTuple<T, U, V> copy() {
		return new ThreeTuple<T, U, V>(itemOne, itemTwo, itemThree);
	}

    @Override
    public float similarity(ThreeTuple<T, U, V> other)
    {
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

        return similarity / 3F;
    }
    
    @Override
    public String toString()
    {
        return "(" + String.valueOf(this.getItemOne()) + ", " + 
            String.valueOf(this.getItemTwo()) + ", " + 
            String.valueOf(this.getItemThree()) + ")"; 
    }
}
