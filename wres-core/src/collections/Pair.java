/**
 * 
 */
package collections;

import wres.datamodel.Tuple;

/**
 * A mutable grouping of two values
 * 
 * @author Christopher Tubbs
 *
 */
public class Pair<T extends Comparable<T>, U extends Comparable<U>> implements Group<Pair<T, U>>, Tuple<T,U>
{

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
    
    /**
     * @return The first value
     */
    @Override
    public T getItemOne()
    {
        return itemOne;
    }

    /**
     * @return The second value
     */
    @Override
    public U getItemTwo() {
        return itemTwo;
    }
    
    /**
     * Updates the value of the first item
     * @param itemOne The new value
     */
    public void setItemOne(T itemOne) {
        this.itemOne = itemOne;
    }
    
    /**
     * Updates the value of the second item
     * @param itemTwo The new value
     */
    public void setItemTwo(U itemTwo) {
        this.itemTwo = itemTwo;
    }
	
	private T itemOne = null;
	private U itemTwo = null;

	@Override
	/**
	 * Returns 0 if the values in each pair are equal
	 * 
	 * Returns 1 if the first's first value is greater than the second's first value or the first values are equal and 
	 * the first's second is greater than the second's
	 * 
	 * Returns -1 if the first's first value is less than the second's or the first values are equal but the second's 
	 * second value is greater than the firsts
	 */
	public int compareTo(Pair<T, U> other) {
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

    @Override
    public float similarity(Pair<T, U> other)
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
        
        return similarity / 2F;
    }
    
    @Override
    public String toString()
    {
        return "(" + String.valueOf(this.getItemOne()) + ", " + String.valueOf(this.getItemTwo()) + ")";
    }
}
