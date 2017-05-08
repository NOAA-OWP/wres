/**
 * 
 */
package collections;

import wres.datamodel.Tuple;

/**
 * An immutable pairing of two values
 * 
 * @author Christopher Tubbs
 */
public class TwoTuple<T extends Comparable<T>, U extends Comparable<U>> implements Group<TwoTuple<T, U>>, Tuple<T,U>
{

	/**
	 * Creates the immutable pair of two values
	 */
	public TwoTuple(T itemOne, U itemTwo) {
		this.itemOne = itemOne;
		this.itemTwo = itemTwo;
	}
	
	/**
	 * Returns the first value
	 * @return The first value
	 */
	@Override
    public T getItemOne()
	{
		return itemOne;
	}
	
	/**
	 * Returns the second value
	 * @return The second value
	 */
	@Override
    public U getItemTwo() {
		return itemTwo;
	}

	@Override
	public int compareTo(TwoTuple<T, U> other) {
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
	
	private final T itemOne;
	private final U itemTwo;
	
	@Override
	public boolean isFull() {
		return itemTwo != null && itemOne != null;
	}

	@Override
	public boolean isEmpty() {
		return itemTwo == null && itemOne == null;
	}

	@Override
	public TwoTuple<T, U> copy() {
		return new TwoTuple<T, U>(itemOne, itemTwo);
	}

    @Override
    public float similarity(TwoTuple<T, U> other)
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

        return similarity / 2F;
    }
    
    @Override
    public String toString()
    {
        return "(" + String.valueOf(this.getItemOne()) + ", " + String.valueOf(this.getItemTwo()) + ")";
    }
}
