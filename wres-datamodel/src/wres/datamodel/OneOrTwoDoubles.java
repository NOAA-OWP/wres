package wres.datamodel;

/**
 * Store of one or two double values with a default implementation.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface OneOrTwoDoubles extends Comparable<OneOrTwoDoubles>
{

    /**
     * Returns <code>true</code> if the store contains two values, <code>false</code> if it contains only one value.
     * 
     * @return true if the store contains two values, otherwise false
     */
    
    boolean hasTwo();
    
    /**
     * Returns the first value in the store. This is always defined.
     * 
     * @return the first value
     */
    
    Double first();
    
    /**
     * Returns the second value in the store or null. This is always null when {@link #hasTwo()} is <code>false</code>,
     * otherwise always non-null.
     * 
     * @return the second value or null
     */
    
    Double second();

}
