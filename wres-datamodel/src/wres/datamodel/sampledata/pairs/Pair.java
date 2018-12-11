package wres.datamodel.sampledata.pairs;

/**
 * A two-tuple of left and right values.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface Pair<L,R>
{

    /**
     * Returns the left value.
     * 
     * @return the left value
     */
    
    L getLeft();
    
    /**
     * Returns the right value.
     * 
     * @return the right value
     */
    
    R getRight();
    
}
