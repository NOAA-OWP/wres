package wres.datamodel;

import java.util.List;

/**
 * Store of verification pairs associated with the outcome (true or false) of a multi-category event. The categorical
 * outcomes may be ordered or unordered.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MulticategoryPairs extends MetricInput<List<VectorOfBooleans>>
{

    /**
     * Returns the baseline data as a {@link MetricInput}. 
     * 
     * @return the baseline
     */
    
    MulticategoryPairs getBaselineData();      
    
    /**
     * Returns the number of outcomes or categories in the dataset.
     * 
     * @return the number of categories
     */

    int getCategoryCount();

}
