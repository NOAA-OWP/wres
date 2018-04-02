package wres.datamodel.inputs.pairs;

import wres.datamodel.VectorOfBooleans;
import wres.datamodel.inputs.MetricInput;

/**
 * Store of verification pairs associated with the outcome (true or false) of a multi-category event. The categorical
 * outcomes may be ordered or unordered.
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MulticategoryPairs extends PairedInput<VectorOfBooleans>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
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
