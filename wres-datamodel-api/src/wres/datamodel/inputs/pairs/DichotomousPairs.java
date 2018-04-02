package wres.datamodel.inputs.pairs;

import wres.datamodel.inputs.MetricInput;

/**
 * Store of verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). 
 * 
 * @author james.brown@hydrosolved.com
 */

public interface DichotomousPairs extends MulticategoryPairs
{
    
    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */
    
    DichotomousPairs getBaselineData();
    
}
