package wres.datamodel;

/**
 * Store of verification pairs associated with a dichotomous input, i.e. a single event whose outcome is
 * recorded as occurring (true) or not occurring (false). 
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface DichotomousPairs extends MulticategoryPairs
{
    
    /**
     * Returns the baseline data as a {@link MetricInput}. 
     * 
     * @return the baseline
     */
    
    DichotomousPairs getBaselineData();
    
}
