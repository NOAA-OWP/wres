package wres.datamodel;

/**
 * Store of verification pairs for two probabilistic variables that are defined for a common, discrete, event. 
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface DiscreteProbabilityPairs extends SingleValuedPairs
{
    
    /**
     * Returns the baseline data as a {@link MetricInput}. 
     * 
     * @return the baseline
     */
    
    DiscreteProbabilityPairs getBaselineData();  
    
}
