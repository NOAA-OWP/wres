package wres.datamodel.inputs.pairs;

import wres.datamodel.inputs.MetricInput;

/**
 * Store of verification pairs that comprise two single-valued, continuous numerical, variables. The single-valued
 * variables are not necessarily deterministic (i.e. they may be probabilistic), but they do comprise single values,
 * rather than multiple values.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface SingleValuedPairs extends PairedInput<PairOfDoubles>
{
    
    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined. 
     * 
     * @return the baseline
     */
    
    SingleValuedPairs getBaselineData();
    
}
