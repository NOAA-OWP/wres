package wres.datamodel.inputs.pairs;

import wres.datamodel.inputs.MetricInput;

/**
 * Store of {@link PairOfDoubleAndVectorOfDoubles} where the left side is a single value and the right side is an
 * ensemble of values. Metrics should anticipate the possibility of an inconsistent number of ensemble members 
 * in each pair (e.g. due to missing values).
 * 
 * @author james.brown@hydrosolved.com
 */
public interface EnsemblePairs extends MetricInput<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    EnsemblePairs getBaselineData();

}
