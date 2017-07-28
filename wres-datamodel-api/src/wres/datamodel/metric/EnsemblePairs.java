package wres.datamodel.metric;

import java.util.List;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;

/**
 * Store of {@link PairOfDoubleAndVectorOfDoubles} where the left side is a single value and the right side is an
 * ensemble of values.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface EnsemblePairs extends MetricInput<List<PairOfDoubleAndVectorOfDoubles>>
{

    /**
     * Returns the baseline data as a {@link MetricInput}.
     * 
     * @return the baseline
     */

    EnsemblePairs getBaselineData();

}
