package wres.datamodel;

/**
 * Store of {@link PairOfDoubleAndVectorOfDoubles} where the left side is a single value and the right side is an
 * ensemble of values. Metrics should anticipate the possibility of an inconsistent number of ensemble members 
 * in each pair (e.g. due to missing values).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface EnsemblePairs extends PairedInput<PairOfDoubleAndVectorOfDoubles>
{

    /**
     * Returns the baseline data as a {@link MetricInput}.
     * 
     * @return the baseline
     */

    EnsemblePairs getBaselineData();

}
