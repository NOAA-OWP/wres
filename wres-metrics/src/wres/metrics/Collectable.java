package wres.metrics;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.Statistic;

/**
 * An interface that allows for a final metric output to be derived from an intermediate output, thereby
 * avoiding the need to recompute intermediate outputs that are common to several metrics. See also
 * {@link MetricCollection}, which collects together metrics with common dependencies and exploits this interface to
 * share intermediate statistics between them, computing each intermediate statistic only once. When forming a 
 * {@link Collectable}, it is best to form the largest collection possible for which some intermediate result is 
 * relatively expensive to compute because that result will be re-used across all instances that depend on it. For 
 * example, the root mean square error, the mean square error and the sum of square errors all depend on the sum of 
 * square errors, but the root mean square error also depends on the mean square error. In this case, it is better to 
 * form a larger collection of three metrics with the sum of square errors as the intermediate statistic, since this 
 * will maximize re-use.
 * 
 * @param <S> the input type
 * @param <T> the intermediate output type
 * @param <U> the final output type
 * @author James Brown
 */
public interface Collectable<S extends Pool<?>, T extends Statistic<?>, U extends Statistic<?>>
        extends Metric<S, U>
{
    /**
     * Computes a statistic from an intermediate statistic and a pool, which may be used to compute other statistics
     * that are not part of the intermediate statistic.
     * 
     * @param statistic the intermediate statistic from which the statistic will be computed
     * @param pool, the pool from which other statistics may be computed, if required
     * @return the metric result
     * @throws MetricCalculationException if the metric calculation fails
     * @throws PoolException if the prescribed input is unexpected
     */

    U aggregate( T statistic, S pool );

    /**
     * Returns the intermediate statistic for input to {@link #aggregate(Statistic, Pool)}. Ensure that the 
     * {@link Metric#getMetricName()} associated with the statistic corresponds to that of the implementing class and 
     * not the caller.
     * 
     * @param pool the pool from which the statistic is computed
     * @return the intermediate output that forms the input to metrics within this collection
     * @throws PoolException if the metric input is unexpected
     * @throws MetricCalculationException if the metric could not be computed
     */

    T getIntermediateStatistic( S pool );

    /**
     * Returns the {@link Metric#getMetricName()} of the metric whose output forms the input to this metric. Metrics with common
     * intermediate inputs are collected by the name of the metric that produces the intermediate input.
     * 
     * @return the {@link Metric#getMetricName()} of the metric whose output forms the input to this metric
     */

    MetricConstants getCollectionOf();
}
