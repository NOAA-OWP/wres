package wres.metrics;

import java.util.function.Function;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.Statistic;

/**
 * <p>A metric, which is a function that consumes a pool of data and produces a statistic (one or more). The metric
 * calculation is implemented in {@link #apply(Pool)}. The metric may operate on paired or unpaired inputs, and inputs
 * that comprise one or more individual datasets.
 *
 * <p>In general, a metric is a "dumb" function that cannot perform any pre-processing of a pool, such as rescaling,
 * changing measurement units, filtering, or removing missing values. However, a minority of metrics may consume more
 * complex pools, such as pools of time-series which may, for example, contain missing values. In most cases, the
 * removal of missing values and any other pre-processing should be conducted upfront.
 *
 * @param <S> the type of pool consumed by the metric
 * @param <T> the type of statistic produced by the metric
 * @author James Brown
 */
public interface Metric<S extends Pool<?>, T extends Statistic<?>> extends Function<S, T>
{
    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param pool the pool
     * @return the output
     * @throws PoolException if the pool is unexpected
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    T apply( S pool );

    /**
     * Returns a unique name for the metric from {@link MetricConstants}.
     * 
     * @return a unique name
     */

    MetricConstants getMetricName();

    /**
     * Returns true if the metric generates statistics that are in the same measurement unit as the pool, false if the
     * unit is dimensionless, such as a probability.
     * 
     * @return true if the statistic has real units, false otherwise
     */

    boolean hasRealUnits();

    /**
     * Implementations should provide a string representation of the {@link Metric}.
     * 
     * @return a string representation
     */

    @Override
    String toString();

    /**
     * A convenient default that returns the string representation of {@link #getMetricName()}.
     * 
     * @return the metric name
     */

    default String getMetricNameString()
    {
        return this.getMetricName()
                   .toString();
    }
}
