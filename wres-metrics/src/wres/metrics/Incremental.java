package wres.metrics;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.Statistic;

/**
 * An interface that allows for a statistic to be computed incrementally. Statistics may be separated into intermediate 
 * and final. Intermediate statistics may be combined with other intermediate statistics using 
 * {@link #combine(Pool, Statistic)}. Final statistics are generated from intermediate statistics using 
 * {@link #complete(Statistic)}.
 * 
 * @param <T> the intermediate statistic
 * @param <U> the final statistic
 * @author James Brown
 */
public interface Incremental<S extends Pool<?>, T extends Statistic<?>, U extends Statistic<?>>
{
    /**
     * Computes the next intermediate statistic from the specified statistic, and combines with a prior intermediate 
     * statistic.
     * 
     * @param pool the pool from which to compute the next statistic
     * @param statistic the intermediate statistic to combine with the next statistic
     * @return the combined statistic
     * @throws MetricCalculationException if the metric calculation fails
     * @throws PoolException if the prescribed input is null or unexpected
     */

    T combine( S pool, T statistic );

    /**
     * Returns a final statistic from an intermediate statistic.
     * 
     * @param statistic the statistic to finalize
     * @return the finalized statistic
     * @throws MetricCalculationException if the statistic could not be completed
     * @throws PoolException if the prescribed input is null or unexpected
     */

    U complete( T statistic );
}
