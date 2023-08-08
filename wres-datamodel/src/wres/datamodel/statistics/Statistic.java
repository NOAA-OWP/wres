package wres.datamodel.statistics;

import java.util.Objects;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;

/**
 * <p>A {@link Statistic} is used to describe {@link Pool} or to infer something about the population from which
 * it originates.
 *
 * @param <U> the type of statistic data
 * @author James Brown
 */
public interface Statistic<U>
{
    /**
     * Returns the statistic.
     *
     * @return the statistic
     */

    U getStatistic();

    /**
     * Returns the metadata associated with the pool that produced the statistic.
     *
     * @return the metadata associated with the pool that produced the statistic
     */

    PoolMetadata getPoolMetadata();

    /**
     * Returns the name of the metric that produced the statistic.
     *
     * @return the metric name
     */

    MetricConstants getMetricName();

    /**
     * Returns the sample quantile associated with the statistic, if any. If none, then the statistic is a nominal
     * value, rather than a quantile from its sampling distribution.
     *
     * @see #hasQuantile()
     * @return the sample quantile or null
     */

    Double getSampleQuantile();

    /**
     * Returns {@code true} if the statistic is associated with a quantile, {@code false} if it is a nominal value.
     *
     * @return whether the statistic is associated with a sample quantile
     */

    default boolean hasQuantile()
    {
        return Objects.nonNull( this.getSampleQuantile() );
    }
}
