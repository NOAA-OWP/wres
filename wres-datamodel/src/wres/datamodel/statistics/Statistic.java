package wres.datamodel.statistics;

import java.util.Objects;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.statistics.generated.SummaryStatistic;

/**
 * <p>A {@link Statistic} is used to describe a {@link Pool} or to infer something about the population from which
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
     * Returns the summary statistic metadata associated with the statistic, if any. If none, then the statistic is a
     * raw statistic, and not a summary statistic over a collection of raw statistics.
     *
     * @see #isSummaryStatistic()
     * @return the summary statistic or null
     */

    SummaryStatistic getSummaryStatistic();

    /**
     * Returns {@code true} if the statistic is a summary statistic, {@code false} if it is a raw statistic.
     *
     * @return whether the statistic is a summary statistic over a collection of raw statistics
     */

    default boolean isSummaryStatistic()
    {
        return Objects.nonNull( this.getSummaryStatistic() );
    }
}
