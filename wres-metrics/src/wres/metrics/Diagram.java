package wres.metrics;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.Statistic;

/**
 * An abstract diagram.
 *
 * @param <S> the type of pool consumed by the metric
 * @param <T> the type of statistic produced by the metric
 * @author James Brown
 */

public abstract class Diagram<S extends Pool<?>, T extends Statistic<?>> implements Metric<S, T>
{
    @Override
    public String toString()
    {
        return getMetricName().toString();
    }

}
