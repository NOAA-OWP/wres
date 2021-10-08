package wres.metrics;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.Statistic;

/**
 * An abstract diagram.
 * 
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
