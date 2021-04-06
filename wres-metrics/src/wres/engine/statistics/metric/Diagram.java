package wres.engine.statistics.metric;

import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.Statistic;

/**
 * An abstract diagram.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class Diagram<S extends Pool<?>, T extends Statistic<?>> implements Metric<S, T>
{
    
    @Override
    public String toString()
    {
        return getMetricName().toString();
    }      

}
