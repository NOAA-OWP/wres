package wres.datamodel.statistics;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;

/**
 * <p>A {@link Statistic} is used to describe {@link Pool} or to infer something about the population from which
 * it originates.</p>
 * 
 * @param <U> the type of statistic data
 * @author james.brown@hydrosolved.com
 */
public interface Statistic<U>
{

    /**
     * Returns the statistic.
     * 
     * @return the statistic
     */

    U getData();
    
    /**
     * Returns the sample metadata associated with the statistic.
     * 
     * @return the sample metadata associated with the statistic
     */

    PoolMetadata getMetadata();
        
    /**
     * Returns the name of the metric that produced the statistic.
     * 
     * @return the metric name
     */
    
    MetricConstants getMetricName();
}
