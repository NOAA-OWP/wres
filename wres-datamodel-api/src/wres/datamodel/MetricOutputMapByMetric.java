package wres.datamodel;

/**
 * A sorted map of {@link MetricOutput} stored by metric identifier.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByMetric<T extends MetricOutput<?>>
extends MetricOutputMap<MapKey<MetricConstants>, T>
{    
    
    /**
     * Convenience method that returns the {@link MetricOutput} associated with the specified metric 
     * identifier.
     * 
     * @param metricID the metric identifier
     * @return the output for the specified key or null
     */
    
    T get(MetricConstants metricID);

}
