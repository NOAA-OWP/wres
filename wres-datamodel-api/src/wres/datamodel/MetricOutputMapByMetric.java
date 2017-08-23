package wres.datamodel;

/**
 * A sorted map of {@link MetricOutput} stored by metric identifier.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapByMetric<T extends MetricOutput<?>>
extends MetricOutputMapWithBiKey<MetricConstants, MetricConstants, T>
{  
    
    /**
     * Convenience method that returns the {@link MetricOutput} associated with the specified metric 
     * identifier and {@link MetricConstants#MAIN} for the metric component identifier.
     * 
     * @param metricID the metric identifier
     * @return the output for the specified key or null
     */
    
    default T get(final MetricConstants metricID) {
        return get(metricID,MetricConstants.MAIN);
    }    
    
    /**
     * Convenience method that returns the {@link MetricOutput} associated with the specified pair of metric 
     * identifiers.
     * 
     * @param metricID the metric identifier
     * @param componentID the component identifier
     * @return the output for the specified key or null
     */
    
    T get(MetricConstants metricID, MetricConstants componentID);

}
