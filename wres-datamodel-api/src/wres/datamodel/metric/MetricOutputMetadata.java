package wres.datamodel.metric;

/**
 * A class that stores the metadata associated with a metric output.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricOutputMetadata extends Metadata
{

    /**
     * Returns an identifier associated with the metric that produced the output.
     * 
     * @return the metric identifier
     */
    
    public int getMetricID();
    
    /**
     * Returns an identifier associated with the component of the metric to which the output corresponds. 
     * 
     * @return the component identifier
     */
    
    public int getMetricComponentID();
    
}
