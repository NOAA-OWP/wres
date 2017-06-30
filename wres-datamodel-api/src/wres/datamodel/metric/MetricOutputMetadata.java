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
    
    public MetricConstants getMetricID();
    
    /**
     * Returns an identifier associated with the component of the metric to which the output corresponds. 
     * 
     * @return the component identifier
     */
    
    public MetricConstants getMetricComponentID();
    
    /**
     * Allows for an identifier to be associated with a baseline used for computing forecast skill. The identifier
     * should be meaningful to a user, such as the modeling scenario to which the baseline refers. May be null.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    public String getIDForBaseline();    


}
