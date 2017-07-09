package wres.datamodel.metric;

/**
 * A class that stores the metadata associated with metric data (inputs and outputs).
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface Metadata
{

    /**
     * Returns the sample size associated with the metric data.
     * 
     * @return the sample size
     */

    int getSampleSize();

    /**
     * Returns the dimension associated with the metric.
     * 
     * @return the dimension
     */

    Dimension getDimension();

    /**
     * Optional geospatial identifier (e.g. location identifier) for the metric data.
     * 
     * @return the geospatial identifier associated with the metric data or null
     */

    String getGeospatialID();
    
    /**
     * Optional variable identifier for the metric data.
     * 
     * @return the variable identifier associated with the metric data or null
     */

    String getVariableID();    
    
    /**
     * Optional scenario identifier for the metric data, such as the modeling scenario for which evaluation is being 
     * conducted.
     * 
     * @return the scenario identifier associated with the metric data or null
     */

    String getScenarioID();

}
