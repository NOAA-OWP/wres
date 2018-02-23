package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.Dimension;
import wres.datamodel.MetricConstants;
import wres.datamodel.outputs.MetricOutput;

/**
 * A class that stores the metadata associated with a {@link MetricOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricOutputMetadata extends Metadata
{
  
    /**
     * Returns <code>true</code> if the {@link #getMetricComponentID()} has been set, otherwise <code>false</code>.
     * 
     * @return true if the metric component identifier is defined, otherwise false
     */

    default boolean hasMetricComponentID()
    {
        return Objects.nonNull( getMetricComponentID() );
    }
    
    /**
     * Returns an identifier associated with the metric that produced the output.
     * 
     * @return the metric identifier
     */

    MetricConstants getMetricID();

    /**
     * Returns an optional identifier associated with the component of the metric to which the output corresponds or 
     * a template for a score decomposition where the output contains multiple components. In that case, the template 
     * dictates the order in which components are returned.
     * 
     * @return the component identifier or null
     */

    MetricConstants getMetricComponentID();
    
    /**
     * Returns the dimension associated with the metric input, which may differ from the output. The output dimension
     * is returned by {@link #getDimension()}.
     * 
     * @return the dimension
     */

    Dimension getInputDimension();
    
    /**
     * Returns the sample size from which the {@link MetricOutput} was produced.
     * 
     * @return the sample size
     */

    int getSampleSize();    

    /**
     * <p>
     * Returns <code>true</code> if the input is minimally equal to this {@link MetricOutputMetadata}, otherwise
     * <code>false</code>. The two metadata objects are minimally equal if all of the following are equal, otherwise 
     * they are minimally unequal (and hence also unequal in terms of the stricter {@link Object#equals(Object)}.
     * </p>
     * <ol>
     * <li>{@link #getDimension()}</li>
     * <li>{@link #getInputDimension()}</li>
     * <li>{@link #getMetricID()}</li>
     * <li>{@link #getMetricComponentID()}</li>
     * </ol>
     * 
     * @param meta the metadata to check
     * @return true if the mandatory elements match, false otherwise
     */

    boolean minimumEquals(MetricOutputMetadata meta);

}
