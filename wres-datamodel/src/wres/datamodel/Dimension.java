package wres.datamodel;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;

/**
 * A class that stores the dimension associated with a {@link MetricInput} or a {@link MetricOutput}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Dimension extends Comparable<Dimension>
{

    /**
     * Returns true if the metric data has an explicit dimension, false if it is dimensionless.
     * 
     * @return true if the metric data has an explicit dimension, false otherwise
     */
    
    public boolean hasDimension();
    
    /**
     * Returns the named dimension.
     * 
     * @return the named dimension
     */

    public String getDimension();
}
