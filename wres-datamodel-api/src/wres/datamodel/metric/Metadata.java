package wres.datamodel.metric;

import java.util.Objects;

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
     * Returns an optional dataset identifier or null.
     * 
     * @return an identifier or null
     */

    DatasetIdentifier getIdentifier();
    
    /**
     * Returns true if {@link #getIdentifier()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    default boolean hasIdentifier() {
        return Objects.nonNull(getIdentifier());
    }

}
