package wres.datamodel;

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
     * Returns true if {@link #getIdentifier()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getIdentifier()} returns non-null, false otherwise.
     */
    default boolean hasIdentifier()
    {
        return Objects.nonNull(getIdentifier());
    } 
    
    /**
     * Returns true if {@link #getLeadTime()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getLeadTime()} returns non-null, false otherwise.
     */
    default boolean hasLeadTime()
    {
        return Objects.nonNull(getLeadTime());
    }      
    
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
     * Returns an optional forecast lead time to associate with the metadata or null. For analysis and simulation types,
     * this might be <code>0</code>.
     * 
     * @return a lead time or null
     */

    Integer getLeadTime();

}
