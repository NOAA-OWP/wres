package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;
import wres.datamodel.time.TimeWindow;

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
     * Returns true if {@link #getTimeWindow()} returns non-null, false otherwise.
     * 
     * @return true if {@link #getTimeWindow()} returns non-null, false otherwise.
     */
    default boolean hasTimeWindow()
    {
        return Objects.nonNull(getTimeWindow());
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
     * Returns a {@link TimeWindow} associated with the metadata or null.
     * 
     * @return a lead time or null
     */

    TimeWindow getTimeWindow();

}
