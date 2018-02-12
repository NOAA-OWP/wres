package wres.datamodel.metadata;

import java.util.Objects;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Dimension;

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
     * Returns <code>true</code> if the input is equal to the current {@link Metadata} without considering the 
     * {@link TimeWindow}.
     * 
     * @param input the input metadata
     * @return true if the input is equal to the current metadata, without considering the time window
     */
    default boolean equalsWithoutTimeWindow( final Metadata input )
    {
        if ( Objects.isNull( input ) )
        {
            return false;
        }
        boolean returnMe = input.getDimension().equals( getDimension() ) && hasIdentifier() == input.hasIdentifier();
        if ( hasIdentifier() )
        {
            returnMe = returnMe && getIdentifier().equals( input.getIdentifier() );
        }
        return returnMe;
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
