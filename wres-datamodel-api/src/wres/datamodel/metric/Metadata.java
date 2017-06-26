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

    public int getSampleSize();

    /**
     * Returns the dimension associated with the metric data.
     * 
     * @return the dimension
     */

    public Dimension getDimension();

    /**
     * Allows for an identifier to be associated with the metric data that is meaningful to a user, such as the modeling
     * scenario for which evaluation is being conducted. May be null.
     * 
     * @return the identifier associated with the metric data or null
     */

    public String getID();

    /**
     * Allows for an identifier to be associated with a baseline used for computing forecast skill. The identifier
     * should be meaningful to a user, such as the modeling scenario to which the baseline refers. May be null.
     * 
     * @return the identifier associated with the baseline metric data or null
     */

    public String getIDForBaseline();

}
