package wres.datamodel.outputs;

/**
 * <p>
 * An interface for metric outputs and associated metadata.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MetricOutput<U>
{

    /**
     * Returns the metadata associated with the output.
     * 
     * @return the metadata associated with the output
     */

    MetricOutputMetadata getMetadata();
    
    /**
     * Returns the data.
     * 
     * @return the data
     */

    U getData();

}
