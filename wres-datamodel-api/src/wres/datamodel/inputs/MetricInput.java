package wres.datamodel.inputs;

import wres.datamodel.metadata.Metadata;

/**
 * <p>An input to be iterated over by a metric. A metric input may comprise paired data or unpaired data.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>A dataset may comprise missing elements, including both <code>null</code> elements that are used to retain 
 * position and elements of which a subset of values correspond to a missing value identifier.</p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInput<S>
{

    /**
     * Returns the raw input.
     * 
     * @return the raw input
     */

    S getData();

    /**
     * Returns the metadata associated with the input.
     * 
     * @return the metadata associated with the input
     */

    Metadata getMetadata();

}
