package wres.datamodel.inputs;

import wres.datamodel.metadata.Metadata;

/**
 * An input to be iterated over by a metric. A metric input may comprise paired data or unpaired data. Each
 * dataset should contain one or more elements with no, explicit, missing values. Missing values should be handled in
 * advance. 
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

    /**
     * Returns the number of elements in the input.
     * 
     * @return the size of the input
     */

    int size();

}
