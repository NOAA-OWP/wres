package wres.engine.statistics.metric.inputs;

import wres.datamodel.metric.MetricInput;

/**
 * An immutable collection of metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricInputCollection<S extends MetricInput<?>>
{

    /**
     * Returns the element at the prescribed index.
     * 
     * @param index the index
     * @return the element
     * @throws IndexOutOfBoundsException
     */

    public S get(int index);

    /**
     * Returns the element at the prescribed index wrapped in a collection.
     * 
     * @param index the index
     * @return the wrapped element
     */

    public MetricInputCollection<S> getWrapped(int index);

    /**
     * Returns the size of the collection.
     * 
     * @return the size of the collection
     */

    public int size();
}