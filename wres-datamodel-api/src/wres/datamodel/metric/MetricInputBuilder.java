/**
 * 
 */
package wres.datamodel.metric;

/**
 * An abstract builder for building an immutable metric input that comprises one or more individual inputs and
 * a single instance of {@link Metadata} that applies to all inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInputBuilder<S>
{

    /**
     * Adds an input to the collection.
     * 
     * @param element the input to add
     * @return the builder
     */

    public MetricInputBuilder<S> add(S element);

    /**
     * Sets the metadata associated with the input.
     * 
     * @param meta the metadata
     * @return the builder
     */

    public MetricInputBuilder<S> setMetadata(Metadata meta);

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    public MetricInput<S> build();    
    
}
