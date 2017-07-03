package wres.datamodel.metric;

/**
 * An abstract builder for building an immutable {@link MetricInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInputBuilder<S>
{

    /**
     * Sets the input.
     * 
     * @param mainInput the input
     * @return the builder
     */

    public MetricInputBuilder<S> setData(S mainInput);

    /**
     * Sets the metadata associated with the input.
     * 
     * @param mainMeta the metadata
     * @return the builder
     */

    public MetricInputBuilder<S> setMetadata(Metadata mainMeta);
    
    /**
     * Sets the input for a baseline, which is used to calculate skill.
     * 
     * @param baselineInput the input for the baseline
     * @return the builder
     */

    public MetricInputBuilder<S> setDataForBaseline(S baselineInput);    
    
    /**
     * Sets the metadata associated with the baseline input.
     * 
     * @param baselineMeta the metadata for the baseline
     * @return the builder
     */

    public MetricInputBuilder<S> setMetadataForBaseline(Metadata baselineMeta);        

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    public MetricInput<S> build();    
    
}
