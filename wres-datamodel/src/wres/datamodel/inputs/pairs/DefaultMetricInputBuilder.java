package wres.datamodel.inputs.pairs;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputBuilder;
import wres.datamodel.metadata.Metadata;

/**
 * An abstract builder for building an immutable {@link MetricInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 */
public abstract class DefaultMetricInputBuilder<S> implements MetricInputBuilder<S>
{

    /**
     * Climatology.
     */

    VectorOfDoubles climatology;   
    
    /**
     * Metadata for input.
     */

    Metadata mainMeta;

    /**
     * Metadata for baseline.
     */

    Metadata baselineMeta;         

    /**
     * Sets the metadata associated with the input.
     * 
     * @param mainMeta the metadata
     * @return the builder
     */

    public DefaultMetricInputBuilder<S> setMetadata(Metadata mainMeta)
    {
        this.mainMeta = mainMeta;
        return this;
    }
    
    /**
     * Sets the metadata associated with the baseline input.
     * 
     * @param baselineMeta the metadata for the baseline
     * @return the builder
     */

    public DefaultMetricInputBuilder<S> setMetadataForBaseline(Metadata baselineMeta) 
    {
        this.baselineMeta = baselineMeta;
        return this;
    }
    
    /**
     * Sets a climatological dataset for the input.
     * 
     * @param climatology the climatology
     * @return the builder
     */

    public DefaultMetricInputBuilder<S> setClimatology(VectorOfDoubles climatology) {
        this.climatology = climatology;
        return this;
    }
    
}
