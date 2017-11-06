package wres.datamodel;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.metadata.Metadata;

/**
 * An abstract builder for building an immutable {@link MetricInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
abstract class MetricInputBuilder<S>
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
     * Sets the input.
     * 
     * @param mainInput the input
     * @return the builder
     */

    abstract MetricInputBuilder<S> setData(S mainInput);

    /**
     * Sets the metadata associated with the input.
     * 
     * @param mainMeta the metadata
     * @return the builder
     */

    MetricInputBuilder<S> setMetadata(Metadata mainMeta)
    {
        this.mainMeta = mainMeta;
        return this;
    }
    
    /**
     * Sets the input for a baseline, which is used to calculate skill.
     * 
     * @param baselineInput the input for the baseline
     * @return the builder
     */

    abstract MetricInputBuilder<S> setDataForBaseline(S baselineInput);    
    
    /**
     * Sets the metadata associated with the baseline input.
     * 
     * @param baselineMeta the metadata for the baseline
     * @return the builder
     */

    MetricInputBuilder<S> setMetadataForBaseline(Metadata baselineMeta) 
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

    MetricInputBuilder<S> setClimatology(VectorOfDoubles climatology) {
        this.climatology = climatology;
        return this;
    }

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    abstract MetricInput<S> build();    
    
}
