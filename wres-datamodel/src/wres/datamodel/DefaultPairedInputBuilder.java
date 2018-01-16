package wres.datamodel;

import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.inputs.pairs.builders.PairedInputBuilder;
import wres.datamodel.metadata.Metadata;

/**
 * An abstract builder for building an immutable {@link PairedInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
abstract class DefaultPairedInputBuilder<S> implements PairedInputBuilder<S>
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

    public DefaultPairedInputBuilder<S> setMetadata(Metadata mainMeta)
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

    public DefaultPairedInputBuilder<S> setMetadataForBaseline(Metadata baselineMeta) 
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

    public DefaultPairedInputBuilder<S> setClimatology(VectorOfDoubles climatology) {
        this.climatology = climatology;
        return this;
    }
    
}
