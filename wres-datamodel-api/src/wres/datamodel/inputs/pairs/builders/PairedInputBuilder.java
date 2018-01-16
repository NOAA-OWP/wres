package wres.datamodel.inputs.pairs.builders;

import java.util.List;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.metadata.Metadata;

/**
 * A builder for a {@link PairedInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */

public interface PairedInputBuilder<S>
{

    /**
     * Adds input data, appending to any existing data, as necessary.
     * 
     * @param mainInput the input
     * @return the builder
     */

    PairedInputBuilder<S> addData( List<S> mainInput );

    /**
     * Sets the metadata associated with the input.
     * 
     * @param mainMeta the metadata
     * @return the builder
     */

    PairedInputBuilder<S> setMetadata( Metadata mainMeta );

    /**
     * Adds input data for a baseline, which is used to calculate skill, appending to any existing baseline data, as
     * necessary.
     * 
     * @param baselineInput the input for the baseline
     * @return the builder
     */

    PairedInputBuilder<S> addDataForBaseline( List<S> baselineInput );

    /**
     * Sets the metadata associated with the baseline input.
     * 
     * @param baselineMeta the metadata for the baseline
     * @return the builder
     */

    PairedInputBuilder<S> setMetadataForBaseline( Metadata baselineMeta );

    /**
     * Sets a climatological dataset for the input.
     * 
     * @param climatology the climatology
     * @return the builder
     */

    PairedInputBuilder<S> setClimatology( VectorOfDoubles climatology );

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    PairedInput<S> build();

}
