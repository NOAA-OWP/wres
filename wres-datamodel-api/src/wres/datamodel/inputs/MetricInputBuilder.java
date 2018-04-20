package wres.datamodel.inputs;

import java.util.List;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.Metadata;

/**
 * A builder for a {@link MetricInput} with associated {@link Metadata}.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface MetricInputBuilder<S>
{

    /**
     * Adds input data, appending to any existing data, as necessary.
     * 
     * @param mainInput the input
     * @return the builder
     */

    MetricInputBuilder<S> addData( List<S> mainInput );

    /**
     * Sets the metadata associated with the input.
     * 
     * @param mainMeta the metadata
     * @return the builder
     */

    MetricInputBuilder<S> setMetadata( Metadata mainMeta );

    /**
     * Adds input data for a baseline, which is used to calculate skill, appending to any existing baseline data, as
     * necessary.
     * 
     * @param baselineInput the input for the baseline
     * @return the builder
     */

    MetricInputBuilder<S> addDataForBaseline( List<S> baselineInput );

    /**
     * Sets the metadata associated with the baseline input.
     * 
     * @param baselineMeta the metadata for the baseline
     * @return the builder
     */

    MetricInputBuilder<S> setMetadataForBaseline( Metadata baselineMeta );

    /**
     * Sets a climatological dataset for the input.
     * 
     * @param climatology the climatology
     * @return the builder
     */

    MetricInputBuilder<S> setClimatology( VectorOfDoubles climatology );

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    MetricInput<S> build();

}
