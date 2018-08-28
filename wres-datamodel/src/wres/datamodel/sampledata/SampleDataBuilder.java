package wres.datamodel.sampledata;

import java.util.List;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.SampleMetadata;

/**
 * A builder for {@link SampleData}.
 * 
 * @author james.brown@hydrosolved.com
 */

public interface SampleDataBuilder<S>
{

    /**
     * Adds sample data, appending to any existing sample data, as necessary.
     * 
     * @param sampleData the sample data
     * @return the builder
     */

    SampleDataBuilder<S> addData( List<S> sampleData );

    /**
     * Sets the metadata associated with the sample.
     * 
     * @param sampleMetadata the metadata
     * @return the builder
     */

    SampleDataBuilder<S> setMetadata( SampleMetadata sampleMetadata );

    /**
     * Adds sample data for a baseline, which is used to calculate skill, appending to any existing baseline sample, as
     * necessary.
     * 
     * @param baselineSampleData the sample data for the baseline
     * @return the builder
     */

    SampleDataBuilder<S> addDataForBaseline( List<S> baselineSampleData );

    /**
     * Sets the metadata associated with the baseline sample.
     * 
     * @param baselineSampleMetadata the metadata for the baseline sample
     * @return the builder
     */

    SampleDataBuilder<S> setMetadataForBaseline( SampleMetadata baselineSampleMetadata );

    /**
     * Sets a climatological dataset for the input.
     * 
     * @param climatology the climatology
     * @return the builder
     */

    SampleDataBuilder<S> setClimatology( VectorOfDoubles climatology );

    /**
     * Builds the metric input.
     * 
     * @return the metric input
     */

    SampleData<S> build();

}
