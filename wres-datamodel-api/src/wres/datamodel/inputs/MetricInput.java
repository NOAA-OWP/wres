package wres.datamodel.inputs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.Metadata;

/**
 * <p>An input to be iterated over by a metric. A metric input may comprise paired data or unpaired data.
 * Optionally, it may contain a baseline dataset to be used in the same context (e.g. for skill scores) and a 
 * climatological dataset, which is used to derive quantiles from climatological probabilities.</p>
 * 
 * <p><b>Implementation Requirements:</b></p>
 * 
 * <p>A dataset may contain values that correspond to a missing value identifier.</p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MetricInput<S> extends Iterable<S>
{

    /**
     * Returns true if the metric input has a baseline for skill calculations, false otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    default boolean hasBaseline()
    {
        return Objects.nonNull( this.getRawDataForBaseline() );
    }

    /**
     * Returns true if the metric input has a climatological dataset associated with it, false otherwise.
     * 
     * @return true if a climatological dataset is defined, false otherwise
     */

    default boolean hasClimatology()
    {
        return Objects.nonNull( this.getClimatology() );
    }    
    
    /**
     * Returns the raw input.
     * 
     * @return the raw input
     */

    List<S> getRawData();

    /**
     * Returns the metadata associated with the input.
     * 
     * @return the metadata associated with the input
     */

    Metadata getMetadata();

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    MetricInput<S> getBaselineData();

    /**
     * Returns the raw input associated with a baseline/reference for skill calculations or null if no baseline is
     * defined.
     * 
     * @return the raw input associated with a baseline
     */

    List<S> getRawDataForBaseline();

    /**
     * Returns the metadata associated with the baseline input or null if no baseline is defined.
     * 
     * @return the metadata associated with the baseline input
     */

    Metadata getMetadataForBaseline();

    /**
     * Returns a climatological dataset if {@link #hasClimatology()} returns true, otherwise null.
     * 
     * @return a climatological dataset or null
     */

    VectorOfDoubles getClimatology();

}
