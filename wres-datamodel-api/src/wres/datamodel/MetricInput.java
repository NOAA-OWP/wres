package wres.datamodel;

import java.util.Objects;

/**
 * An input to be iterated over by a metric. A metric input may comprise paired data or unpaired data. In addition, a
 * {@link MetricInput} may contain a baseline dataset to be used in the same context (e.g. for skill scores). Each
 * dataset should contain one or more elements with no, explicit, missing values. Missing values should be handled in
 * advance. Optionally, a climatological dataset may be associated with the {@link MetricInput}. This may be used to
 * derive quantiles from climatological probabilities, for example.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInput<S>
{

    /**
     * Returns true if the metric input has a baseline for skill calculations, false otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    default boolean hasBaseline()
    {
        return !Objects.isNull(getDataForBaseline());
    }

    /**
     * Returns true if the metric input has a climatological dataset associated with it, false otherwise.
     * 
     * @return true if a climatological dataset is defined, false otherwise
     */

    default boolean hasClimatology()
    {
        return !Objects.isNull(getClimatology());
    }

    /**
     * Returns the raw input.
     * 
     * @return the raw input
     */

    S getData();

    /**
     * Returns the metadata associated with the input.
     * 
     * @return the metadata associated with the input
     */

    Metadata getMetadata();

    /**
     * Returns the baseline data as a {@link MetricInput}.
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

    S getDataForBaseline();

    /**
     * Returns the metadata associated with the baseline input or null if no baseline is defined.
     * 
     * @return the metadata associated with the baseline input
     */

    Metadata getMetadataForBaseline();

    /**
     * Returns the number of elements in the input.
     * 
     * @return the size of the input
     */

    int size();
    
    /**
     * Returns a climatological dataset if {@link #hasClimatology()} returns true, otherwise null.
     * 
     * @return a climatological dataset or null
     */
    
    VectorOfDoubles getClimatology();

}
