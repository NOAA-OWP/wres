package wres.datamodel.inputs.pairs;

import java.util.List;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.metadata.Metadata;

/**
 * <p>A list of pairs to be iterated over by a metric. A {@link PairedInput} may contain a baseline dataset to be used in 
 * the same context (e.g. for skill scores). Optionally, a climatological dataset may be associated with the 
 * {@link PairedInput}. This may be used to derive quantiles from climatological probabilities, for example.</p>
 * 
 * <p>For convenience, a builder is included.</p>.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface PairedInput<S> extends MetricInput<List<S>>, Iterable<S>
{

    /**
     * Returns true if the metric input has a baseline for skill calculations, false otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    default boolean hasBaseline()
    {
        return !Objects.isNull( getDataForBaseline() );
    }

    /**
     * Returns true if the metric input has a climatological dataset associated with it, false otherwise.
     * 
     * @return true if a climatological dataset is defined, false otherwise
     */

    default boolean hasClimatology()
    {
        return !Objects.isNull( getClimatology() );
    }

    /**
     * Returns the baseline data as a {@link MetricInput} or null if no baseline is defined.
     * 
     * @return the baseline
     */

    PairedInput<S> getBaselineData();

    /**
     * Returns the raw input associated with a baseline/reference for skill calculations or null if no baseline is
     * defined.
     * 
     * @return the raw input associated with a baseline
     */

    List<S> getDataForBaseline();

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
