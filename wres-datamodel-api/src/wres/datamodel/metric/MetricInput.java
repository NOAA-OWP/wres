package wres.datamodel.metric;

/**
 * <p>
 * An input to be iterated over by a metric. A metric input may comprise paired data or unpaired data and may contain
 * one or more individual datasets. In addition, a metric input may contain a baseline dataset to be used in the same
 * context (e.g. for skill scores). Each dataset should contain one or more elements with no, explicit, missing values.
 * Missing values should be handled in advance. For ensemble forecasts, metrics should anticipate the possibility of an
 * inconsistent number of ensemble members (e.g. due to missing values).
 * </p>
 * <p>
 * By convention, the two datasets required for a skill calculation should be stored with the reference dataset or
 * baseline in the second index.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInput<S>
{

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
     * Returns true if the metric input has a baseline for skill calculations, false otherwise.
     * 
     * @return true if a baseline is defined, false otherwise
     */

    boolean hasBaseline();

}
