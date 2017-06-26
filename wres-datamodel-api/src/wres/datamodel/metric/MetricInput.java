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
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface MetricInput<S>
{

    /**
     * Returns the metadata associated with the input.
     * 
     * @return the metadata associated with the input
     */

    Metadata getMetadata();

    /**
     * Returns the data at a prescribed index.
     * 
     * @param index the index
     * @throws IndexOutOfBoundsException if the index is out of range
     * @return the data
     */

    S getData(int index);

    /**
     * Convenience method that returns true if exactly two datasets are available for a skill calculation, false
     * otherwise.
     * 
     * @return true if two datasets are available
     */

    boolean hasBaselineForSkill();

    /**
     * Returns the number of items in the dataset.
     * 
     * @return the size of the dataset
     */
    int size();

}
