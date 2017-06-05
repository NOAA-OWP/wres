package wres.datamodel.metric;

/**
 * <p>
 * Generic class for an input to be iterated over by a metric. A metric input may comprise paired data or unpaired data
 * and may contain one or more individual datasets. In addition, a metric input may contain a baseline dataset to be
 * used in the same context (e.g. for skill scores). Each dataset should contain one or more elements with no, explicit,
 * missing values. Missing values should be handled in advance. For ensemble forecasts, metrics should anticipate the
 * possibility of an inconsistent number of ensemble members (e.g. due to missing values).
 * </p>
 * <p>
 * By convention, the two datasets required for a skill calculation should be stored with the reference dataset or
 * baseline in the second index.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MetricInput<S>
{

    /**
     * An abstract builder to build the collection od datasets.
     */

    public interface MetricInputBuilder<S>
    {

        /**
         * Adds an input to the collection.
         * 
         * @param element the input to add
         * @return the builder
         */

        public MetricInputBuilder<S> add(S element);

        /**
         * Sets the dimension associated with the input.
         * 
         * @param dim the dimension
         * @return the builder
         */

        public MetricInputBuilder<S> setDimension(Dimension dim);

        /**
         * Builds the metric input.
         * 
         * @return the metric input
         */

        public MetricInput<S> build();
    }

    /**
     * Convenience method that returns true if two datasets are available for a skill calculation, false otherwise.
     * 
     * @return true if two datasets are available
     */

    boolean hasTwo();

    /**
     * Returns the dimension associated with the input or null if the input is dimensionless.
     * 
     * @return the dimension associated with the input or null
     */

    Dimension getDimension();

    /**
     * Returns the data at a prescribed index.
     * 
     * @param index the index
     * @throws IndexOutOfBoundsException if the index is out of range
     * @return the data
     */

    S get(int index);

    /**
     * Returns the number of items in the dataset.
     * 
     * @return the size of the dataset
     */
    int size();

}
