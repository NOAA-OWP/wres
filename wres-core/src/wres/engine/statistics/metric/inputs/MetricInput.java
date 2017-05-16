package wres.engine.statistics.metric.inputs;

import java.util.List;

/**
 * Generic class for an input to be iterated over by a metric. A metric input may comprise paired data or unpaired data
 * and may contain one or more individual datasets. In addition, a metric input may contain a baseline dataset to be
 * used in the same context (e.g. for skill scores). Each dataset should contain one or more elements with no, explicit,
 * missing values. Missing values should be handled in advance. For ensemble forecasts, metrics should anticipate the
 * possibility of an inconsistent number of ensemble members (e.g. due to missing values).
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MetricInput<S>
{

    /**
     * Returns true if the input has a baseline associated with it, false otherwise.
     * 
     * @return true if the input has a baseline, false otherwise
     */

    boolean hasBaseline();

    /**
     * Returns the dimension associated with the input or null if the input is dimensionless.
     * 
     * @return the dimension associated with the input or null
     */

    Dimension getDimension();

    /**
     * Returns a list of {@link S}.
     * 
     * @return the data
     */

    List<S> getData();

    /**
     * Returns a list of {@link S} representing a baseline.
     * 
     * @return a list of baseline data
     */

    List<S> getBaselineData();

    /**
     * Returns the number of items in the dataset.
     * 
     * @return the size of the dataset
     */
    int size();

    /**
     * Returns the number of items in the baseline dataset.
     * 
     * @return the size of the baseline
     */

    int baseSize();

    /**
     * Returns the baseline or null if no baseline exists. See {@link #hasBaseline()}.
     * 
     * @return the baseline
     */

    MetricInput<S> getBaseline();

}
