package wres.engine.statistics.metric;

import java.util.function.Function;

import wres.datamodel.MetricConstants;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.statistics.Statistic;

/**
 * <p>
 * Abstract base class for an immutable metric. The metric calculation is implemented in {@link #apply(SampleData)}.
 * The metric may operate on paired or unpaired inputs, and inputs that comprise one or more individual datasets. The
 * structure of the input and output is prescribed by a particular subclass.
 * </p>
 * <p>
 * A metric cannot conduct any pre-processing of the input, such as rescaling, changing measurement units, conditioning,
 * or removing missing values. However, for metrics that rely on ordered input, such as a time index or spatial 
 * coordinate, missing values may be used to retain (relative) position. Such metrics are uniquely aware of missing 
 * values. In other cases, missing values should be removed upfront.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface Metric<S extends SampleData<?>, T extends Statistic<?>> extends Function<S, T>
{

    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param s the input
     * @return the output
     * @throws SampleDataException if the metric input is unexpected
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    T apply( S s );

    /**
     * Returns a unique identifier for the metric from {@link MetricConstants}.
     * 
     * @return a unique identifier
     */

    MetricConstants getMetricName();

    /**
     * Returns true if the metric generates outputs that are dimensioned in real units, false if the outputs are in
     * statistical or probabilistic units.
     * 
     * @return true if the outputs are dimensioned in real units, false otherwise
     */

    boolean hasRealUnits();

    /**
     * Implementations should provide a string representation of the {@link Metric}.
     * 
     * @return a string representation
     */

    @Override
    String toString();

    /**
     * Returns the unique name of the metric, namely the string representation of {@link #getMetricName()}.
     * 
     * @return the unique metric name
     */

    default String getName()
    {
        return this.getMetricName().toString();
    }

}
