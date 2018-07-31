package wres.engine.statistics.metric;

import java.util.function.Function;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.outputs.MetricOutput;

/**
 * <p>
 * Abstract base class for an immutable metric. The metric calculation is implemented in {@link #apply(MetricInput)}.
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
public interface Metric<S extends MetricInput<?>, T extends MetricOutput<?>> extends Function<S, T>
{

    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param s the input
     * @return the output
     * @throws MetricInputException if the metric input is unexpected
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    T apply( S s );

    /**
     * Returns a unique identifier for the metric from {@link MetricConstants}.
     * 
     * @return a unique identifier
     */

    MetricConstants getID();

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
     * Returns the unique name of the metric, namely the string representation of {@link #getID()}.
     * 
     * @return the unique metric name
     */

    default String getName()
    {
        return getID().toString();
    }

    /**
     * Returns true when the input is a {@link Metric} and the metrics have equivalent names, i.e. {@link #getName()}
     * returns an equivalent string.
     * 
     * @param o the object to test for equality with the current object
     * @return true if the input is a metric and has an equivalent name to the current metric, false otherwise
     */
    default boolean nameEquals( final Object o )
    {
        return o instanceof Metric && ( (Metric<?, ?>) o ).getName().equals( getName() );
    }

}
