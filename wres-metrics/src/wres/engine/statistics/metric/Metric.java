package wres.engine.statistics.metric;

import java.util.function.Function;

import wres.datamodel.metric.MetricInput;
import wres.datamodel.metric.MetricOutput;

/**
 * <p>
 * Abstract base class for an immutable metric. The metric calculation is implemented in {@link #apply(MetricInput)}.
 * The metric may operate on paired or unpaired inputs, and inputs that comprise one or more individual datasets. The
 * structure of the input and output is prescribed by a particular subclass.
 * </p>
 * <p>
 * A metric cannot conduct any pre-processing of the input, such as rescaling, changing measurement units, conditioning,
 * or removing missing values (except for missing ensemble members whose treatment may be metric-specific and whose
 * inclusion may be important for recovering ensemble traces).
 * </p>
 * <p>
 * In order to build a metric, implement the inner class {@link MetricBuilder} and validate the parameters in a hidden
 * constructor.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class Metric<S extends MetricInput<?>, T extends MetricOutput<?>> implements Function<S, T>
{

    /**
     * Applies the function to the input and throws a {@link MetricCalculationException} if the calculation fails.
     * 
     * @param s the input
     * @return the output
     * @throws MetricCalculationException if the metric calculation fails
     */

    @Override
    public abstract T apply(S s);

    /**
     * Returns a unique identifier for the metric from {@link MetricConstants}.
     * 
     * @return a unique identifier
     */
    
    public abstract int getID();
    
    /**
     * Returns true if the metric generates outputs that are dimensioned in real units, false if the outputs are in
     * statistical or probabilistic units. 
     * 
     * @return true if the outputs are dimensioned in real units, false otherwise
     */
    
    public abstract boolean hasRealUnits();
    
    /**
     * Returns the unique name of the metric. See also {@link #getID()} and {@link MetricConstants#getMetricName(int)}.
     * 
     * @return the unique metric name
     */

    public String getName() {
        return MetricConstants.getMetricName(getID());
    }

    /**
     * Returns {@link #getName()}
     * 
     * @return a string representation of the metric.
     */

    @Override
    public String toString()
    {
        return getName();
    }

    /**
     * Returns true when the input is a {@link Metric} and the metrics have equivalent names, i.e. {@link #getName()}
     * returns an equivalent string.
     * 
     * @param o the object to test for equality with the current object
     * @return true if the input is a metric and has an equivalent name to the current metric, false otherwise
     */
    public boolean nameEquals(final Object o)
    {
        return o != null && o instanceof Metric && ((Metric<?, ?>)o).getName().equals(getName());
    }

    /**
     * An abstract builder to build a {@link Metric}. Implement this interface when building a {@link Metric}, and hide
     * the constructor. Add setters to set the parameters of the metric, as required, prior to building. For thread
     * safety, validate the parameters using the hidden constructor of the {@link Metric}, i.e. do not validate the
     * parameters in the {@link MetricBuilder} before construction.
     */

    public interface MetricBuilder<S extends MetricInput<?>, T extends MetricOutput<?>>
    {

        /**
         * Build the {@link Metric}.
         * 
         * @return a {@link Metric}
         */

        public Metric<S, T> build();

    }

}
