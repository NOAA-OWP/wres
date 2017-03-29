package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import wres.engine.statistics.metric.inputs.MetricInput;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.parameters.MetricParameter;
import wres.engine.statistics.metric.parameters.MetricParameterException;

/**
 * <p>
 * Abstract base class for a metric. The metric calculation is implemented in {@link #apply(MetricInput)}. The metric
 * may operate on paired or unpaired inputs, and inputs that comprise one or more individual datasets. The structure of
 * the input and output is prescribed by a particular subclass.
 * </p>
 * <p>
 * A metric cannot conduct any pre-processing of the input, such as rescaling, changing measurement units, conditioning,
 * or removing missing values (except for missing ensemble members whose treatment may be metric-specific and whose
 * inclusion may be important for recovering ensemble traces).
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class Metric<S extends MetricInput, T extends MetricOutput> implements Function<S, T>
{

    /**
     * Ordered map of parameters associated with the metric. For convenience, they are stored by their type identifier.
     */

    private final TreeMap<Integer, MetricParameter> parameters = new TreeMap<>();

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
     * Validates the metric parameters. Throws an exception if an incorrect number of parameters is provided, or if the
     * parameters are provided in an incorrect order or if any of the parameters are null.
     * 
     * @param par the metric parameters
     * @throws MetricParameterException if the parameters are unexpected, null, or in an incorrect order
     */

    public abstract void checkParameters(final MetricParameter... par);

    /**
     * Returns the unique name of the metric.
     * 
     * @return the unique metric name
     */

    public abstract String getName();

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
    @Override
    public boolean equals(final Object o)
    {
        return o != null && o instanceof Metric && ((Metric)o).getName().equals(getName());
    }

    /**
     * Returns a deep copy of the metric parameters.
     * 
     * @return a deep copy of the metric parameters
     */

    public final ArrayList<MetricParameter> getParameters()
    {
        final ArrayList<MetricParameter> returnMe = new ArrayList<>();
        for(final Map.Entry<Integer, MetricParameter> p: parameters.entrySet())
        {
            returnMe.add(p.getValue().deepCopy());
        }
        return returnMe;
    }

    /**
     * Returns a named parameter associated with the metric or null if the parameter does not exist.
     * 
     * @param id the parameter id
     * @return the named parameter or null
     */

    public final MetricParameter getParameter(final int id)
    {
        return parameters.get(id);
    }

    /**
     * Sets the metric parameters by first calling {@link #checkParameters(MetricParameter...)}. The parameters are deep
     * copied upon setting.
     * 
     * @param par the metric parameters
     * @throws MetricParameterException if the parameters are unexpected, null, or in an incorrect order
     */

    public final void setParameters(final MetricParameter... par)
    {
        checkParameters(par);
        for(final MetricParameter p: par)
        {
            parameters.put(p.getID(), p.deepCopy());
        }
    }

}
