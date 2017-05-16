package wres.engine.statistics.metric;

import java.util.Objects;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutput;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;

/**
 * A generic implementation of an error metric that applies a {@link DoubleErrorFunction} to each pair within a
 * {@link SingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class DoubleErrorMetric<S extends SingleValuedPairs, T extends MetricOutput<?, ?>> extends Metric<S, T>
{
    /**
     * The error function.
     */

    DoubleErrorFunction f;

    /**
     * Constructor.
     * 
     * @param f the error function
     */

    public DoubleErrorMetric(final DoubleErrorFunction f)
    {
        Objects.requireNonNull(f, "Specify a non-null function from which to construct the metric.");
        this.f = f;
    }

    @Override
    public T apply(final S s)
    {
        Objects.requireNonNull(s, "Specify non-null input for the '" + toString() + "'.");
        //Compute the atomic errors in a parallel stream
        return MetricOutputFactory.getVectorExtendsMetricOutput(s.getData().stream().mapToDouble(f).toArray(),
                                                                s.size(),
                                                                null);
    }

}
