package wres.engine.statistics.metric;

import java.util.Arrays;

import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * A generic implementation of an error score that applies a {@link DoubleErrorFunction} to each pair within a
 * {@link SingleValuedPairs} and returns the average error across those pairs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public abstract class DoubleErrorScore<S extends SingleValuedPairs, T extends MetricOutput<?>>
extends
    DoubleErrorMetric<S, T>
implements Score
{
    /**
     * Construct the error score.
     * 
     * @param f the error function
     */

    public DoubleErrorScore(final DoubleErrorFunction f)
    {
        super(f);
    }

    @Override
    public T apply(final S s)
    {
        return MetricOutputFactory.getScalarExtendsMetricOutput((Arrays.stream(((VectorOutput)super.apply(s)).getData()
                                                                                                             .getDoubles())
                                                                       .sum()
            / s.size()), s.size(), s.getDimension());
    }

}
