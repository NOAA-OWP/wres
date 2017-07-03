package wres.engine.statistics.metric;

import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DecomposableDoubleErrorScore<S extends SingleValuedPairs>
extends
    Metric<S,VectorOutput>
implements Score
{

    /**
     * Construct a {@link DecomposableDoubleErrorScore} with a {@link MetricOutputFactory}.
     * 
     * @param outputFactory the {@link MetricOutputFactory}.
     */

    protected DecomposableDoubleErrorScore(final MetricOutputFactory outputFactory)
    {
        super(outputFactory);
    }

}
