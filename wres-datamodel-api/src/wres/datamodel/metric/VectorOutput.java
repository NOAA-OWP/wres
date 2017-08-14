package wres.datamodel.metric;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;

/**
 * A vector of outputs associated with a decomposable score. The number of outputs, as well as the individual outputs and the order
 * in which they are stored, is prescribed by the {@link MetricDecompositionGroup} associated with the output.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface VectorOutput extends MetricOutput<VectorOfDoubles>
{
    
    /**
     * Returns the {@link MetricDecompositionGroup}, which defines the number and order in which outputs are stored. 
     * This is accessible from {@link MetricDecompositionGroup#getMetricComponents()}.
     * 
     * @return the {@link MetricDecompositionGroup} associated with the metric output
     */
    
    MetricDecompositionGroup getOutputTemplate();
    
}
