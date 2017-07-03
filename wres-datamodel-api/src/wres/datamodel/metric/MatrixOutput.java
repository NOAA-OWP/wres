package wres.datamodel.metric;

import wres.datamodel.MatrixOfDoubles;

/**
 * A matrix of outputs associated with a metric. The number of elements and the order in which they are stored, is
 * prescribed by the metric from which the outputs originate.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MatrixOutput extends MetricOutput<MatrixOfDoubles>
{
}
