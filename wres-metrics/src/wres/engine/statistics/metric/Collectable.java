package wres.engine.statistics.metric;

import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutput;

/**
 * An interface that allows for a final metric output to be derived from an intermediate output, thereby
 * avoiding the need to recompute intermediate outputs that are common to several metrics. See also
 * {@link MetricCollection}, which collects together metrics with common dependencies and exploits this interface to
 * share intermediate outputs between them. There is no value in implementing this interface unless there are two or
 * more metrics that have the same input and final output types. Use {@link #aggregate(MetricOutput)} to compute the 
 * aggregate output from the intermediate output.
 * 
 * @param <S> the input type
 * @param <T> the intermediate output type
 * @param <U> the final output type
 * @author james.brown@hydrosolved.com
 */
public interface Collectable<S extends MetricInput<?>, T extends MetricOutput<?>, U extends MetricOutput<?>>
        extends Metric<S, U>
{

    /**
     * Aggregates the final metric output from an intermediate output.
     * 
     * @param output the intermediate input from which the metric result will be computed
     * @return the metric result
     * @throws MetricCalculationException if the metric calculation fails
     * @throws MetricInputException if the prescribed input is unexpected
     */

    U aggregate( T output );

    /**
     * Returns the result whose method {@link Metric#apply(MetricInput)} provides the input to
     * {@link #aggregate(MetricOutput)}. Ensure that the {@link Metric#getID()} associated with the
     * {@link MetricOutputMetadata} of the output corresponds to that of the implementing class and not the caller.
     * 
     * @param input the metric input
     * @return the intermediate output that forms the input to metrics within this collection
     * @throws MetricInputException if the metric input is unexpected
     * @throws MetricCalculationException if the metric could not be computed
     */

    T getCollectionInput( S input );

    /**
     * Returns the {@link Metric#getID()} of the metric whose output forms the input to this metric. Metrics with common
     * intermediate inputs are collected by the name of the metric that produces the intermediate input.
     * 
     * @return the {@link Metric#getID()} of the metric whose output forms the input to this metric
     */

    MetricConstants getCollectionOf();

}
