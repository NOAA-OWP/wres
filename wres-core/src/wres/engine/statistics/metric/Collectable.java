package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.MetricInput;
import wres.engine.statistics.metric.inputs.MetricInputException;
import wres.engine.statistics.metric.outputs.MetricOutput;

/**
 * An interface that allows for a {@link Metric} to be computed from an intermediate {@link MetricOutput}, thereby
 * avoiding the need to recompute intermediate outputs that are common to several metrics. See also
 * {@link MetricCollection}, which collects together metrics with common dependencies and exploits this interface to
 * share intermediate outputs between them. There is no value in implementing this interface unless there are two or
 * more metrics that share a common ancestry.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface Collectable<S extends MetricInput, T extends MetricOutput, U extends MetricOutput>
{

    /**
     * Computes a metric with an intermediate result, contained in a {@link MetricOutput}.
     * 
     * @param output the intermediate input from which the metric result will be computed
     * @return the metric result
     * @throws MetricCalculationException if the metric calculation fails
     * @throws MetricInputException if the prescribed input is unexpected
     */

    U apply(T output);

    /**
     * Returns the result whose method {@link Metric#apply(wres.engine.statistics.metric.inputs.MetricInput)} provides
     * the input to {@link #apply(MetricOutput)}
     * 
     * @param input the metric input
     * @return the intermediate output that forms the input to metrics within this collection
     */

    T getCollectionInput(S input);

    /**
     * Returns the {@link Metric#getName()} of the metric whose output forms the input to this metric. Metrics with
     * common intermediate inputs are collected by the name of the metric that produces the intermediate input.
     * 
     * @return the {@link Metric#getName()} of the metric whose output forms the input to this metric
     */

    String getCollectionOf();
}
