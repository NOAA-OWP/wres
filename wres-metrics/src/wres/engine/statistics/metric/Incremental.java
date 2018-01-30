package wres.engine.statistics.metric;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.outputs.MetricOutput;

/**
 * An interface that allows for a metric output to be updated with additional outputs. Metric outputs may be separated
 * into intermediate outputs and final outputs. Intermediate outputs may be combined with other intermediate outputs 
 * using {@link #combine(MetricInput, MetricOutput)}. Final outputs are generated from intermediate outputs using 
 * {@link #complete(MetricOutput)}.
 * 
 * @param <T> the intermediate output type
 * @param <U> the final output type
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public interface Incremental<S extends MetricInput<?>, T extends MetricOutput<?>, U extends MetricOutput<?>>
{

    /**
     * Computes the next intermediate output from the specified input, and combines with a prior intermediate 
     * output.
     * 
     * @param input the input from which to compute the next output
     * @param output the intermediate output to combine with the next output
     * @return the combined output
     * @throws MetricCalculationException if the metric calculation fails
     * @throws MetricInputException if the prescribed input is null or unexpected
     */

    T combine( S input, T output );

    /**
     * Returns a final output from an intermediate output.
     * 
     * @param output the output to finalize
     * @return the finalized output
     * @throws MetricCalculationException if the metric could not be completed
     * @throws MetricInputException if the prescribed input is null or unexpected
     */

    U complete( T output );

}
