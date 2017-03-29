package wres.engine.statistics.metric.outputs;

import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.inputs.Dimension;
import wres.engine.statistics.metric.inputs.Sample;

/**
 * <p>
 * A base class for metric outputs and corresponding sample sizes.
 * </p>
 * <p>
 * TODO: implement a method in the {@link Metric} class that provides an identification of the outputs.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public interface MetricOutput extends Sample
{

    /**
     * Returns the dimension associated with the output or null if the output is dimensionless. See
     * {@link #isDimensionless()}
     * 
     * @return the dimension associated with the output or null
     */

    Dimension getDimension();

    /**
     * Returns true if the output is dimensionless, false if the output has a prescribed dimension/unit of measurement.
     * 
     * @return true if the output is dimensionless, false otherwise
     */

    boolean isDimensionless();

}
