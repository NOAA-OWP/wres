package wres.engine.statistics.metric;

import java.util.function.ToDoubleFunction;

import wres.engine.statistics.metric.inputs.DoubleVector;

/**
 * Interface for a class of function that applies to a {@link DoubleVector} and returns a <code>double</code>.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface DoubleErrorFunction extends ToDoubleFunction<DoubleVector>
{
}
