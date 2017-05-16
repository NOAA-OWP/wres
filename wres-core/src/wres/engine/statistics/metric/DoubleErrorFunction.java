package wres.engine.statistics.metric;

import java.util.function.ToDoubleFunction;

import wres.datamodel.PairOfDoubles;

/**
 * Interface for a class of function that applies to a {@link PairOfDoubles} and returns a <code>double</code>.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public interface DoubleErrorFunction extends ToDoubleFunction<PairOfDoubles>
{
}
