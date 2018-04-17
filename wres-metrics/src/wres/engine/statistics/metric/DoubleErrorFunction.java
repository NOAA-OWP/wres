package wres.engine.statistics.metric;

import java.util.function.ToDoubleFunction;

import wres.datamodel.inputs.pairs.PairOfDoubles;

/**
 * Interface for a class of function that applies to a {@link PairOfDoubles} and returns a <code>double</code>.
 * 
 * @author james.brown@hydrosolved.com
 */
public interface DoubleErrorFunction extends ToDoubleFunction<PairOfDoubles>
{
}
