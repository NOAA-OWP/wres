package wres.engine.statistics.metric;

import java.util.function.ToDoubleFunction;

import wres.datamodel.sampledata.pairs.SingleValuedPair;

/**
 * Interface for a class of function that applies to a {@link SingleValuedPair} and returns a <code>double</code>.
 * 
 * @author james.brown@hydrosolved.com
 */
public interface DoubleErrorFunction extends ToDoubleFunction<SingleValuedPair>
{
}
