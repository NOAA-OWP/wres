package wres.metrics;

import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Interface for a class of function that applies to a single-valued pair and returns a <code>double</code>.
 * 
 * @author James Brown
 */
public interface DoubleErrorFunction extends ToDoubleFunction<Pair<Double,Double>>
{
}
