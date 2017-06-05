package wres.engine.statistics.metric;

import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * A generic implementation of an error score that is decomposable.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class DecomposableDoubleErrorScore<S extends SingleValuedPairs, T extends VectorOutput>
extends
    Metric<S, T>
implements Score
{

}
