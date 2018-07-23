package wres.engine.statistics.metric;

import wres.datamodel.inputs.MetricInput;
import wres.datamodel.outputs.MetricOutput;

/**
 * An abstract diagram.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class Diagram<S extends MetricInput<?>, T extends MetricOutput<?>> implements Metric<S, T>
{
    
    @Override
    public String toString()
    {
        return getID().toString();
    }      

}
