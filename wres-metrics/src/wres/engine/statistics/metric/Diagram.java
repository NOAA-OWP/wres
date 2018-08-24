package wres.engine.statistics.metric;

import wres.datamodel.sampledata.MetricInput;
import wres.datamodel.statistics.MetricOutput;

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
