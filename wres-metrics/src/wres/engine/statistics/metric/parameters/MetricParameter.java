package wres.engine.statistics.metric.parameters;

// WRES dependencies
import wres.util.DeepCopy;

/**
 * @author james.brown@hydrosolved.com
 */
public interface MetricParameter extends DeepCopy<MetricParameter>
{

    /**
     * Returns a unique identifier associated with the metric parameter.
     * 
     * @return a unique identifier
     */

    int getID();

}
