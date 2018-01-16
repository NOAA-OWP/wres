package wres.datamodel.outputs;

import java.util.Map;

/**
 * Stores a map of outputs by key and value.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

public interface MapOutput<S,T> extends MetricOutput<Map<S,T>>
{    
}
