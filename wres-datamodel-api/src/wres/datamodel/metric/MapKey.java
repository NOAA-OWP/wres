package wres.datamodel.metric;

/**
 * A {@link MapKey} for storing a {@link MetricOutput} in a map. The key comprises a single component.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MapKey<S> extends Comparable<MapKey<S>>
{  
    
    /**
     * Returns the raw key.
     * 
     * @return the raw key
     */
    
    S getKey();
    
}
