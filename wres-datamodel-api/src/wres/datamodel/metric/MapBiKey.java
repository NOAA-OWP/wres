package wres.datamodel.metric;

/**
 * A {@link MapKey} for storing a {@link MetricOutput} in a map. The key comprises a two components.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MapBiKey<S,T> extends Comparable<MapBiKey<S,T>>
{

    /**
     * Returns the first raw key.
     * 
     * @return the first raw key
     */
    
    S getFirstKey();
    
    /**
     * Returns the second raw key.
     * 
     * @return the second raw key
     */
    
    T getSecondKey();
    
}
