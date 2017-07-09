package wres.datamodel.metric;

import java.util.Set;

/**
 * A sorted map of {@link MetricOutput} stored by a {@link MapBiKey} whose components include forecast lead time and
 * threshold.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapWithBiKey<S,T,U extends MetricOutput<?>>
extends MetricOutputMap<MapBiKey<S, T>, U>
{

    /**
     * Returns a submap whose entries correspond to the first key in the {@link MapBiKey}.
     * 
     * @param key the key by which to slice
     * @return a submap whose keys meet the condition
     * @throws IllegalArgumentException if no mappings match the input logic
     */
    
    MetricOutputMapWithBiKey<S,T,U> sliceByFirst(S key);
    
    /**
     * Returns a submap whose entries correspond to the second key in the {@link MapBiKey}.
     * 
     * @param key the key by which to slice
     * @return a submap whose keys meet the condition
     * @throws IllegalArgumentException if no mappings match the input logic
     */
    
    MetricOutputMapWithBiKey<S,T,U> sliceBySecond(T key);    
    
    /**
     * Returns a set view of the first key in the {@link MapBiKey}.
     * 
     * @return a view of first key
     */
    
    Set<S> keySetByFirstKey();
    
    /**
     * Returns a set view of the second key in the {@link MapBiKey}.
     * 
     * @return a view of the second key
     */
    
    Set<T> keySetBySecondKey();    
    

}
