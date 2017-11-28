package wres.datamodel.outputs;

import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

/**
 * A sorted map of {@link MetricOutput} stored by a {@link Pair}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMapWithBiKey<S,T,U extends MetricOutput<?>>
extends MetricOutputMap<Pair<S, T>, U>
{

    /**
     * Returns a submap whose entries correspond to the first key in the {@link Pair}.
     * 
     * @param key the key by which to slice
     * @return a submap whose keys meet the condition
     * @throws IllegalArgumentException if no mappings match the input logic
     */
    
    MetricOutputMapWithBiKey<S,T,U> sliceByFirst(S key);
    
    /**
     * Returns a submap whose entries correspond to the second key in the {@link Pair}.
     *
     * @param key the key by which to slice
     * @return a submap whose keys meet the condition
     * @throws IllegalArgumentException if no mappings match the input logic
     */
    
    MetricOutputMapWithBiKey<S,T,U> sliceBySecond(T key);    
    
    /**
     * Returns a set view of the first key in the {@link Pair}.
     * 
     * @return a view of first key
     */
    
    Set<S> keySetByFirstKey();
    
    /**
     * Returns a set view of the second key in the {@link Pair}.
     * 
     * @return a view of the second key
     */
    
    Set<T> keySetBySecondKey();    
    

}
