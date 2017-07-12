package wres.datamodel.metric;

import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A sorted map of {@link MetricOutput} stored by a {@link Comparable} key.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMap<S extends Comparable<?>, T extends MetricOutput<?>>
{     
    
    /**
     * Returns the {@link MetricOutput} associated with the specified {@link Comparable} key or null.
     * 
     * @param key the key
     * @return the output for the specified key or null
     */
    
    T get(S key);
    
    /**
     * Returns the key at the specified index in the sorted map.
     * 
     * @param index the index 
     * @return the key
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    
    S getKey(int index);

    /**
     * Returns the value at the specified index in the sorted map.
     * 
     * @param index the index 
     * @return the value
     * @throws IndexOutOfBoundsException if the index is out of range
     */
    
    T getValue(int index);    
    
    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */
    
    Set<S> keySet();
    
    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */
    
    void forEach(BiConsumer<S,T> consumer);
    
    /**
     * Returns the number of element in the map.
     * 
     * @return the size of the mapping
     */
    
    int size();
   
}
