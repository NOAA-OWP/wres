package wres.datamodel;

import java.util.Map;
import java.util.Set;

/**
 * A map of {@link MetricOutputMap} stored by metric identifier. Implements the same read-only API as the {@link Map}.
 * However, for an immutable implementation, changes in the returned values are not backed by this map. A builder is
 * included to support construction on-the-fly from inputs of {@link MetricOutputMap}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMap<T extends MetricOutput<?>>
{

    /**
     * Returns true if the map is empty, false otherwise.
     * 
     * @return true if the map is empty, false otherwise
     */

    default boolean isEmpty()
    {
        return size() > 0;
    }

    /**
     * Returns a builder.
     * 
     * @return a builder
     */

    Builder<T> builder();

    /**
     * Returns true if the mapping contains the specified key, false otherwise.
     * 
     * @param key the key
     * @return true if the map contains the key, false otherwise
     */

    boolean containsKey(MapKey<MetricConstants> key);

    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */

    Set<MapKey<MetricConstants>> keySet();

    /**
     * Returns the number of element in the map.
     * 
     * @return the size of the mapping
     */

    int size();

    /**
     * A builder.
     *
     * @param <T> the type of output to store
     */

    interface Builder<T extends MetricOutput<?>>
    {

        /**
         * Returns the built store.
         * 
         * @return the result store
         */

        MetricOutputMultiMap<T> build();

    }

}
