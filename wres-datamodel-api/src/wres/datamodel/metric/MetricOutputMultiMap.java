package wres.datamodel.metric;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map of {@link MetricOutputMap} stored by metric identifier. Implements the same read-only API as the {@link Map}.
 * However, for an immutable implementation, changes in the returned values are not backed by this map. A builder is
 * included to support construction on-the-fly from inputs of {@link MetricOutputMap}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMap<T extends MetricOutputMap<?, ?>>
{

    /**
     * Returns a {@link MetricOutputMap} corresponding to the input identifiers or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    default T get(final MapBiKey<MetricConstants, MetricConstants> key)
    {
        return get(key.getFirstKey(), key.getSecondKey());
    }

    /**
     * Convenience method that returns the {@link MetricOutputMap} associated with the specified metric identifier and
     * {@link MetricConstants#MAIN} for the metric component identifier.
     * 
     * @param metricID the metric identifier
     * @return the output for the specified key or null
     */

    default T get(final MetricConstants metricID)
    {
        return get(metricID, MetricConstants.MAIN);
    }

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
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    default void forEach(BiConsumer<MapBiKey<MetricConstants, MetricConstants>, T> consumer)
    {
        for(Entry<MapBiKey<MetricConstants, MetricConstants>, T> entry: entrySet())
        {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Returns a builder.
     * 
     * @return a builder
     */

    Builder<T> builder();

    /**
     * Returns a {@link MetricOutputMap} corresponding to the input identifiers or null
     * 
     * @param metricID the metric identifier
     * @param componentID the metric component identifier
     * @return the mapping or null
     */

    T get(MetricConstants metricID, MetricConstants componentID);

    /**
     * Returns true if the mapping contains the specified key, false otherwise.
     * 
     * @param key the key
     * @return true if the map contains the key, false otherwise
     */

    boolean containsKey(MapBiKey<MetricConstants, MetricConstants> key);

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    boolean containsValue(T value);

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    Collection<T> values();

    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */

    Set<MapBiKey<MetricConstants, MetricConstants>> keySet();

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    Set<Entry<MapBiKey<MetricConstants, MetricConstants>, T>> entrySet();

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

    interface Builder<T extends MetricOutputMap<?, ?>>
    {

        /**
         * Returns the built store.
         * 
         * @return the result store
         */

        MetricOutputMultiMap<T> build();

        /**
         * Adds a new result to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        Builder<T> put(MapBiKey<MetricConstants, MetricConstants> key, T result);

    }

}
