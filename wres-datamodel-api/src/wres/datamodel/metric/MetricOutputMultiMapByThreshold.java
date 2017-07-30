package wres.datamodel.metric;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map of {@link MetricOutputMapByMetric} stored by {@link Threshold}. Implements the same read-only API as the
 * {@link Map}. However, for an immutable implementation, changes in the returned values are not backed by this map. A
 * builder is included to support construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMapByThreshold<T extends MetricOutput<?>>
{

    /**
     * Returns a {@link MetricOutputMapByMetric} corresponding to the specified threshold or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    default MetricOutputMapByMetric<T> get(final MapKey<Threshold> key)
    {
        return get(key.getKey());
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

    default void forEach(BiConsumer<MapKey<Threshold>, MetricOutputMapByMetric<T>> consumer)
    {
        for(Entry<MapKey<Threshold>, MetricOutputMapByMetric<T>> entry: entrySet())
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
     * Returns a {@link MetricOutputMap} corresponding to the specified {@link Threshold} or null
     * 
     * @param threshold the threshold
     * @return the mapping or null
     */

    MetricOutputMapByMetric<T> get(Threshold threshold);

    /**
     * Returns true if the mapping contains the specified key, false otherwise.
     * 
     * @param key the key
     * @return true if the map contains the key, false otherwise
     */

    boolean containsKey(MapKey<Threshold> key);

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    boolean containsValue(MetricOutputMapByMetric<T> value);

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    Collection<MetricOutputMapByMetric<T>> values();

    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */

    Set<MapKey<Threshold>> keySet();

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    Set<Entry<MapKey<Threshold>, MetricOutputMapByMetric<T>>> entrySet();

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

        MetricOutputMultiMapByThreshold<T> build();

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param key the result key
         * @param result the result
         * @return the builder
         */

        default Builder<T> put(MapKey<Threshold> key, MetricOutputMapByMetric<T> result)
        {
            put(key.getKey(), result);
            return this;
        }

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder<T> put(Threshold threshold, MetricOutputMapByMetric<T> result);

    }

}
