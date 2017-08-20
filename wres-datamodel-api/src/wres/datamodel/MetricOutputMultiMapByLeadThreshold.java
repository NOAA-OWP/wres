package wres.datamodel;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map of {@link MetricOutputMapByLeadThreshold} stored by metric identifier. Implements the same read-only API as the
 * {@link Map}. However, for an immutable implementation, changes in the returned values are not backed by this map. A
 * builder is included to support construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMapByLeadThreshold<S extends MetricOutput<?>> extends MetricOutputMultiMap<S>
{

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} corresponding to the input identifiers or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    default MetricOutputMapByLeadThreshold<S> get(final MapBiKey<MetricConstants, MetricConstants> key)
    {
        return get(key.getFirstKey(), key.getSecondKey());
    }

    /**
     * Convenience method that returns the {@link MetricOutputMapByLeadThreshold} associated with the specified metric
     * identifier and {@link MetricConstants#MAIN} for the metric component identifier.
     * 
     * @param metricID the metric identifier
     * @return the output for the specified key or null
     */

    default MetricOutputMapByLeadThreshold<S> get(final MetricConstants metricID)
    {
        return get(metricID, MetricConstants.MAIN);
    }

    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    default void forEach(BiConsumer<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>> consumer)
    {
        for(Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>> entry: entrySet())
        {
            consumer.accept(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Returns a {@link MetricOutputMap} corresponding to the input identifiers or null
     * 
     * @param metricID the metric identifier
     * @param componentID the metric component identifier
     * @return the mapping or null
     */

    MetricOutputMapByLeadThreshold<S> get(MetricConstants metricID, MetricConstants componentID);

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    Set<Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>>> entrySet();

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    boolean containsValue(MetricOutputMapByLeadThreshold<S> value);

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    Collection<MetricOutputMapByLeadThreshold<S>> values();

    /**
     * A builder.
     *
     * @param <S> the type of output to store
     */

    interface MetricOutputMultiMapByLeadThresholdBuilder<S extends MetricOutput<?>>
    extends MetricOutputMultiMap.Builder<S>
    {

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param key the result key
         * @param result the result
         * @return the builder
         */

        default MetricOutputMultiMapByLeadThresholdBuilder<S> put(MapBiKey<Integer, Threshold> key,
                                                                  MetricOutputMapByMetric<S> result)
        {
            put(key.getFirstKey(), key.getSecondKey(), result);
            return this;
        }

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param leadTime the forecast lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputMultiMapByLeadThresholdBuilder<S> put(int leadTime,
                                                          Threshold threshold,
                                                          MetricOutputMapByMetric<S> result);

        /**
         * Adds a new result to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        MetricOutputMultiMapByLeadThresholdBuilder<S> put(MapBiKey<MetricConstants, MetricConstants> key,
                                                          MetricOutputMapByLeadThreshold<S> result);

    }

}
