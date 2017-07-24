package wres.datamodel.metric;

import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map of {@link MetricOutputMapByLeadThreshold} stored by metric identifier. A builder is included to support
 * construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMap<T extends MetricOutput<?>>
{

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

    MetricOutputMapByLeadThreshold<T> get(MetricConstants metricID, MetricConstants componentID);

    /**
     * Returns a view of the keys in the map for iteration.
     * 
     * @return a view of the keys
     */

    Set<MapBiKey<MetricConstants, MetricConstants>> keySet();

    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    void forEach(BiConsumer<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<T>> consumer);

    /**
     * Returns the number of element in the map.
     * 
     * @return the size of the mapping
     */

    int size();

    /**
     * Returns a {@link MetricOutputMap} corresponding to the input identifiers or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    default MetricOutputMapByLeadThreshold<T> get(final MapBiKey<MetricConstants, MetricConstants> key)
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

    default MetricOutputMapByLeadThreshold<T> get(final MetricConstants metricID)
    {
        return get(metricID, MetricConstants.MAIN);
    }

    /**
     * A builder.
     *
     * @param <T> the type of output to store
     */

    public interface Builder<T extends MetricOutput<?>>
    {

        /**
         * Returns the built store.
         * 
         * @return the result store
         */

        MetricOutputMultiMap<T> build();

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param leadTime the forecast lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder<T> add(int leadTime, Threshold threshold, MetricOutputMapByMetric<T> result);

    }

}
