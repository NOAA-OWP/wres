package wres.datamodel;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * A map of {@link MetricOutputMapByTimeAndThreshold} stored by metric identifier. Implements the same read-only API as the
 * {@link Map}. However, for an immutable implementation, changes in the returned values are not backed by this map. A
 * builder is included to support construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMapByTimeAndThreshold<S extends MetricOutput<?>> extends MetricOutputMultiMap<S>
{

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} corresponding to the input identifiers or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    default MetricOutputMapByTimeAndThreshold<S> get( final MapKey<MetricConstants> key )
    {
        return get( key.getKey() );
    }

    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    default void forEach( BiConsumer<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>> consumer )
    {
        for ( Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>> entry : entrySet() )
        {
            consumer.accept( entry.getKey(), entry.getValue() );
        }
    }

    /**
     * Returns a {@link MetricOutputMap} corresponding to the input identifiers or null
     * 
     * @param metricID the metric identifier
     * @return the mapping or null
     */

    MetricOutputMapByTimeAndThreshold<S> get( MetricConstants metricID );

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    Set<Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>>> entrySet();

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    boolean containsValue( MetricOutputMapByTimeAndThreshold<S> value );

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    Collection<MetricOutputMapByTimeAndThreshold<S>> values();

    /**
     * A builder.
     *
     * @param <S> the type of output to store
     */

    interface MetricOutputMultiMapByTimeAndThresholdBuilder<S extends MetricOutput<?>>
            extends MetricOutputMultiMap.Builder<S>
    {

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param key the result key
         * @param result the result
         * @return the builder
         */

        default MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( MapBiKey<TimeWindow, Threshold> key,
                                                                   MetricOutputMapByMetric<S> result )
        {
            put( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( TimeWindow timeWindow,
                                                           Threshold threshold,
                                                           MetricOutputMapByMetric<S> result );

        /**
         * Adds a new result to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( MapKey<MetricConstants> key,
                                                           MetricOutputMapByTimeAndThreshold<S> result );

    }

}
