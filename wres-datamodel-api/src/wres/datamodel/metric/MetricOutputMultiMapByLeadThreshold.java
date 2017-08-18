package wres.datamodel.metric;

import java.util.Map;

/**
 * A map of {@link MetricOutputMapByLeadThreshold} stored by metric identifier. Implements the same read-only API as the
 * {@link Map}. However, for an immutable implementation, changes in the returned values are not backed by this map. A
 * builder is included to support construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputMultiMapByLeadThreshold<S extends MetricOutput<?>>
extends MetricOutputMultiMap<MetricOutputMapByLeadThreshold<S>>
{

    /**
     * A builder.
     *
     * @param <S> the type of output to store
     */

    interface Builder<S extends MetricOutput<?>>
    extends MetricOutputMultiMap.Builder<MetricOutputMapByLeadThreshold<S>>
    {

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param key the result key
         * @param result the result
         * @return the builder
         */

        default Builder<S> put(MapBiKey<Integer, Threshold> key, MetricOutputMapByMetric<S> result)
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

        Builder<S> put(int leadTime, Threshold threshold, MetricOutputMapByMetric<S> result);

    }

}
