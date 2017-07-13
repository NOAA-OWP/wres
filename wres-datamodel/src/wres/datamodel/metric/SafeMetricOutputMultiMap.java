package wres.datamodel.metric;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

/**
 * Default implementation of a safe multi-map that contains {@link MetricOutputMapByLeadThreshold} for several metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputMultiMap<S extends MetricOutput<?>> implements MetricOutputMultiMap<S>
{
    
    /**
     * Output factory.
     */

    private static final MetricOutputFactory outFactory = DefaultMetricOutputFactory.getInstance();  
    
    /**
     * The store of results.
     */

    private final TreeMap<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>> store;

    @Override
    public Builder<S> builder()
    {
        return new MultiMapBuilder<>();
    }

    @Override
    public MetricOutputMapByLeadThreshold<S> get(final MetricConstants metricID, final MetricConstants componentID)
    {
        return store.get(outFactory.getMapKey(metricID, componentID));
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public Set<MapBiKey<MetricConstants, MetricConstants>> keySet()
    {
        final Set<MapBiKey<MetricConstants, MetricConstants>> returnMe = new TreeSet<>(store.keySet());
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public void forEach(BiConsumer<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>> consumer)
    {
        store.forEach(consumer);
    }

    protected static class MultiMapBuilder<S extends MetricOutput<?>> implements Builder<S>
    {

        /**
         * Thread safe map.
         */

        final ConcurrentMap<MapBiKey<MetricConstants, MetricConstants>, SafeMetricOutputMapByLeadThreshold.Builder<S>> internal =
                                                                                                                                new ConcurrentSkipListMap<>();

        @Override
        public SafeMetricOutputMultiMap<S> build()
        {
            return new SafeMetricOutputMultiMap<>(this);
        }

        @Override
        public wres.datamodel.metric.MetricOutputMultiMap.Builder<S> add(final int leadTime,
                                                                         final Threshold threshold,
                                                                         final MetricOutputMapByMetric<S> result)
        {
            Objects.requireNonNull(result, "Specify a non-null metric result.");
            result.forEach((key, value) -> {
                final MetricOutputMetadata d = value.getMetadata();
                final MapBiKey<MetricConstants, MetricConstants> check = outFactory.getMapKey(d.getMetricID(),
                                                                                   d.getMetricComponentID());
                if(internal.containsKey(check))
                {
                    internal.get(check).put(outFactory.getMapKey(leadTime, threshold), value);
                }
                else
                {
                    final SafeMetricOutputMapByLeadThreshold.Builder<S> addMe =
                                                                              new SafeMetricOutputMapByLeadThreshold.Builder<>();
                    addMe.put(outFactory.getMapKey(leadTime, threshold), value);
                    internal.put(key,addMe);
                }
            });
            return this;
        }
    }

    /**
     * Main constructor.
     * 
     * @param builder the builder
     */
    private SafeMetricOutputMultiMap(final MultiMapBuilder<S> builder)
    {
        //Bounds checks
        builder.internal.forEach((key, value) -> {
            if(Objects.isNull(key))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null key for the input map.");
            }
            if(Objects.isNull(value))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null value for the input map.");
            }
        });
        //Initialize
        store = new TreeMap<>();     
        //Build
        builder.internal.forEach((key, value) -> store.put(key, value.build()));
    }
}