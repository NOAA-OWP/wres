package wres.datamodel.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

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

    private static final DataFactory dataFactory = DefaultDataFactory.getInstance();

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
        return store.get(dataFactory.getMapKey(metricID, componentID));
    }

    @Override
    public boolean containsKey(MapBiKey<MetricConstants, MetricConstants> key)
    {
        return store.containsKey(key);
    }

    @Override
    public boolean containsValue(MetricOutputMapByLeadThreshold<S> value)
    {
        return store.containsValue(value);
    }

    @Override
    public Collection<MetricOutputMapByLeadThreshold<S>> values()
    {
        return Collections.unmodifiableCollection(store.values());
    }    
    
    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public Set<MapBiKey<MetricConstants, MetricConstants>> keySet()
    {
        return Collections.unmodifiableSet(store.keySet());
    }

    @Override
    public Set<Entry<MapBiKey<MetricConstants, MetricConstants>, MetricOutputMapByLeadThreshold<S>>> entrySet()
    {
        return Collections.unmodifiableSet(store.entrySet());
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
                final MapBiKey<MetricConstants, MetricConstants> check =
                                                                       dataFactory.getMapKey(d.getMetricID(),
                                                                                             d.getMetricComponentID());
                //Safe put
                final SafeMetricOutputMapByLeadThreshold.Builder<S> addMe = new SafeMetricOutputMapByLeadThreshold.Builder<>();
                addMe.put(dataFactory.getMapKey(leadTime, threshold), value);
                final SafeMetricOutputMapByLeadThreshold.Builder<S> checkMe =
                                                                          internal.putIfAbsent(check,
                                                                                               addMe);
                //Add if already exists 
                if(!Objects.isNull(checkMe))
                {
                    checkMe.put(dataFactory.getMapKey(leadTime, threshold), value);
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