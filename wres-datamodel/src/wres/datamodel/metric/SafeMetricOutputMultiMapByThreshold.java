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
 * Default implementation of a safe map that contains {@link MetricOutputMapByMetric} for one or more {@link Threshold}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputMultiMapByThreshold<S extends MetricOutput<?>> implements MetricOutputMultiMapByThreshold<S>
{

    /**
     * Output factory.
     */

    private static final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * The store of results.
     */

    private final TreeMap<MapKey<Threshold>, MetricOutputMapByMetric<S>> store;

    @Override
    public Builder<S> builder()
    {
        return new MultiMapBuilder<>();
    }

    @Override
    public MetricOutputMapByMetric<S> get(final Threshold threshold)
    {
        return store.get(dataFactory.getMapKey(threshold));
    }

    @Override
    public boolean containsKey(MapKey<Threshold> key)
    {
        return store.containsKey(key);
    }

    @Override
    public boolean containsValue(MetricOutputMapByMetric<S> value)
    {
        return store.containsValue(value);
    }

    @Override
    public Collection<MetricOutputMapByMetric<S>> values()
    {
        return Collections.unmodifiableCollection(store.values());
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public Set<MapKey<Threshold>> keySet()
    {
        return Collections.unmodifiableSet(store.keySet());
    }

    @Override
    public Set<Entry<MapKey<Threshold>, MetricOutputMapByMetric<S>>> entrySet()
    {
        return Collections.unmodifiableSet(store.entrySet());
    }

    /**
     * Builder.
     * 
     * @param <S> the input type
     */
    
    protected static class MultiMapBuilder<S extends MetricOutput<?>> implements Builder<S>
    {

        /**
         * Thread safe map.
         */

        final ConcurrentMap<MapKey<Threshold>, MetricOutputMapByMetric<S>> internal = new ConcurrentSkipListMap<>();

        @Override
        public SafeMetricOutputMultiMapByThreshold<S> build()
        {
            return new SafeMetricOutputMultiMapByThreshold<>(this);
        }

        @Override
        public Builder<S> put(final Threshold threshold, final MetricOutputMapByMetric<S> result)
        {
            internal.put(dataFactory.getMapKey(threshold), result);
            return this;
        }
    }

    /**
     * Main constructor.
     * 
     * @param builder the builder
     */
    private SafeMetricOutputMultiMapByThreshold(final MultiMapBuilder<S> builder)
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
        store = new TreeMap<>(builder.internal);
    }

}