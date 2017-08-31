package wres.datamodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;

/**
 * Immutable map of {@link MetricOutput} stored by unique metric identifier.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMetricOutputMapByMetric<T extends MetricOutput<?>> implements MetricOutputMapByMetric<T>
{

    /**
     * Underlying store.
     */

    private final TreeMap<MapKey<MetricConstants>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<MapKey<MetricConstants>> internal;

    @Override
    public T get(final MapKey<MetricConstants> key)
    {
        return store.get(key);
    }

    @Override
    public T get(final MetricConstants metricID)
    {
        return get(DefaultDataFactory.getInstance().getMapKey(metricID));
    }

    @Override
    public MapKey<MetricConstants> getKey(final int index)
    {
        return internal.get(index);
    }

    @Override
    public T getValue(final int index)
    {
        return get(getKey(index));
    }

    @Override
    public boolean containsKey(final MapKey<MetricConstants> key)
    {
        return store.containsKey(key);
    }

    @Override
    public boolean containsValue(T value)
    {
        return store.containsValue(value);
    }

    @Override
    public Collection<T> values()
    {
        return Collections.unmodifiableCollection(store.values());
    }

    @Override
    public Set<MapKey<MetricConstants>> keySet()
    {
        return Collections.unmodifiableSet(store.keySet());
    }

    @Override
    public Set<Entry<MapKey<MetricConstants>, T>> entrySet()
    {
        return Collections.unmodifiableSet(store.entrySet());
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> subMap(MapKey<MetricConstants> fromKey,
                                                                           MapKey<MetricConstants> toKey)
    {
        return (SortedMap<MapKey<MetricConstants>, T>)Collections.unmodifiableMap(store.subMap(fromKey,
                                                                                                                  toKey));
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> headMap(MapKey<MetricConstants> toKey)
    {
        return (SortedMap<MapKey<MetricConstants>, T>)Collections.unmodifiableMap(store.headMap(toKey));
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> tailMap(MapKey<MetricConstants> fromKey)
    {
        return (SortedMap<MapKey<MetricConstants>, T>)Collections.unmodifiableMap(store.tailMap(fromKey));
    }

    @Override
    public MapKey<MetricConstants> firstKey()
    {
        return store.firstKey();
    }

    @Override
    public MapKey<MetricConstants> lastKey()
    {
        return store.lastKey();
    }

    /**
     * Builds the immutable mapping.
     *
     * @param <T> the metric output
     */

    protected static class Builder<T extends MetricOutput<?>>
    {

        private final TreeMap<MapKey<MetricConstants>, T> store = new TreeMap<>();

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         */

        protected Builder<T> put(final MapKey<MetricConstants> key, final T value)
        {
            store.put(key, value);
            return this;
        }

        /**
         * Return the mapping.
         * 
         * @return the mapping
         */

        protected MetricOutputMapByMetric<T> build()
        {
            return new SafeMetricOutputMapByMetric<>(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeMetricOutputMapByMetric(final Builder<T> builder)
    {
        //Bounds checks
        if(builder.store.isEmpty())
        {
            throw new UnsupportedOperationException("Specify one or more <key,value> mappings to build the map.");
        }
        builder.store.forEach((key, value) -> {
            if(Objects.isNull(key))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null key for the input map.");
            }
            if(Objects.isNull(value))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null value for the input map.");
            }
        });
        store = new TreeMap<>();
        store.putAll(builder.store);
        internal = new ArrayList<>(store.keySet());
    }

    /**
     * Return a string representation.
     * 
     * @return a string representation
     */
    @Override
    public String toString()
    {
        StringJoiner joiner  = new StringJoiner(",","[","]");
        this.forEach((key, value) -> joiner.add(value.toString()));
        return joiner.toString();
    }

}
