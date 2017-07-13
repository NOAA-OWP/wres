package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.BiConsumer;

/**
 * Immutable map of {@link MetricOutput} stored by unique metric identifier.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class SafeMetricOutputMapByMetric<T extends MetricOutput<?>> implements MetricOutputMapByMetric<T>
{

    /**
     * Underlying store.
     */

    private final TreeMap<MapBiKey<MetricConstants, MetricConstants>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<MapBiKey<MetricConstants, MetricConstants>> internal;

    @Override
    public T get(final MapBiKey<MetricConstants, MetricConstants> key)
    {
        return store.get(key);
    }

    @Override
    public T get(final MetricConstants metricID, final MetricConstants componentID)
    {
        return get(DefaultMetricOutputFactory.getInstance().getMapKey(metricID, componentID));
    }

    @Override
    public MapBiKey<MetricConstants, MetricConstants> getKey(final int index)
    {
        return internal.get(index);
    }

    @Override
    public T getValue(final int index)
    {
        return get(getKey(index));
    }

    @Override
    public void forEach(final BiConsumer<MapBiKey<MetricConstants, MetricConstants>, T> consumer)
    {
        store.forEach(consumer);
    }

    @Override
    public Set<MapBiKey<MetricConstants, MetricConstants>> keySet()
    {
        final Set<MapBiKey<MetricConstants, MetricConstants>> returnMe = new TreeSet<>(store.keySet());
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public Set<MetricConstants> keySetByFirstKey()
    {
        final Set<MetricConstants> returnMe = new TreeSet<>();
        store.keySet().forEach(a -> returnMe.add(a.getFirstKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public Set<MetricConstants> keySetBySecondKey()
    {
        final Set<MetricConstants> returnMe = new TreeSet<>();
        store.keySet().forEach(a -> returnMe.add(a.getSecondKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public MetricOutputMapWithBiKey<MetricConstants, MetricConstants, T> sliceByFirst(final MetricConstants first)
    {
        Objects.requireNonNull(first, "Specify a non-null threshold by which to slice the map.");
        final Builder<T> b = new Builder<>();
        store.forEach((key, value) -> {
            if(first.equals(key.getFirstKey()))
            {
                b.put(key, value);
            }
        });
        if(b.store.isEmpty())
        {
            throw new IllegalArgumentException("No metric outputs match the specified criteria on forecast lead time.");
        }
        return b.build();
    }

    @Override
    public MetricOutputMapWithBiKey<MetricConstants, MetricConstants, T> sliceBySecond(final MetricConstants second)
    {
        Objects.requireNonNull(second, "Specify a non-null threshold by which to slice the map.");
        final Builder<T> b = new Builder<>();
        store.forEach((key, value) -> {
            if(second.equals(key.getSecondKey()))
            {
                b.put(key, value);
            }
        });
        if(b.store.isEmpty())
        {
            throw new IllegalArgumentException("No metric outputs match the specified criteria on threshold value.");
        }
        return b.build();
    }

    /**
     * Builds the immutable mapping.
     *
     * @param <T> the metric output
     */

    protected static class Builder<T extends MetricOutput<?>>
    {

        private final TreeMap<MapBiKey<MetricConstants, MetricConstants>, T> store = new TreeMap<>();

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         */

        protected Builder<T> put(final MapBiKey<MetricConstants, MetricConstants> key, final T value)
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
        final StringBuilder b = new StringBuilder();
        b.append("[");
        this.forEach((key, value) -> b.append(value).append(","));
        b.delete(b.length() - 1, b.length());
        b.append("]");
        return b.toString();
    }

}
