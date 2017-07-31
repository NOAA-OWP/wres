package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Immutable map of {@link MetricOutput} stored by forecast lead time and threshold in their natural order.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMetricOutputMapByLeadThreshold<T extends MetricOutput<?>> implements MetricOutputMapByLeadThreshold<T>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();

    /**
     * Metadata.
     */

    private final MetricOutputMetadata metadata;

    /**
     * Underlying store.
     */

    private final TreeMap<MapBiKey<Integer, Threshold>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<MapBiKey<Integer, Threshold>> internal;

    @Override
    public T get(final MapBiKey<Integer, Threshold> key)
    {
        return store.get(key);
    }

    @Override
    public MapBiKey<Integer, Threshold> getKey(final int index)
    {
        return internal.get(index);
    }

    @Override
    public T getValue(final int index)
    {
        return get(getKey(index));
    }

    @Override
    public boolean containsKey(final MapBiKey<Integer, Threshold> key)
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
    public Set<MapBiKey<Integer, Threshold>> keySet()
    {
        return Collections.unmodifiableSet(store.keySet());
    }

    @Override
    public Set<Entry<MapBiKey<Integer, Threshold>, T>> entrySet()
    {
        return Collections.unmodifiableSet(store.entrySet());
    }

    @Override
    public Set<Integer> keySetByFirstKey()
    {
        final Set<Integer> returnMe = new TreeSet<>();
        store.keySet().forEach(a -> returnMe.add(a.getFirstKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public Set<Threshold> keySetBySecondKey()
    {
        final Set<Threshold> returnMe = new TreeSet<>();
        store.keySet().forEach(a -> returnMe.add(a.getSecondKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<MapBiKey<Integer, Threshold>, T> subMap(MapBiKey<Integer, Threshold> fromKey,
                                                             MapBiKey<Integer, Threshold> toKey)
    {
        return (SortedMap<MapBiKey<Integer, Threshold>, T>)Collections.unmodifiableMap(store.subMap(fromKey, toKey));
    }

    @Override
    public SortedMap<MapBiKey<Integer, Threshold>, T> headMap(MapBiKey<Integer, Threshold> toKey)
    {
        return (SortedMap<MapBiKey<Integer, Threshold>, T>)Collections.unmodifiableMap(store.headMap(toKey));
    }

    @Override
    public SortedMap<MapBiKey<Integer, Threshold>, T> tailMap(MapBiKey<Integer, Threshold> fromKey)
    {
        return (SortedMap<MapBiKey<Integer, Threshold>, T>)Collections.unmodifiableMap(store.tailMap(fromKey));
    }

    @Override
    public MapBiKey<Integer, Threshold> firstKey()
    {
        return store.firstKey();
    }

    @Override
    public MapBiKey<Integer, Threshold> lastKey()
    {
        return store.lastKey();
    }
    
    @Override
    public boolean equals(Object o) {
        if(! (o instanceof SafeMetricOutputMapByLeadThreshold)) {
            return false;
        }
        SafeMetricOutputMapByLeadThreshold<?> in = (SafeMetricOutputMapByLeadThreshold<?>)o;
        return in.metadata.equals(metadata) && in.store.equals(store);     
    }
    
    @Override
    public int hashCode() {
        return metadata.hashCode() + store.hashCode();     
    }    

    @Override
    public MetricOutputMapWithBiKey<Integer, Threshold, T> sliceByFirst(final Integer first)
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
    public MetricOutputMapWithBiKey<Integer, Threshold, T> sliceBySecond(final Threshold second)
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

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return metadata;
    }

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        forEach((key, value) -> b.append("[")
                                 .append(key.getFirstKey())
                                 .append(", ")
                                 .append(key.getSecondKey())
                                 .append(", ")
                                 .append(value)
                                 .append("]")
                                 .append(NEWLINE));
        int lines = b.length();
        b.delete(lines - NEWLINE.length(), lines);
        return b.toString();
    }

    /**
     * Builds the immutable mapping.
     *
     * @param <T> the metric output
     */

    protected static class Builder<T extends MetricOutput<?>>
    {

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, T> store = new ConcurrentSkipListMap<>();
        private MetricOutputMetadata overrideMeta;
        private MetricOutputMetadata referenceMetadata;

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         */

        protected Builder<T> put(final MapBiKey<Integer, Threshold> key, final T value)
        {
            if(Objects.isNull(referenceMetadata))
            {
                referenceMetadata = value.getMetadata();
            }
            store.put(key, value);
            return this;
        }

        /**
         * Sets the override metadata.
         * 
         * @param overrideMeta the override metadata
         * @return the builder
         */

        protected Builder<T> setOverrideMetadata(final MetricOutputMetadata overrideMeta)
        {
            this.overrideMeta = overrideMeta;
            return this;
        }

        /**
         * Return the mapping.
         * 
         * @return the mapping
         */

        protected MetricOutputMapByLeadThreshold<T> build()
        {
            return new SafeMetricOutputMapByLeadThreshold<>(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeMetricOutputMapByLeadThreshold(final Builder<T> builder)
    {
        //Bounds checks
        if(builder.store.isEmpty())
        {
            throw new UnsupportedOperationException("Specify one or more <key,value> mappings to build the map.");
        }
        final MetricOutputMetadata checkAgainst = builder.referenceMetadata;
        builder.store.forEach((key, value) -> {
            if(Objects.isNull(key))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null key for the input map.");
            }
            if(Objects.isNull(value))
            {
                throw new UnsupportedOperationException("Cannot prescribe a null value for the input map.");
            }
            //Check metadata
            if(!value.getMetadata().minimumEquals(checkAgainst))
            {
                throw new UnsupportedOperationException("Cannot construct the map from inputs that comprise "
                    + "inconsistent metadata.");
            }
        });
        //Set the metadata
        if(!Objects.isNull(builder.overrideMeta))
        {
            if(!builder.overrideMeta.minimumEquals(checkAgainst))
            {
                throw new UnsupportedOperationException("Cannot construct the map. The override metadata is "
                    + "inconsistent with the metadata of the stored outputs.");
            }
            metadata = builder.overrideMeta;
        }
        else
        {
            metadata = checkAgainst;
        }
        store = new TreeMap<>();
        store.putAll(builder.store);
        internal = new ArrayList<>(store.keySet());
    }

}
