package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;

/**
 * Immutable map of {@link MetricOutput} stored by forecast lead time and threshold in their natural order.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public class SafeMetricOutputMapByLeadThreshold<T extends MetricOutput<?>> implements MetricOutputMapByLeadThreshold<T>
{

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
    public void forEach(final BiConsumer<MapBiKey<Integer, Threshold>, T> consumer)
    {
        store.forEach(consumer);
    }

    @Override
    public Set<MapBiKey<Integer, Threshold>> keySet()
    {
        final Set<MapBiKey<Integer, Threshold>> returnMe = new TreeSet<>(store.keySet());
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public Set<Integer> keySetByFirstKey()
    {
        final Set<Integer> returnMe = new TreeSet<Integer>();
        store.keySet().forEach(a -> returnMe.add(a.getFirstKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public Set<Threshold> keySetBySecondKey()
    {
        final Set<Threshold> returnMe = new TreeSet<Threshold>();
        store.keySet().forEach(a -> returnMe.add(a.getSecondKey()));
        return Collections.unmodifiableSet(returnMe);
    }

    @Override
    public int size()
    {
        return store.size();
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
            if(Objects.isNull(referenceMetadata)) {
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
