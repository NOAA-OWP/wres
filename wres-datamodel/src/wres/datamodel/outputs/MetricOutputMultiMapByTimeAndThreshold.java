package wres.datamodel.outputs;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * A map of {@link MetricOutputMapByTimeAndThreshold} stored by metric identifier. Implements the same read-only API as the
 * {@link Map}. However, for an immutable implementation, changes in the returned values are not backed by this map. A
 * builder is included to support construction on-the-fly from inputs of {@link MetricOutputMapByMetric}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricOutputMultiMapByTimeAndThreshold<S extends MetricOutput<?>> implements MetricOutputMultiMap<S>
{

    /**
     * The store of results.
     */

    private final TreeMap<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>> store;

    /**
     * Returns a {@link MetricOutputMapByTimeAndThreshold} corresponding to the input identifiers or null
     * 
     * @param key the key
     * @return the mapping or null
     */

    public MetricOutputMapByTimeAndThreshold<S> get( final MapKey<MetricConstants> key )
    {
        return get( key.getKey() );
    }

    /**
     * Consume each pair in the map.
     * 
     * @param consumer the consumer
     */

    public void forEach( BiConsumer<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>> consumer )
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

    public MetricOutputMapByTimeAndThreshold<S> get( final MetricConstants metricID )
    {
        return store.get( DataFactory.getMapKey( metricID ) );
    }

    /**
     * Returns a view of the entries in the map for iteration.
     * 
     * @return a view of the map entries
     */

    public Set<Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>>> entrySet()
    {
        return Collections.unmodifiableSet( store.entrySet() );
    }

    /**
     * Returns true if the mapping contains the specified value, false otherwise.
     * 
     * @param value the value
     * @return true if the map contains the value, false otherwise
     */

    public boolean containsValue( MetricOutputMapByTimeAndThreshold<S> value )
    {
        return store.containsValue( value );
    }

    /**
     * Returns a collection view of the values in the map.
     * 
     * @return a collection view of the values
     */

    public Collection<MetricOutputMapByTimeAndThreshold<S>> values()
    {
        return Collections.unmodifiableCollection( store.values() );
    }

    @Override
    public MetricOutputMultiMapByTimeAndThresholdBuilder<S> builder()
    {
        return new MetricOutputMultiMapByTimeAndThresholdBuilder<>();
    }

    @Override
    public boolean containsKey( MapKey<MetricConstants> key )
    {
        return store.containsKey( key );
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public Set<MapKey<MetricConstants>> keySet()
    {
        return Collections.unmodifiableSet( store.keySet() );
    }

    @Override
    public String toString()
    {
        String newLine = System.getProperty( "line.separator" );
        StringBuilder b = new StringBuilder();
        store.forEach( ( key, value ) -> {
            b.append( key.getKey() );
            b.append( newLine );
            b.append( value );
            b.append( newLine );
        } );
        return b.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof MetricOutputMultiMapByTimeAndThreshold ) )
        {
            return false;
        }
        return ( (MetricOutputMultiMapByTimeAndThreshold<?>) o ).store.equals( store );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( store );
    }

    /**
     * A builder.
     *
     * @param <S> the type of output to store
     */

    public static class MetricOutputMultiMapByTimeAndThresholdBuilder<S extends MetricOutput<?>>
            implements MetricOutputMultiMap.Builder<S>
    {

        /**
         * Thread safe map.
         */

        final ConcurrentMap<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<S>> internal =
                new ConcurrentSkipListMap<>();

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param key the result key
         * @param result the result
         * @return the builder
         * @throws NullPointerException if the key is null
         */

        public MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                                     MetricOutputMapByMetric<S> result )
        {
            Objects.requireNonNull( key, "Specify a non null key." );
            put( key.getLeft(), key.getRight(), result );
            return this;
        }

        @Override
        public MetricOutputMultiMapByTimeAndThreshold<S> build()
        {
            return new MetricOutputMultiMapByTimeAndThreshold<>( this );
        }

        /**
         * Adds a new result for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( final TimeWindow timeWindow,
                                                                     final OneOrTwoThresholds threshold,
                                                                     final MetricOutputMapByMetric<S> result )
        {
            if ( Objects.isNull( result ) )
            {
                throw new MetricOutputException( "Specify a non-null metric result." );
            }
            result.forEach( ( key, value ) -> {
                final MetricOutputMetadata d = value.getMetadata();
                final MapKey<MetricConstants> check =
                        DataFactory.getMapKey( d.getMetricID() );
                //Safe put
                final MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<S> addMe =
                        new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();
                addMe.put( Pair.of( timeWindow, threshold ), value );
                final MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<S> checkMe =
                        internal.putIfAbsent( check, addMe );
                //Add if already exists 
                if ( !Objects.isNull( checkMe ) )
                {
                    checkMe.put( Pair.of( timeWindow, threshold ), value );
                }
            } );
            return this;
        }

        /**
         * Adds a new result to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        public MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( MapKey<MetricConstants> key,
                                                                     MetricOutputMapByTimeAndThreshold<S> result )
        {
            if ( Objects.isNull( result ) )
            {
                throw new MetricOutputException( "Specify a non-null metric result." );
            }
            //Safe put
            final MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<S> addMe =
                    new MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<>();
            result.forEach( addMe::put );
            final MetricOutputMapByTimeAndThreshold.MetricOutputMapByTimeAndThresholdBuilder<S> checkMe =
                    internal.putIfAbsent( key, addMe );
            //Add if already exists 
            if ( !Objects.isNull( checkMe ) )
            {
                result.forEach( checkMe::put );
            }
            return this;
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricOutputException if any of the inputs are invalid
     */
    private MetricOutputMultiMapByTimeAndThreshold( final MetricOutputMultiMapByTimeAndThresholdBuilder<S> builder )
    {
        //Initialize
        store = new TreeMap<>();
        //Build
        builder.internal.forEach( ( key, value ) -> store.put( key, value.build() ) );
        //Bounds checks
        if ( store.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more <key,value> mappings to build the map." );
        }
        //Bounds checks
        store.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null key for the input map." );
            }
            if ( Objects.isNull( value ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null value for the input map." );
            }
        } );
    }

}
