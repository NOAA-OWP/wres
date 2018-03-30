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

import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputMapByMetric;

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
    public T get( final MapKey<MetricConstants> key )
    {
        return store.get( key );
    }

    @Override
    public T get( final MetricConstants metricID )
    {
        return get( DefaultDataFactory.getInstance().getMapKey( metricID ) );
    }

    @Override
    public MapKey<MetricConstants> getKey( final int index )
    {
        return internal.get( index );
    }

    @Override
    public T getValue( final int index )
    {
        return get( getKey( index ) );
    }

    @Override
    public boolean containsKey( final MapKey<MetricConstants> key )
    {
        return store.containsKey( key );
    }

    @Override
    public boolean containsValue( T value )
    {
        return store.containsValue( value );
    }

    @Override
    public Collection<T> values()
    {
        return Collections.unmodifiableCollection( store.values() );
    }

    @Override
    public Set<MapKey<MetricConstants>> keySet()
    {
        return Collections.unmodifiableSet( store.keySet() );
    }

    @Override
    public Set<Entry<MapKey<MetricConstants>, T>> entrySet()
    {
        return Collections.unmodifiableSet( store.entrySet() );
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> subMap( MapKey<MetricConstants> fromKey,
                                                         MapKey<MetricConstants> toKey )
    {
        return (SortedMap<MapKey<MetricConstants>, T>) Collections.unmodifiableMap( store.subMap( fromKey,
                                                                                                  toKey ) );
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> headMap( MapKey<MetricConstants> toKey )
    {
        return (SortedMap<MapKey<MetricConstants>, T>) Collections.unmodifiableMap( store.headMap( toKey ) );
    }

    @Override
    public SortedMap<MapKey<MetricConstants>, T> tailMap( MapKey<MetricConstants> fromKey )
    {
        return (SortedMap<MapKey<MetricConstants>, T>) Collections.unmodifiableMap( store.tailMap( fromKey ) );
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

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof SafeMetricOutputMapByMetric ) )
        {
            return false;
        }
        return store.equals( ( (SafeMetricOutputMapByMetric<?>) o ).store );
    }
    
    @Override
    public int hashCode( )
    {
        return Objects.hashCode( store );
    }    

    /**
     * Builds the immutable mapping.
     *
     * @param <T> the metric output
     */

    static class SafeMetricOutputMapByMetricBuilder<T extends MetricOutput<?>>
    {

        private final TreeMap<MapKey<MetricConstants>, T> store = new TreeMap<>();

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         * @throws MetricOutputException if the input already exists in this store
         */

        SafeMetricOutputMapByMetricBuilder<T> put( final MapKey<MetricConstants> key, final T value )
        {
            if ( store.containsKey( key ) )
            {
                throw new MetricOutputException( "While attempting to add a '" + key.getKey()
                                                 + "' to a store that already "
                                                 + "contains one." );
            }
            store.put( key, value );
            return this;
        }

        /**
         * Adds an existing {@link MetricOutputMapByMetric} to the store.
         * 
         * @param map the existing map
         * @return the builder
         * @throws MetricOutputException if one or more of the inputs already exist in this store
         */

        SafeMetricOutputMapByMetricBuilder<T> put( MetricOutputMapByMetric<T> map )
        {
            map.forEach( this::put );
            return this;
        }

        /**
         * Return the mapping.
         * 
         * @return the mapping
         */

        MetricOutputMapByMetric<T> build()
        {
            return new SafeMetricOutputMapByMetric<>( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private SafeMetricOutputMapByMetric( final SafeMetricOutputMapByMetricBuilder<T> builder )
    {
        store = new TreeMap<>();
        store.putAll( builder.store );
        internal = new ArrayList<>( store.keySet() );
        //Bounds checks
        if ( store.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more <key,value> mappings to build the map." );
        }
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

    /**
     * Return a string representation.
     * 
     * @return a string representation
     */
    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );
        this.forEach( ( key, value ) -> joiner.add( value.toString() ) );
        return joiner.toString();
    }

}
