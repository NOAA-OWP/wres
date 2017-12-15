package wres.datamodel;

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

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapWithBiKey;
import wres.datamodel.outputs.MetricOutputMetadata;

/**
 * Immutable map of {@link MetricOutput} stored by {@link TimeWindow} and {@link Threshold} in their natural order.
 *
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMetricOutputMapByTimeAndThreshold<T extends MetricOutput<?>> implements MetricOutputMapByTimeAndThreshold<T>
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

    private final TreeMap<Pair<TimeWindow, Threshold>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<Pair<TimeWindow, Threshold>> internal;

    @Override
    public T get( final Pair<TimeWindow, Threshold> key )
    {
        return store.get( key );
    }

    @Override
    public Pair<TimeWindow, Threshold> getKey( final int index )
    {
        return internal.get( index );
    }

    @Override
    public T getValue( final int index )
    {
        return get( getKey( index ) );
    }

    @Override
    public boolean containsKey( final Pair<TimeWindow, Threshold> key )
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
    public Set<Pair<TimeWindow, Threshold>> keySet()
    {
        return Collections.unmodifiableSet( store.keySet() );
    }

    @Override
    public Set<Entry<Pair<TimeWindow, Threshold>, T>> entrySet()
    {
        return Collections.unmodifiableSet( store.entrySet() );
    }

    @Override
    public Set<TimeWindow> keySetByFirstKey()
    {
        final Set<TimeWindow> returnMe = new TreeSet<>();
        store.keySet().forEach( a -> returnMe.add( a.getLeft() ) );
        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public Set<Threshold> keySetBySecondKey()
    {
        final Set<Threshold> returnMe = new TreeSet<>();
        store.keySet().forEach( a -> returnMe.add( a.getRight() ) );
        return Collections.unmodifiableSet( returnMe );
    }
    
    @Override
    public Set<Long> keySetByLeadTimeInHours()
    {
        final Set<Long> returnMe = new TreeSet<>();
        for ( TimeWindow next : keySetByTime() )
        {
            returnMe.add( next.getEarliestLeadTimeInHours() );
            returnMe.add( next.getLatestLeadTimeInHours() );
        }
        return Collections.unmodifiableSet( returnMe );
    }    

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<Pair<TimeWindow, Threshold>, T> subMap( Pair<TimeWindow, Threshold> fromKey,
                                                             Pair<TimeWindow, Threshold> toKey )
    {
        return (SortedMap<Pair<TimeWindow, Threshold>, T>) Collections.unmodifiableMap( store.subMap( fromKey,
                                                                                                      toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, Threshold>, T> headMap( Pair<TimeWindow, Threshold> toKey )
    {
        return (SortedMap<Pair<TimeWindow, Threshold>, T>) Collections.unmodifiableMap( store.headMap( toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, Threshold>, T> tailMap( Pair<TimeWindow, Threshold> fromKey )
    {
        return (SortedMap<Pair<TimeWindow, Threshold>, T>) Collections.unmodifiableMap( store.tailMap( fromKey ) );
    }

    @Override
    public Pair<TimeWindow, Threshold> firstKey()
    {
        return store.firstKey();
    }

    @Override
    public Pair<TimeWindow, Threshold> lastKey()
    {
        return store.lastKey();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( ! ( o instanceof SafeMetricOutputMapByTimeAndThreshold ) )
        {
            return false;
        }
        SafeMetricOutputMapByTimeAndThreshold<?> in = (SafeMetricOutputMapByTimeAndThreshold<?>) o;
        return in.metadata.equals( metadata ) && in.store.equals( store );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( store, metadata );
    }

    @Override
    public MetricOutputMapWithBiKey<TimeWindow, Threshold, T> filterByFirst( final TimeWindow first )
    {
        if ( Objects.isNull( first ) )
        {
            throw new MetricOutputException( "Specify a non-null threshold by which to slice the map." );
        }
        final Builder<T> b = new Builder<>();
        store.forEach( ( key, value ) -> {
            if ( first.equals( key.getLeft() ) )
            {
                b.put( key, value );
            }
        } );
        if ( b.store.isEmpty() )
        {
            throw new MetricOutputException( "No metric outputs match the specified criteria on time window." );
        }
        return b.build();
    }

    @Override
    public MetricOutputMapWithBiKey<TimeWindow, Threshold, T> filterBySecond( final Threshold second )
    {
        if ( Objects.isNull( second ) )
        {
            throw new MetricOutputException( "Specify a non-null threshold by which to slice the map." );
        }
        final Builder<T> b = new Builder<>();
        store.forEach( ( key, value ) -> {
            if ( second.equals( key.getRight() ) )
            {
                b.put( key, value );
            }
        } );
        if ( b.store.isEmpty() )
        {
            throw new MetricOutputException( "No metric outputs match the specified criteria on threshold value." );
        }
        return b.build();
    }


    @Override
    public MetricOutputMapByTimeAndThreshold<T> filterByLeadTimeInHours( long leadHours )
    {
        final Builder<T> b = new Builder<>();
        store.forEach( ( key, value ) -> {
            if ( key.getLeft().getEarliestLeadTimeInHours() == leadHours
                 || key.getLeft().getLatestLeadTimeInHours() == leadHours )
            {
                b.put( key, value );
            }
        } );
        if ( b.store.isEmpty() )
        {
            throw new MetricOutputException( "No metric outputs match the specified criteria on forecast lead time." );
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
        forEach( ( key, value ) -> b.append( "[" )
                                    .append( key.getLeft() )
                                    .append( ", " )
                                    .append( key.getRight() )
                                    .append( ", " )
                                    .append( value )
                                    .append( "]" )
                                    .append( NEWLINE ) );
        int lines = b.length();
        b.delete( lines - NEWLINE.length(), lines );
        return b.toString();
    }

    /**
     * Builds the immutable mapping.
     *
     * @param <T> the metric output
     */

    protected static class Builder<T extends MetricOutput<?>>
    {

        /**
         * The data store.
         */
        private final ConcurrentMap<Pair<TimeWindow, Threshold>, T> store = new ConcurrentSkipListMap<>();

        /**
         * The metadata.
         */

        private MetricOutputMetadata overrideMeta;

        /**
         * The reference metadata.
         */

        private MetricOutputMetadata referenceMetadata;

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         */

        protected Builder<T> put( final Pair<TimeWindow, Threshold> key, final T value )
        {
            if ( Objects.isNull( referenceMetadata ) )
            {
                referenceMetadata = value.getMetadata();
            }
            store.put( key, value );
            return this;
        }

        /**
         * Sets the override metadata.
         * 
         * @param overrideMeta the override metadata
         * @return the builder
         */

        protected Builder<T> setOverrideMetadata( final MetricOutputMetadata overrideMeta )
        {
            this.overrideMeta = overrideMeta;
            return this;
        }

        /**
         * Return the mapping.
         * 
         * @return the mapping
         */

        protected MetricOutputMapByTimeAndThreshold<T> build()
        {
            return new SafeMetricOutputMapByTimeAndThreshold<>( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private SafeMetricOutputMapByTimeAndThreshold( final Builder<T> builder )
    {
        //Bounds checks
        if ( builder.store.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more <key,value> mappings to build the map." );
        }
        final MetricOutputMetadata checkAgainst = builder.referenceMetadata;
        builder.store.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null key for the input map." );
            }
            if ( Objects.isNull( value ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null value for the input map." );
            }
            //Check metadata
            if ( !value.getMetadata().minimumEquals( checkAgainst ) )
            {
                throw new MetricOutputException( "Cannot construct the map from inputs that comprise "
                                                 + "inconsistent metadata." );
            }
        } );
        //Set the metadata
        if ( !Objects.isNull( builder.overrideMeta ) )
        {
            if ( !builder.overrideMeta.minimumEquals( checkAgainst ) )
            {
                throw new MetricOutputException( "Cannot construct the map. The override metadata is "
                                                 + "inconsistent with the metadata of the stored outputs." );
            }
            metadata = builder.overrideMeta;
        }
        else
        {
            metadata = checkAgainst;
        }
        store = new TreeMap<>();
        store.putAll( builder.store );
        internal = new ArrayList<>( store.keySet() );
    }

}
