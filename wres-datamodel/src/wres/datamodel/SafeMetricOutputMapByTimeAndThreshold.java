package wres.datamodel;

import java.time.Duration;
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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapWithBiKey;

/**
 * Immutable map of {@link MetricOutput} stored by {@link TimeWindow} and {@link Thresholds} in their natural order.
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

    private final TreeMap<Pair<TimeWindow, Thresholds>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<Pair<TimeWindow, Thresholds>> internal;

    @Override
    public T get( final Pair<TimeWindow, Thresholds> key )
    {
        return store.get( key );
    }

    @Override
    public Pair<TimeWindow, Thresholds> getKey( final int index )
    {
        return internal.get( index );
    }

    @Override
    public T getValue( final int index )
    {
        return get( getKey( index ) );
    }

    @Override
    public boolean containsKey( final Pair<TimeWindow, Thresholds> key )
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
    public Set<Pair<TimeWindow, Thresholds>> keySet()
    {
        return Collections.unmodifiableSet( store.keySet() );
    }

    @Override
    public Set<Entry<Pair<TimeWindow, Thresholds>, T>> entrySet()
    {
        return Collections.unmodifiableSet( store.entrySet() );
    }

    @Override
    public Set<TimeWindow> setOfFirstKey()
    {
        final Set<TimeWindow> returnMe = new TreeSet<>();
        store.keySet().forEach( a -> returnMe.add( a.getLeft() ) );
        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public Set<Thresholds> setOfSecondKey()
    {
        final Set<Thresholds> returnMe = new TreeSet<>();
        store.keySet().forEach( a -> returnMe.add( a.getRight() ) );
        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public Set<TimeWindow> setOfTimeWindowKeyByLeadTime()
    {
        //Group by matching durations
        Function<Pair<TimeWindow, Thresholds>, Pair<Duration, Duration>> groupBy =
                a -> Pair.of( a.getLeft().getEarliestLeadTime(), a.getLeft().getLatestLeadTime() );
        Set<TimeWindow> returnMe = new TreeSet<>();
        store.keySet()
             .stream()
             .collect( Collectors.groupingBy( groupBy ) )
             .forEach( ( key, value ) -> returnMe.add( value.get( 0 ).getLeft() ) );
        return returnMe;
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<Pair<TimeWindow, Thresholds>, T> subMap( Pair<TimeWindow, Thresholds> fromKey,
                                                             Pair<TimeWindow, Thresholds> toKey )
    {
        return (SortedMap<Pair<TimeWindow, Thresholds>, T>) Collections.unmodifiableMap( store.subMap( fromKey,
                                                                                                      toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, Thresholds>, T> headMap( Pair<TimeWindow, Thresholds> toKey )
    {
        return (SortedMap<Pair<TimeWindow, Thresholds>, T>) Collections.unmodifiableMap( store.headMap( toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, Thresholds>, T> tailMap( Pair<TimeWindow, Thresholds> fromKey )
    {
        return (SortedMap<Pair<TimeWindow, Thresholds>, T>) Collections.unmodifiableMap( store.tailMap( fromKey ) );
    }

    @Override
    public Pair<TimeWindow, Thresholds> firstKey()
    {
        return store.firstKey();
    }

    @Override
    public Pair<TimeWindow, Thresholds> lastKey()
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
    public MetricOutputMapWithBiKey<TimeWindow, Thresholds, T> filterByFirstKey( final TimeWindow first )
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
    public MetricOutputMapWithBiKey<TimeWindow, Thresholds, T> filterBySecondKey( final Thresholds second )
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
    public MetricOutputMapByTimeAndThreshold<T> filterByLeadTime( TimeWindow window )
    {
        if ( Objects.isNull( window ) )
        {
            throw new MetricOutputException( "Specify a non-null time window by which to slice the map." );
        }
        final Builder<T> b = new Builder<>();
        store.forEach( ( key, value ) -> {
            if ( key.getKey().getEarliestLeadTime().equals( window.getEarliestLeadTime() )
                 && key.getKey().getLatestLeadTime().equals( window.getLatestLeadTime() ) )
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
        private final ConcurrentMap<Pair<TimeWindow, Thresholds>, T> store = new ConcurrentSkipListMap<>();

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

        protected Builder<T> put( final Pair<TimeWindow, Thresholds> key, final T value )
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
        //Set then check
        store = new TreeMap<>();
        store.putAll( builder.store );
        internal = new ArrayList<>( store.keySet() );

        //Set the metadata, updating the time window to find the union of the inputs, if available
        final MetricOutputMetadata checkAgainst = builder.referenceMetadata;
        MetricOutputMetadata builderLocalMeta;
        if ( !Objects.isNull( builder.overrideMeta ) )
        {
            builderLocalMeta = builder.overrideMeta;
            if ( !builderLocalMeta.minimumEquals( checkAgainst ) )
            {
                throw new MetricOutputException( "Cannot construct the map. The override metadata is "
                                                 + "inconsistent with the metadata of the stored outputs." );
            }
        }
        else
        {
            builderLocalMeta = checkAgainst;
        }

        //Update the metadata with the union of the time windows
        if ( builderLocalMeta.hasTimeWindow() )
        {
            List<TimeWindow> windows = new ArrayList<>();
            store.keySet().forEach( a -> windows.add( a.getLeft() ) );
            if ( !windows.isEmpty() )
            {
                builderLocalMeta =
                        DefaultMetadataFactory.getInstance().getOutputMetadata( builderLocalMeta,
                                                                                TimeWindow.unionOf( windows ) );
            }
        }
        metadata = builderLocalMeta;

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
            //Check metadata
            if ( !value.getMetadata().minimumEquals( checkAgainst ) )
            {
                throw new MetricOutputException( "Cannot construct the map from inputs that comprise "
                                                 + "inconsistent metadata." );
            }
        } );

    }

}
