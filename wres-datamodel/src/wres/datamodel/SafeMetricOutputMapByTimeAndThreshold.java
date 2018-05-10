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
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;

/**
 * Immutable map of {@link MetricOutput} stored by {@link TimeWindow} and {@link OneOrTwoThresholds} in their natural order.
 *
 * @author james.brown@hydrosolved.com
 */
class SafeMetricOutputMapByTimeAndThreshold<T extends MetricOutput<?>> implements MetricOutputMapByTimeAndThreshold<T>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();

    /**
     * Null threshold.
     */

    private static final String NULL_THRESHOLD_ERROR = "Specify a non-null threshold by which to slice the map.";

    /**
     * Metadata.
     */

    private final MetricOutputMetadata metadata;

    /**
     * Underlying store.
     */

    private final TreeMap<Pair<TimeWindow, OneOrTwoThresholds>, T> store;

    /**
     * Internal array of map keys.
     */

    private final List<Pair<TimeWindow, OneOrTwoThresholds>> internal;

    @Override
    public T get( final Pair<TimeWindow, OneOrTwoThresholds> key )
    {
        return store.get( key );
    }

    @Override
    public Pair<TimeWindow, OneOrTwoThresholds> getKey( final int index )
    {
        return internal.get( index );
    }

    @Override
    public T getValue( final int index )
    {
        return get( getKey( index ) );
    }

    @Override
    public boolean containsKey( final Pair<TimeWindow, OneOrTwoThresholds> key )
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
    public Set<Pair<TimeWindow, OneOrTwoThresholds>> keySet()
    {
        return Collections.unmodifiableSet( store.keySet() );
    }

    @Override
    public Set<Entry<Pair<TimeWindow, OneOrTwoThresholds>, T>> entrySet()
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
    public Set<OneOrTwoThresholds> setOfSecondKey()
    {
        final Set<OneOrTwoThresholds> returnMe = new TreeSet<>();
        store.keySet().forEach( a -> returnMe.add( a.getRight() ) );
        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public Set<TimeWindow> setOfTimeWindowKeyByLeadTime()
    {
        //Group by matching durations
        Function<Pair<TimeWindow, OneOrTwoThresholds>, Pair<Duration, Duration>> groupBy =
                a -> Pair.of( a.getLeft().getEarliestLeadTime(), a.getLeft().getLatestLeadTime() );
        Set<TimeWindow> returnMe = new TreeSet<>();
        store.keySet()
             .stream()
             .collect( Collectors.groupingBy( groupBy ) )
             .forEach( ( key, value ) -> returnMe.add( value.get( 0 ).getLeft() ) );
        return returnMe;
    }


    @Override
    public Set<Threshold> setOfThresholdOne()
    {
        return Collections.unmodifiableSet( store.keySet()
                                                 .stream()
                                                 .map( next -> next.getValue().first() )
                                                 .collect( Collectors.toSet() ) );
    }

    @Override
    public Set<Threshold> setOfThresholdTwo()
    {
        return Collections.unmodifiableSet( store.keySet()
                                                 .stream()
                                                 .filter( next -> next.getRight().hasTwo() )
                                                 .map( next -> next.getValue().second() )
                                                 .collect( Collectors.toSet() ) );
    }

    @Override
    public int size()
    {
        return store.size();
    }

    @Override
    public SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T> subMap( Pair<TimeWindow, OneOrTwoThresholds> fromKey,
                                                                      Pair<TimeWindow, OneOrTwoThresholds> toKey )
    {
        return (SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T>) Collections.unmodifiableMap( store.subMap( fromKey,
                                                                                                               toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T> headMap( Pair<TimeWindow, OneOrTwoThresholds> toKey )
    {
        return (SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T>) Collections.unmodifiableMap( store.headMap( toKey ) );
    }

    @Override
    public SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T> tailMap( Pair<TimeWindow, OneOrTwoThresholds> fromKey )
    {
        return (SortedMap<Pair<TimeWindow, OneOrTwoThresholds>, T>) Collections.unmodifiableMap( store.tailMap( fromKey ) );
    }

    @Override
    public Pair<TimeWindow, OneOrTwoThresholds> firstKey()
    {
        return store.firstKey();
    }

    @Override
    public Pair<TimeWindow, OneOrTwoThresholds> lastKey()
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
    public MetricOutputMapWithBiKey<TimeWindow, OneOrTwoThresholds, T> filterByFirstKey( final TimeWindow first )
    {
        if ( Objects.isNull( first ) )
        {
            throw new MetricOutputException( NULL_THRESHOLD_ERROR );
        }
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> b = new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
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
    public MetricOutputMapWithBiKey<TimeWindow, OneOrTwoThresholds, T>
            filterBySecondKey( final OneOrTwoThresholds second )
    {
        if ( Objects.isNull( second ) )
        {
            throw new MetricOutputException( NULL_THRESHOLD_ERROR );
        }
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> b = new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
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
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> b = new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
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
    public MetricOutputMapByTimeAndThreshold<T> filterByThresholdOne( Threshold threshold )
    {
        if ( Objects.isNull( threshold ) )
        {
            throw new MetricOutputException( NULL_THRESHOLD_ERROR );
        }
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> b = new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        store.forEach( ( key, value ) -> {
            if ( threshold.equals( key.getRight().first() ) )
            {
                b.put( key, value );
            }
        } );
        if ( b.store.isEmpty() )
        {
            throw new MetricOutputException( "No metric outputs match the specified threshold." );
        }
        return b.build();
    }

    @Override
    public MetricOutputMapByTimeAndThreshold<T> filterByThresholdTwo( Threshold threshold )
    {
        if ( Objects.isNull( threshold ) )
        {
            throw new MetricOutputException( NULL_THRESHOLD_ERROR );
        }
        final SafeMetricOutputMapByTimeAndThresholdBuilder<T> b = new SafeMetricOutputMapByTimeAndThresholdBuilder<>();
        store.forEach( ( key, value ) -> {
            if ( threshold.equals( key.getRight().second() ) )
            {
                b.put( key, value );
            }
        } );
        if ( b.store.isEmpty() )
        {
            throw new MetricOutputException( "No metric outputs match the specified threshold." );
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

    static class SafeMetricOutputMapByTimeAndThresholdBuilder<T extends MetricOutput<?>>
    {

        /**
         * The data store.
         */
        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, T> store = new ConcurrentSkipListMap<>();

        /**
         * The metadata.
         */

        private MetricOutputMetadata overrideMeta;

        /**
         * Adds a mapping to the store.
         * 
         * @param key the key
         * @param value the value
         * @return the builder
         */

        protected SafeMetricOutputMapByTimeAndThresholdBuilder<T> put( final Pair<TimeWindow, OneOrTwoThresholds> key,
                                                                       final T value )
        {
            this.store.put( key, value );
            return this;
        }

        /**
         * Sets the override metadata.
         * 
         * @param overrideMeta the override metadata
         * @return the builder
         */

        protected SafeMetricOutputMapByTimeAndThresholdBuilder<T>
                setOverrideMetadata( final MetricOutputMetadata overrideMeta )
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

    private SafeMetricOutputMapByTimeAndThreshold( final SafeMetricOutputMapByTimeAndThresholdBuilder<T> builder )
    {
        //Set then check
        this.store = new TreeMap<>();
        this.store.putAll( builder.store );
        this.internal = new ArrayList<>( this.store.keySet() );

        //Set the metadata, updating the time window to find the union of the inputs, if available
        final MetricOutputMetadata checkAgainst = this.store.firstEntry().getValue().getMetadata();
        MetricOutputMetadata builderLocalMeta;
        if ( Objects.nonNull( builder.overrideMeta ) )
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
            this.store.keySet().forEach( a -> windows.add( a.getLeft() ) );
            if ( !windows.isEmpty() )
            {
                builderLocalMeta =
                        DefaultMetadataFactory.getInstance().getOutputMetadata( builderLocalMeta,
                                                                                TimeWindow.unionOf( windows ) );
            }
        }
        this.metadata = builderLocalMeta;

        //Bounds checks
        if ( this.store.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more <key,value> mappings to build the map." );
        }

        this.store.forEach( ( key, value ) -> {
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
