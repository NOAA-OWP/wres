package wres.datamodel;

import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.outputs.MapKey;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.time.TimeWindow;

/**
 * Default implementation of a safe map that contains {@link MetricOutputMapByTimeAndThreshold} for several metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputMultiMapByTimeAndThreshold<S extends MetricOutput<?>>
        implements MetricOutputMultiMapByTimeAndThreshold<S>
{

    /**
     * Output factory.
     */

    private static final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * The store of results.
     */

    private final TreeMap<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>> store;

    @Override
    public SafeMetricOutputMultiMapByTimeAndThresholdBuilder<S> builder()
    {
        return new SafeMetricOutputMultiMapByTimeAndThresholdBuilder<>();
    }

    @Override
    public MetricOutputMapByTimeAndThreshold<S> get( final MetricConstants metricID )
    {
        return store.get( dataFactory.getMapKey( metricID ) );
    }

    @Override
    public boolean containsKey( MapKey<MetricConstants> key )
    {
        return store.containsKey( key );
    }

    @Override
    public boolean containsValue( MetricOutputMapByTimeAndThreshold<S> value )
    {
        return store.containsValue( value );
    }

    @Override
    public Collection<MetricOutputMapByTimeAndThreshold<S>> values()
    {
        return Collections.unmodifiableCollection( store.values() );
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
    public Set<Entry<MapKey<MetricConstants>, MetricOutputMapByTimeAndThreshold<S>>> entrySet()
    {
        return Collections.unmodifiableSet( store.entrySet() );
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

    protected static class SafeMetricOutputMultiMapByTimeAndThresholdBuilder<S extends MetricOutput<?>>
            implements MetricOutputMultiMapByTimeAndThresholdBuilder<S>
    {

        /**
         * Thread safe map.
         */

        final ConcurrentMap<MapKey<MetricConstants>, SafeMetricOutputMapByTimeAndThreshold.Builder<S>> internal =
                new ConcurrentSkipListMap<>();

        @Override
        public SafeMetricOutputMultiMapByTimeAndThreshold<S> build()
        {
            return new SafeMetricOutputMultiMapByTimeAndThreshold<>( this );
        }

        @Override
        public MetricOutputMultiMapByTimeAndThresholdBuilder<S>
                put( final TimeWindow timeWindow, final Threshold threshold, final MetricOutputMapByMetric<S> result )
        {
            if ( Objects.isNull( result ) )
            {
                throw new MetricOutputException( "Specify a non-null metric result." );
            }
            result.forEach( ( key, value ) -> {
                final MetricOutputMetadata d = value.getMetadata();
                final MapKey<MetricConstants> check =
                        dataFactory.getMapKey( d.getMetricID() );
                //Safe put
                final SafeMetricOutputMapByTimeAndThreshold.Builder<S> addMe =
                        new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
                addMe.put( Pair.of( timeWindow, threshold ), value );
                final SafeMetricOutputMapByTimeAndThreshold.Builder<S> checkMe = internal.putIfAbsent( check, addMe );
                //Add if already exists 
                if ( !Objects.isNull( checkMe ) )
                {
                    checkMe.put( Pair.of( timeWindow, threshold ), value );
                }
            } );
            return this;
        }

        @Override
        public MetricOutputMultiMapByTimeAndThresholdBuilder<S> put( MapKey<MetricConstants> key,
                                                                  MetricOutputMapByTimeAndThreshold<S> result )
        {
            if ( Objects.isNull( result ) )
            {
                throw new MetricOutputException( "Specify a non-null metric result." );
            }
            //Safe put
            final SafeMetricOutputMapByTimeAndThreshold.Builder<S> addMe =
                    new SafeMetricOutputMapByTimeAndThreshold.Builder<>();
            final SafeMetricOutputMapByTimeAndThreshold.Builder<S> checkMe = internal.putIfAbsent( key, addMe );
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
    private SafeMetricOutputMultiMapByTimeAndThreshold( final SafeMetricOutputMultiMapByTimeAndThresholdBuilder<S> builder )
    {
        //Bounds checks
        if ( builder.internal.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more <key,value> mappings to build the map." );
        }
        //Bounds checks
        builder.internal.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null key for the input map." );
            }
            if ( Objects.isNull( value ) )
            {
                throw new MetricOutputException( "Cannot prescribe a null value for the input map." );
            }
        } );
        //Initialize
        store = new TreeMap<>();
        //Build
        builder.internal.forEach( ( key, value ) -> store.put( key, value.build() ) );
    }

}