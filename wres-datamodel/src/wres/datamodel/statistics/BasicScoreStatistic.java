package wres.datamodel.statistics;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;

/**
 * An abstract base class for an immutable score output.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class BasicScoreStatistic<T,U extends ScoreStatistic<T,?>> implements ScoreStatistic<T,U>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();
    
    /**
     * Null output message.
     */

    private static final String NULL_OUTPUT_MESSAGE = "Specify a non-null statistic.";
    
    /**
     * Null metadata message.
     */

    private static final String NULL_METADATA_MESSAGE = "Specify non-null metadata for the statistic.";
        
    /**
     * The statistic.
     */

    private final EnumMap<MetricConstants, T> statistic;

    /**
     * The metadata associated with the statistic.
     */

    private final StatisticMetadata meta;

    /**
     * Returns a score from the specified input.
     * 
     * @param input the input
     * @param meta the score metadata
     * @return the score
     */
    
    abstract U getScore( T input, StatisticMetadata meta );    
    
    @Override
    public StatisticMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof BasicScoreStatistic ) )
        {
            return false;
        }
        final BasicScoreStatistic<?,?> v = (BasicScoreStatistic<?,?>) o;
        boolean start = meta.equals( v.getMetadata() );
        if ( !start )
        {
            return false;
        }
        return statistic.equals( v.statistic );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( statistic, meta );
    }

    @Override
    public T getData()
    {
        if( this.hasComponent( MetricConstants.MAIN ) )
        {
            return statistic.get( MetricConstants.MAIN );
        }
        else if( statistic.size() == 1 )
        {
            return statistic.values().iterator().next();
        }
        return null;
    }

    @Override
    public Iterator<Pair<MetricConstants, T>> iterator()
    {
        return new Iterator<Pair<MetricConstants, T>>()
        {
            private final Iterator<Entry<MetricConstants, T>> iterator = statistic.entrySet().iterator();

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public Pair<MetricConstants, T> next()
            {
                Entry<MetricConstants, T> next = iterator.next();
                return Pair.of( next.getKey(), next.getValue() );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException( "Cannot modify this immutable container of score statistics." );
            }
        };

    }

    @Override
    public Set<MetricConstants> getComponents()
    {
        return Collections.unmodifiableSet( statistic.keySet() );
    }

    @Override
    public boolean hasComponent( MetricConstants component )
    {
        return statistic.containsKey( component );
    }
    
    @Override
    public U getComponent( MetricConstants component )
    {
        return getScore( statistic.get( component ),
                               StatisticMetadata.of( meta, meta.getMetricID(), component ) );
    }        

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        statistic.forEach( ( key, value ) -> b.append( "(" )
                                           .append( key )
                                           .append( "," )
                                           .append( value )
                                           .append( ")" )
                                           .append( NEWLINE ) );
        int lines = b.length();
        b.delete( lines - NEWLINE.length(), lines );
        return b.toString();
    }

    /**
     * Construct the output.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    BasicScoreStatistic( final T statistic, final StatisticMetadata meta )
    {
        // Allow a null score, but not null metadata
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( NULL_METADATA_MESSAGE );
        }
        
        this.statistic = new EnumMap<>( MetricConstants.class );
        if( Objects.nonNull( meta.getMetricComponentID() ) )
        {
            this.statistic.put( meta.getMetricComponentID(), statistic );
        }
        else
        {
            this.statistic.put( MetricConstants.MAIN, statistic );
        }
        this.meta = meta;
    }

    /**
     * Construct the statistic with a map.
     * 
     * @param statistic the verification statistic
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    BasicScoreStatistic( final Map<MetricConstants, T> statistic, final StatisticMetadata meta )
    {
        this.statistic = new EnumMap<>( MetricConstants.class );
        this.statistic.putAll( statistic );
        this.meta = meta;
        
        // Validate
        if ( Objects.isNull( statistic ) )
        {
            throw new StatisticException( NULL_OUTPUT_MESSAGE );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( NULL_METADATA_MESSAGE );
        }
        // Allow a null score, but not a null identifier
        statistic.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) )
            {
                throw new StatisticException( "Cannot build a score with null components." );
            }
        } );
    }

    /**
     * Construct the statistic with a template.
     * 
     * @param statistic the verification statistic
     * @param template the score template
     * @param meta the metadata
     * @throws StatisticException if any of the inputs are invalid
     */

    BasicScoreStatistic( final T[] statistic, final ScoreGroup template, final StatisticMetadata meta )
    {
        this.statistic = new EnumMap<>( MetricConstants.class );
        this.meta = meta;
        
        // Validate
        if ( Objects.isNull( template ) )
        {
            throw new StatisticException( "Specify a non-null output group for the score output." );
        }
        if ( Objects.isNull( statistic ) )
        {
            throw new StatisticException( NULL_OUTPUT_MESSAGE );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new StatisticException( NULL_METADATA_MESSAGE );
        }
        // Check that the decomposition template is compatible
        Set<MetricConstants> components = template.getAllComponents();
        if ( components.size() != statistic.length )
        {
            throw new StatisticException( "The specified output template '" + template
                                             + "' has more components than metric inputs provided ["
                                             + template.getAllComponents().size()
                                             + ", "
                                             + statistic.length
                                             + "]." );
        }
        // Add the components
        Iterator<MetricConstants> iterator = components.iterator();
        int index = 0;
        while ( iterator.hasNext() )
        {
            this.statistic.put( iterator.next(), statistic[index] );
            index++;
        }
    }

}
