package wres.datamodel;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.ScoreOutput;

/**
 * An abstract base class for an immutable score output.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

abstract class SafeScoreOutput<T> implements ScoreOutput<T>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();

    /**
     * The output.
     */

    private final EnumMap<MetricConstants, T> output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeScoreOutput ) )
        {
            return false;
        }
        final SafeScoreOutput<?> v = (SafeScoreOutput<?>) o;
        boolean start = meta.equals( v.getMetadata() );
        if ( !start )
        {
            return false;
        }
        return output.equals( v.output );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output, meta );
    }

    @Override
    public T getData()
    {
        return output.get( MetricConstants.MAIN );
    }

    @Override
    public T getValue( MetricConstants component )
    {
        return output.get( component );
    }

    @Override
    public Iterator<Pair<MetricConstants, T>> iterator()
    {
        return new Iterator<Pair<MetricConstants, T>>()
        {
            private final Iterator<Entry<MetricConstants, T>> iterator = output.entrySet().iterator();

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
                throw new UnsupportedOperationException( "Cannot modify this immutable container of score outputs." );
            }
        };

    }

    @Override
    public Set<MetricConstants> getComponents()
    {
        return Collections.unmodifiableSet( output.keySet() );
    }

    @Override
    public boolean hasComponent( MetricConstants component )
    {
        return output.containsKey( component );
    }

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        output.forEach( ( key, value ) -> b.append( "[" )
                                           .append( key )
                                           .append( ", " )
                                           .append( value )
                                           .append( "]" )
                                           .append( NEWLINE ) );
        int lines = b.length();
        b.delete( lines - NEWLINE.length(), lines );
        return b.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeScoreOutput( final T output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        this.output = new EnumMap<>( MetricConstants.class );
        this.output.put( MetricConstants.MAIN, output );
        this.meta = meta;
    }

    /**
     * Construct the output with a map.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeScoreOutput( final Map<MetricConstants, T> output, final MetricOutputMetadata meta )
    {
        this.output = new EnumMap<>( MetricConstants.class );
        this.output.putAll( output );
        this.meta = meta;
        // Validate
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        output.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) || Objects.isNull( value ) )
            {
                throw new MetricOutputException( "Cannot build a score with null components." );
            }
        } );
    }

    /**
     * Construct the output with a template.
     * 
     * @param output the verification output
     * @param template the score template
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeScoreOutput( final T[] output, final ScoreOutputGroup template, final MetricOutputMetadata meta )
    {
        this.output = new EnumMap<>( MetricConstants.class );
        this.meta = meta;
        Set<MetricConstants> components = template.getMetricComponents();
        // Validate
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        if ( Objects.isNull( template ) )
        {
            throw new MetricOutputException( "Specify a non-null output group for the score output." );
        }
        //Check that the decomposition template is compatible
        if ( components.size() != output.length )
        {
            throw new MetricOutputException( "The specified output template '" + template
                                             + "' has more components than metric inputs provided ["
                                             + template.getMetricComponents().size()
                                             + ", "
                                             + output.length
                                             + "]." );
        }
        // Add the components
        Iterator<MetricConstants> iterator = components.iterator();
        int index = 0;
        while ( iterator.hasNext() )
        {
            this.output.put( iterator.next(), output[index] );
            index++;
        }
    }

}
