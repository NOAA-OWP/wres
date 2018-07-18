package wres.datamodel.outputs;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.DefaultMetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.ScoreOutput;

/**
 * An abstract base class for an immutable score output.
 * 
 * @author james.brown@hydrosolved.com
 */

abstract class SafeScoreOutput<T,U extends ScoreOutput<T,?>> implements ScoreOutput<T,U>
{

    /**
     * Line separator for printing.
     */

    private static final String NEWLINE = System.lineSeparator();
    
    /**
     * Null output message.
     */

    private static final String NULL_OUTPUT_MESSAGE = "Specify a non-null output.";
    
    /**
     * Null metadata message.
     */

    private static final String NULL_METADATA_MESSAGE = "Specify non-null metadata.";
        
    /**
     * The output.
     */

    private final EnumMap<MetricConstants, T> output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    /**
     * Returns a score from the specified input.
     * 
     * @param input the input
     * @param meta the score metadata
     * @return the score
     */
    
    abstract U getScoreOutput( T input, MetricOutputMetadata meta );    
    
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
        final SafeScoreOutput<?,?> v = (SafeScoreOutput<?,?>) o;
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
        if( this.hasComponent( MetricConstants.MAIN ) )
        {
            return output.get( MetricConstants.MAIN );
        }
        else if( output.size() == 1 )
        {
            return output.values().iterator().next();
        }
        return null;
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
    public U getComponent( MetricConstants component )
    {
        return getScoreOutput( output.get( component ),
                               DefaultMetadataFactory.getInstance().getOutputMetadata( meta, component ) );
    }        

    @Override
    public String toString()
    {
        StringBuilder b = new StringBuilder();
        output.forEach( ( key, value ) -> b.append( "(" )
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
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeScoreOutput( final T output, final MetricOutputMetadata meta )
    {
        // Allow a null score, but not null metadata
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( NULL_METADATA_MESSAGE );
        }
        
        this.output = new EnumMap<>( MetricConstants.class );
        if( Objects.nonNull( meta.getMetricComponentID() ) )
        {
            this.output.put( meta.getMetricComponentID(), output );
        }
        else
        {
            this.output.put( MetricConstants.MAIN, output );
        }
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
            throw new MetricOutputException( NULL_OUTPUT_MESSAGE );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( NULL_METADATA_MESSAGE );
        }
        // Allow a null score, but not a null identifier
        output.forEach( ( key, value ) -> {
            if ( Objects.isNull( key ) )
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
        
        // Validate
        if ( Objects.isNull( template ) )
        {
            throw new MetricOutputException( "Specify a non-null output group for the score output." );
        }
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( NULL_OUTPUT_MESSAGE );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( NULL_METADATA_MESSAGE );
        }
        // Check that the decomposition template is compatible
        Set<MetricConstants> components = template.getAllComponents();
        if ( components.size() != output.length )
        {
            throw new MetricOutputException( "The specified output template '" + template
                                             + "' has more components than metric inputs provided ["
                                             + template.getAllComponents().size()
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
