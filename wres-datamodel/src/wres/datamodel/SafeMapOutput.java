package wres.datamodel;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MapOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * A read-only map of outputs. The keys and values may or may not be immutable, but the map is read only.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

class SafeMapOutput<S,T> implements MapOutput<S,T>
{

    /**
     * The output.
     */

    private final Map<S,T> output;

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
        if ( ! ( o instanceof SafeMapOutput ) )
        {
            return false;
        }
        final SafeMapOutput<?,?> v = (SafeMapOutput<?,?>) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && v.output.equals( output );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output , meta );
    }

    @Override
    public Map<S,T> getData()
    {
        return output;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( ",", "[", "]" );
        output.forEach( ( key, value ) -> joiner.add( value.toString() ) );
        return joiner.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeMapOutput( final Map<S,T> output, final MetricOutputMetadata meta )
    {        
        //Validate
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        //Set content
        this.output = Collections.unmodifiableMap( output );     
        this.meta = meta;

        //Validate content
        output.forEach( ( key, value ) -> {
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
