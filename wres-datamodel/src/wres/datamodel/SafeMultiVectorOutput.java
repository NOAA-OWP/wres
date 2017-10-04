package wres.datamodel;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

import wres.datamodel.MetricConstants.MetricDimension;

/**
 * An immutable mapping of {@link VectorOfDouble} to a {@link MetricDimension}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMultiVectorOutput implements MultiVectorOutput
{

    /**
     * The output.
     */

    private final EnumMap<MetricDimension, VectorOfDoubles> output;

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
    public VectorOfDoubles get( MetricDimension identifier )
    {
        return output.get( identifier );
    }

    @Override
    public boolean containsKey( MetricDimension identifier )
    {
        return output.containsKey( identifier );
    }

    @Override
    public Map<MetricDimension, VectorOfDoubles> getData()
    {
        return new EnumMap<>( output );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeMultiVectorOutput ) )
        {
            return false;
        }
        final SafeMultiVectorOutput v = (SafeMultiVectorOutput) o;
        return meta.equals( v.getMetadata() ) && output.equals( v.output );
    }

    @Override
    public int hashCode()
    {
        return getMetadata().hashCode() + output.hashCode();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeMultiVectorOutput( final Map<MetricDimension, VectorOfDoubles> output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        this.output = new EnumMap<>( MetricDimension.class );
        DefaultDataFactory inFac = (DefaultDataFactory) DefaultDataFactory.getInstance();
        output.forEach( ( key, value ) -> this.output.put( key, inFac.safeVectorOf( value ) ) );
        this.meta = meta;
    }

}
