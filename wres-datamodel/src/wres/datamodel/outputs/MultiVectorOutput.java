package wres.datamodel.outputs;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.DataFactory;
import wres.datamodel.VectorOfDoubles;

/**
 * One or more vectors that are explicitly mapped to elements in {@link MetricDimension}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MultiVectorOutput implements MetricOutput<Map<MetricDimension, VectorOfDoubles>>
{
    /**
     * The output.
     */

    private final EnumMap<MetricDimension, VectorOfDoubles> output;

    /**
     * The metadata associated with the output.
     */

    private final MetricOutputMetadata meta;

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     * @return an instance of the output
     */

    public static MultiVectorOutput of( final Map<MetricDimension, VectorOfDoubles> output,
                                        final MetricOutputMetadata meta )
    {
        return new MultiVectorOutput( output, meta );
    }

    /**
     * Returns a prescribed vector from the map or null if no mapping exists.
     * 
     * @param identifier the identifier
     * @return a vector or null
     */

    public VectorOfDoubles get( MetricDimension identifier )
    {
        return output.get( identifier );
    }

    /**
     * Returns true if the store contains a mapping for the prescribed identifier, false otherwise.
     * 
     * @param identifier the identifier
     * @return true if the mapping exists, false otherwise
     */

    public boolean containsKey( MetricDimension identifier )
    {
        return output.containsKey( identifier );
    }

    @Override
    public MetricOutputMetadata getMetadata()
    {
        return meta;
    }

    @Override
    public Map<MetricDimension, VectorOfDoubles> getData()
    {
        return new EnumMap<>( output );
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof MultiVectorOutput ) )
        {
            return false;
        }
        final MultiVectorOutput v = (MultiVectorOutput) o;
        return meta.equals( v.getMetadata() ) && output.equals( v.output );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, output );
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner( System.lineSeparator() );
        output.forEach( ( key, value ) -> joiner.add( key + ": " + value ) );
        return joiner.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    private MultiVectorOutput( final Map<MetricDimension, VectorOfDoubles> output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        if ( output.isEmpty() )
        {
            throw new MetricOutputException( "Specify one or more outputs to store." );
        }
        this.output = new EnumMap<>( MetricDimension.class );
        output.forEach( ( key, value ) -> this.output.put( key, DataFactory.safeVectorOf( value ) ) );
        this.meta = meta;
    }

}
