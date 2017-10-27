package wres.datamodel;

import java.util.Arrays;
import java.util.Objects;

/**
 * An immutable matrix of outputs associated with a metric. The number of elements and the order in which they are
 * stored, is prescribed by the metric from which the outputs originate.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeMatrixOutput implements MatrixOutput
{

    /**
     * The output data.
     */

    private final MatrixOfDoubles output;

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
    public MatrixOfDoubles getData()
    {
        return output;
    }

    @Override
    public boolean equals( final Object o )
    {
        if ( ! ( o instanceof SafeMatrixOutput ) )
        {
            return false;
        }
        final SafeMatrixOutput m = (SafeMatrixOutput) o;
        boolean start = meta.equals( m.getMetadata() );
        start = start && m.getData().rows() == output.rows() && m.getData().columns() == output.columns();
        start = start && Arrays.deepEquals( output.getDoubles(), m.getData().getDoubles() );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( meta, Arrays.deepHashCode( output.getDoubles() ) );
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param meta the metadata.
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeMatrixOutput( final MatrixOfDoubles output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        this.output = ( (DefaultDataFactory) DefaultDataFactory.getInstance() ).safeMatrixOf( output );
        this.meta = meta;
    }

}
