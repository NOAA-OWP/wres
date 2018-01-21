package wres.datamodel;

import java.util.Objects;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeScalarOutput implements DoubleScoreOutput
{

    /**
     * The output.
     */

    private final double output;

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
        if ( ! ( o instanceof SafeScalarOutput ) )
        {
            return false;
        }
        final SafeScalarOutput v = (SafeScalarOutput) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && Math.abs( ( (SafeScalarOutput) o ).getData() - output ) < .00000001;
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output , meta );
    }

    @Override
    public Double getData()
    {
        return output;
    }

    @Override
    public String toString()
    {
        return Double.toString( output );
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeScalarOutput( final double output, final MetricOutputMetadata meta )
    {
        if ( Objects.isNull( output ) )
        {
            throw new MetricOutputException( "Specify a non-null output." );
        }
        if ( Objects.isNull( meta ) )
        {
            throw new MetricOutputException( "Specify non-null metadata." );
        }
        this.output = output;
        this.meta = meta;
    }

}
