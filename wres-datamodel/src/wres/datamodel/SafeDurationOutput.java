package wres.datamodel;

import java.time.Duration;
import java.util.Objects;

import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DurationOutput;
import wres.datamodel.outputs.MetricOutputException;

/**
 * An immutable {@link Duration} output produced by a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */

class SafeDurationOutput implements DurationOutput
{

    /**
     * The output.
     */

    private final Duration output;

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
        if ( ! ( o instanceof SafeDurationOutput ) )
        {
            return false;
        }
        final SafeDurationOutput v = (SafeDurationOutput) o;
        boolean start = meta.equals( v.getMetadata() );
        start = start && ( (SafeDurationOutput) o ).getData().equals( output );
        return start;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( output, meta );
    }

    @Override
    public Duration getData()
    {
        return output;
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     * @throws MetricOutputException if any of the inputs are invalid
     */

    SafeDurationOutput( final Duration output, final MetricOutputMetadata meta )
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
