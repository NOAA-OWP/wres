package wres.datamodel.metric;

import java.util.Objects;

/**
 * An immutable scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

final class SafeScalarOutput implements ScalarOutput
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
    public boolean equals(final Object o)
    {
        boolean start = o instanceof SafeScalarOutput;
        if(start)
        {
            final SafeScalarOutput v = (SafeScalarOutput)o;
            start = meta.equals(v.getMetadata());
            start = start && Math.abs(((SafeScalarOutput)o).getData() - output) < .00000001;
        }
        return start;        
    }

    @Override
    public int hashCode()
    {
        return Double.hashCode(output) + meta.hashCode();
    }

    @Override
    public Double getData()
    {
        return output;
    }

    @Override
    public String toString()
    {
        return Double.toString(output);
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     */

    SafeScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output,"Specify a non-null output.");
        Objects.requireNonNull(meta,"Specify non-null metadata.");        
        this.output = output;
        this.meta = meta;
    }

}
