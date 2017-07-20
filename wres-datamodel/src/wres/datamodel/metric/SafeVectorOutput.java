package wres.datamodel.metric;

import java.util.Arrays;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;

/**
 * An immutable vector of outputs associated with a metric. The number of outputs, as well as the individual outputs and
 * the order in which they are stored, is prescribed by the metric from which the outputs originate.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
class SafeVectorOutput implements VectorOutput
{

    /**
     * The output.
     */

    private final VectorOfDoubles output;

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
    public VectorOfDoubles getData()
    {
        return output;
    }

    @Override
    public boolean equals(final Object o)
    {
        if(!(o instanceof SafeVectorOutput))
        {
            return false;
        }
        final SafeVectorOutput v = (SafeVectorOutput)o;
        return meta.equals(v.getMetadata()) && Arrays.equals(output.getDoubles(), v.getData().getDoubles());
    }

    @Override
    public int hashCode()
    {
        return getMetadata().hashCode() + Arrays.hashCode(output.getDoubles());
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param meta the metadata
     */

    SafeVectorOutput(final VectorOfDoubles output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output, "Specify a non-null output.");
        Objects.requireNonNull(meta, "Specify non-null metadata.");
        this.output = ((DefaultMetricInputFactory)DefaultMetricInputFactory.getInstance()).safeVectorOf(output);
        this.meta = meta;
    }

}
