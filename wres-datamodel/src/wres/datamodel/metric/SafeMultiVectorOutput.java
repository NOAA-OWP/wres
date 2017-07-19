package wres.datamodel.metric;

import java.util.EnumMap;
import java.util.Objects;

import wres.datamodel.SafeVectorOfDoubles;
import wres.datamodel.VectorOfDoubles;

/**
 * An immutable mapping of {@link VectorOfDouble} to {@link MetricConstants}.
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

    private final EnumMap<MetricConstants, SafeVectorOfDoubles> output;

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
    public VectorOfDoubles get(MetricConstants identifier)
    {
        return output.get(identifier);
    }

    @Override
    public boolean containsKey(MetricConstants identifier)
    {
        return output.containsKey(identifier);
    }    
    
    @Override
    public EnumMap<MetricConstants, VectorOfDoubles> getData()
    {
        EnumMap<MetricConstants, VectorOfDoubles> copy = new EnumMap<>(MetricConstants.class);
        copy.putAll(output);
        return copy;
    }

    @Override
    public boolean equals(final Object o)
    {
        if(!(o instanceof SafeMultiVectorOutput))
        {
            return false;
        }
        final SafeMultiVectorOutput v = (SafeMultiVectorOutput)o;
        return meta.equals(v.getMetadata()) && output.equals(v.output);
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
     */

    SafeMultiVectorOutput(final EnumMap<MetricConstants, SafeVectorOfDoubles> output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output, "Specify a non-null output.");
        Objects.requireNonNull(meta, "Specify non-null metadata.");
        this.output = output.clone();
        this.meta = meta;
    }

}
