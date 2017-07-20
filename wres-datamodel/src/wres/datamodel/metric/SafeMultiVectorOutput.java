package wres.datamodel.metric;

import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

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

    private final EnumMap<MetricConstants, VectorOfDoubles> output;

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
    public Map<MetricConstants, VectorOfDoubles> getData()
    {
        return new EnumMap<>(output);
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

    SafeMultiVectorOutput(final Map<MetricConstants, VectorOfDoubles> output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output, "Specify a non-null output.");
        Objects.requireNonNull(meta, "Specify non-null metadata.");
        this.output = new EnumMap<>(MetricConstants.class);
        DefaultMetricInputFactory inFac = (DefaultMetricInputFactory)DefaultMetricInputFactory.getInstance();
        output.forEach((key,value)->this.output.put(key,inFac.safeVectorOf(value)));
        this.meta = meta;
    }

}
