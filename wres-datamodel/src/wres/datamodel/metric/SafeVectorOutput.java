package wres.datamodel.metric;

import java.util.Arrays;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;

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

    /**
     * The template associated with the outputs.
     */

    private MetricDecompositionGroup template;

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
    public MetricDecompositionGroup getOutputTemplate()
    {
        return template;
    }

    @Override
    public boolean equals(final Object o)
    {
        if(!(o instanceof SafeVectorOutput))
        {
            return false;
        }
        final SafeVectorOutput v = (SafeVectorOutput)o;
        return meta.equals(v.getMetadata()) && template.equals(v.template)
            && Arrays.equals(output.getDoubles(), v.getData().getDoubles());
    }

    @Override
    public int hashCode()
    {
        return getMetadata().hashCode() + getOutputTemplate().hashCode() + Arrays.hashCode(output.getDoubles());
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output
     * @param template the output template
     * @param meta the metadata
     */

    SafeVectorOutput(final VectorOfDoubles output,
                     final MetricDecompositionGroup template,
                     final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output, "Specify a non-null output.");
        Objects.requireNonNull(meta, "Specify non-null metadata.");
        Objects.requireNonNull(template, "Specify a non-null decomposition template for the vector output.");
        //Check that the decomposition template is compatible
        if(template.getMetricComponents().size() != output.size())
        {
            throw new IllegalArgumentException("The specified output template '" + template
                + "' has more components than metric inputs provided [" + template.getMetricComponents().size() + ", "
                + output.size() + "].");
        }
        this.output = ((DefaultDataFactory)DefaultDataFactory.getInstance()).safeVectorOf(output);
        this.meta = meta;
        this.template = template;
    }

}
