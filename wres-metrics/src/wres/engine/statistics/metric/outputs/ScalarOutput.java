package wres.engine.statistics.metric.outputs;

import java.util.Objects;

import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * A scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class ScalarOutput implements MetricOutput<Double>
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
        boolean start = o instanceof ScalarOutput;
        if(start)
        {
            final ScalarOutput v = (ScalarOutput)o;
            start = meta.equals(v.getMetadata());
            start = start && FunctionFactory.doubleEquals().test(((ScalarOutput)o).getData(), output);
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

    protected ScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        Objects.requireNonNull(output,"Specify a non-null output.");
        Objects.requireNonNull(meta,"Specify non-null metadata.");        
        this.output = output;
        this.meta = meta;
    }

}
