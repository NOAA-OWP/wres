package wres.engine.statistics.metric.outputs;

import java.util.Objects;

import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricOutput;
import wres.engine.statistics.metric.FunctionFactory;

/**
 * A scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScalarOutput implements MetricOutput<Double>
{

    /**
     * The output.
     */

    private final double output;

    /**
     * The dimension associated with the output or null for dimensionless output.
     */

    private final Dimension dim;

    /**
     * The sample size associated with the output.
     */

    private final int sampleSize;

    @Override
    public Dimension getDimension()
    {
        return dim;
    }

    @Override
    public boolean isDimensionless()
    {
        return dim == null;
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean start = o instanceof ScalarOutput && !Objects.isNull(o);
        start = start && FunctionFactory.doubleEquals().test(((ScalarOutput)o).getData(), output);
        start = start && ((ScalarOutput)o).sampleSize == sampleSize;
        start = start && (Objects.isNull(((ScalarOutput)o).dim) == Objects.isNull(dim));
        return (dim != null) ? start && ((ScalarOutput)o).dim.equals(dim) : start;
    }

    @Override
    public int hashCode()
    {
        int returnMe = Double.hashCode(output) + Integer.hashCode(sampleSize);
        if(dim != null)
        {
            returnMe = returnMe + dim.hashCode();
        }
        return returnMe;
    }

    @Override
    public int getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public Double getData()
    {
        return output;
    }

    /**
     * Construct a dimensionless output with a sample size.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     */

    protected ScalarOutput(final double output, final int sampleSize)
    {
        this(output, sampleSize, null);
    }

    /**
     * Construct the output.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     * @param dim the dimension.
     */

    protected ScalarOutput(final double output, final int sampleSize, final Dimension dim)
    {
        this.output = output;
        this.sampleSize = sampleSize;
        this.dim = dim;
    }

}
