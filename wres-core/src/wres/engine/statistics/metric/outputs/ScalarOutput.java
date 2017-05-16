package wres.engine.statistics.metric.outputs;

import wres.engine.statistics.metric.inputs.Dimension;
import wres.engine.statistics.metric.inputs.Sample;

/**
 * A scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScalarOutput implements MetricOutput<Double, Sample>, Sample
{

    /**
     * The output.
     */

    private final Double output;

    /**
     * The dimension associated with the output or null for dimensionless output.
     */

    private final Dimension dim;

    /**
     * The sample size associated with the output.
     */

    private final Integer sampleSize;

    /**
     * Construct a dimensionless output with a sample size.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     */

    public ScalarOutput(final Double output, final Integer sampleSize)
    {
        this(output, sampleSize, null);
    }

    /**
     * Construct the output.
     * 
     * @param output2 the verification output.
     * @param sampleSize2 the sample size
     * @param dim the dimension.
     */

    public ScalarOutput(final Double output2, final Integer sampleSize2, final Dimension dim)
    {
        this.output = output2;
        this.sampleSize = sampleSize2;
        this.dim = dim;
    }

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
        final boolean start = o != null && o instanceof ScalarOutput
            && ((ScalarOutput)o).getData().doubleValue() == output.doubleValue()
            && ((ScalarOutput)o).sampleSize.intValue() == sampleSize.intValue()
            && (((ScalarOutput)o).dim == null) == (dim == null);
        return (dim != null) ? start && ((ScalarOutput)o).dim.equals(dim) : start;
    }

    @Override
    public int hashCode()
    {
        int returnMe = Double.hashCode(output.doubleValue()) + Integer.hashCode(sampleSize.intValue());
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

}
