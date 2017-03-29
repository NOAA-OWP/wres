package wres.engine.statistics.metric.outputs;

import wres.engine.statistics.metric.inputs.Dimension;
import wres.engine.statistics.metric.inputs.DoubleScalar;

/**
 * A scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScalarOutput extends DoubleScalar implements MetricOutput
{

    /**
     * The dimension associated with the output or null for dimensionless output.
     */

    private final Dimension dim;

    /**
     * The sample size associated with the output.
     */

    private final int sampleSize;

    /**
     * Construct a dimensionless output with a sample size.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     */

    public ScalarOutput(final double output, final int sampleSize)
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

    public ScalarOutput(final double output, final int sampleSize, final Dimension dim)
    {
        super(output);
        this.dim = dim;
        this.sampleSize = sampleSize;
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
    public Integer getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public boolean equals(final Object o)
    {
        final boolean start = o != null && o instanceof ScalarOutput && ((ScalarOutput)o).valueOf() == valueOf()
            && ((ScalarOutput)o).sampleSize == sampleSize && (((ScalarOutput)o).dim == null) == (dim == null);
        return (dim != null) ? start && ((ScalarOutput)o).dim.equals(dim) : start;
    }

    @Override
    public int hashCode()
    {
        int returnMe = Double.hashCode(valueOf()) + Integer.hashCode(sampleSize);
        if(dim != null)
        {
            returnMe = returnMe + dim.hashCode();
        }
        return returnMe;
    }

}
