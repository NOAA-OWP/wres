package wres.engine.statistics.metric.outputs;

import gov.noaa.wres.datamodel.Dimension;
import wres.engine.statistics.metric.inputs.DoubleScalar;
import wres.engine.statistics.metric.inputs.IntegerScalar;

/**
 * A scalar outputs associated with a metric.
 * 
 * @author james.brown@hydrosolved.com
 */

public class ScalarOutput implements MetricOutput<DoubleScalar, IntegerScalar>
{

    /**
     * The output.
     */

    private final DoubleScalar output;

    /**
     * The dimension associated with the output or null for dimensionless output.
     */

    private final Dimension dim;

    /**
     * The sample size associated with the output.
     */

    private final IntegerScalar sampleSize;

    /**
     * Construct a dimensionless output with a sample size.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     */

    public ScalarOutput(final DoubleScalar output, final IntegerScalar sampleSize)
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

    public ScalarOutput(final DoubleScalar output, final IntegerScalar sampleSize, final Dimension dim)
    {
        this.output = output;
        this.sampleSize = sampleSize;
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
            && ((ScalarOutput)o).getData().valueOf() == output.valueOf()
            && ((ScalarOutput)o).sampleSize.valueOf() == sampleSize.valueOf()
            && (((ScalarOutput)o).dim == null) == (dim == null);
        return (dim != null) ? start && ((ScalarOutput)o).dim.equals(dim) : start;
    }

    @Override
    public int hashCode()
    {
        int returnMe = Double.hashCode(output.valueOf()) + Integer.hashCode(sampleSize.valueOf());
        if(dim != null)
        {
            returnMe = returnMe + dim.hashCode();
        }
        return returnMe;
    }

    @Override
    public IntegerScalar getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public DoubleScalar getData()
    {
        return output;
    }

}
