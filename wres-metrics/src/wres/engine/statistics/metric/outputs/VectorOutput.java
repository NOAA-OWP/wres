package wres.engine.statistics.metric.outputs;

import java.util.Arrays;
import java.util.Objects;

import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricOutput;

/**
 * <p>
 * A vector of outputs associated with a metric. The number of outputs, as well as the individual outputs and the order
 * in which they are stored, is prescribed by the metric from which the outputs originate.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class VectorOutput implements MetricOutput<VectorOfDoubles>
{

    /**
     * The output.
     */

    private final VectorOfDoubles output;

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
    public int getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public VectorOfDoubles getData()
    {
        return output;
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean start = o instanceof VectorOutput && !Objects.isNull(o);
        final VectorOutput v = (VectorOutput)o;
        start = start && v.isDimensionless() == isDimensionless();
        if(!isDimensionless())
        {
            start = start && getDimension().equals(v.getDimension());
        }
        start = start && v.getSampleSize() == getSampleSize();
        return start && Arrays.equals(output.getDoubles(), v.getData().getDoubles());
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(output.getDoubles());
    }

    /**
     * Construct a dimensionless output with a sample size.
     * 
     * @param output the verification output.
     * @param sampleSize the sample size
     */

    protected VectorOutput(final VectorOfDoubles output, final int sampleSize)
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

    protected VectorOutput(final VectorOfDoubles output, final int sampleSize, final Dimension dim)
    {
        this.output = output;
        this.sampleSize = sampleSize;
        this.dim = dim;
    }

}
