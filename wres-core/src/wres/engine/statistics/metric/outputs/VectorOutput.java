package wres.engine.statistics.metric.outputs;

import wres.engine.statistics.metric.inputs.Dimension;
import wres.engine.statistics.metric.inputs.DoubleVector;

/**
 * <p>
 * A vector of outputs associated with a metric. The number of outputs, as well as the individual outputs and the order
 * in which they are stored, is prescribed by the metric from which the outputs originate.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class VectorOutput extends DoubleVector implements MetricOutput
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

    public VectorOutput(final double[] output, final int sampleSize)
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

    public VectorOutput(final double[] output, final int sampleSize, final Dimension dim)
    {
        super(output); //Bounds check in super
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
    public Integer getSampleSize()
    {
        return sampleSize;
    }

}
