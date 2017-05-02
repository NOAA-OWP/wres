package wres.engine.statistics.metric.outputs;

import gov.noaa.wres.datamodel.Dimension;
import wres.engine.statistics.metric.inputs.DoubleMatrix;
import wres.engine.statistics.metric.inputs.IntegerScalar;

/**
 * <p>
 * A matrix of outputs associated with a metric. The number of elements and the order in which they are stored, is
 * prescribed by the metric from which the outputs originate.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class MatrixOutput implements MetricOutput<DoubleMatrix, IntegerScalar>
{

    /**
     * The output data.
     */

    private final DoubleMatrix output;

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

    public MatrixOutput(final DoubleMatrix output, final IntegerScalar sampleSize)
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

    public MatrixOutput(final DoubleMatrix output, final IntegerScalar sampleSize, final Dimension dim)
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
    public IntegerScalar getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public DoubleMatrix getData()
    {
        return output;
    }

}
