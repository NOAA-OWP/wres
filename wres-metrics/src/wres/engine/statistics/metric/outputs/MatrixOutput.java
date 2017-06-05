package wres.engine.statistics.metric.outputs;

import java.util.Arrays;
import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricOutput;

/**
 * <p>
 * A matrix of outputs associated with a metric. The number of elements and the order in which they are stored, is
 * prescribed by the metric from which the outputs originate.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class MatrixOutput implements MetricOutput<MatrixOfDoubles>
{

    /**
     * The output data.
     */

    private final MatrixOfDoubles output;

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

    public MatrixOutput(final MatrixOfDoubles output, final int sampleSize)
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

    public MatrixOutput(final MatrixOfDoubles output, final int sampleSize, final Dimension dim)
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
    public int getSampleSize()
    {
        return sampleSize;
    }

    @Override
    public MatrixOfDoubles getData()
    {
        return output;
    }

    @Override
    public boolean equals(final Object o)
    {
        boolean start = o instanceof MatrixOutput && !Objects.isNull(o);
        final MatrixOutput m = (MatrixOutput)o;
        start = start && m.isDimensionless() == isDimensionless();
        if(!isDimensionless())
        {
            start = start && getDimension().equals(m.getDimension());
        }
        start = start && m.getSampleSize() == getSampleSize();
        start = start && m.output.rows() == output.rows() && m.output.columns() == output.columns();
        return start && Arrays.deepEquals(output.getDoubles(), m.getData().getDoubles());
    }

    @Override
    public int hashCode()
    {
        int code = 0;
        if(!isDimensionless())
        {
            code += getDimension().hashCode();
        }
        code += Integer.valueOf(sampleSize).hashCode();
        return code += Arrays.deepHashCode(output.getDoubles());
    }

}
