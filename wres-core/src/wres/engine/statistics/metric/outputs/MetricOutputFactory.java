package wres.engine.statistics.metric.outputs;

import gov.noaa.wres.datamodel.Dimension;
import wres.engine.statistics.metric.inputs.DoubleMatrix;
import wres.engine.statistics.metric.inputs.DoubleScalar;
import wres.engine.statistics.metric.inputs.DoubleVector;
import wres.engine.statistics.metric.inputs.IntegerScalar;

/**
 * A factory class for producing metric outputs. TODO: revisit the suppressed warnings.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricOutputFactory
{

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @return a {@link ScalarOutput}
     */

    public static ScalarOutput getScalarOutput(final DoubleScalar output,
                                               final IntegerScalar sampleSize,
                                               final Dimension d)
    {
        return new ScalarOutput(output, sampleSize, d);
    }

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @return a {@link ScalarOutput}
     */

    public static ScalarOutput getScalarOutput(final double output, final int sampleSize, final Dimension d)
    {
        return getScalarOutput(new DoubleScalar(output), new IntegerScalar(sampleSize), d);
    }

    /**
     * Return a {@link MatrixOutput} wrapped as a {@link MetricOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @param <T> the metric output
     * @return a {@link MatrixOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends MetricOutput<?, ?>> T getMatrixExtendsMetricOutput(final double[][] output,
                                                                                final int sampleSize,
                                                                                final Dimension d)
    {
        return (T)new MatrixOutput(new DoubleMatrix(output), new IntegerScalar(sampleSize));
    }

    /**
     * Return a {@link VectorOutput} wrapped as a {@link MetricOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @param <T> the metric output
     * @return a {@link VectorOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends MetricOutput<?, ?>> T getVectorExtendsMetricOutput(final double[] output,
                                                                                final int sampleSize,
                                                                                final Dimension d)
    {
        return (T)new VectorOutput(new DoubleVector(output), new IntegerScalar(sampleSize));
    }

    /**
     * Return a {@link ScalarOutput} wrapped as a {@link MetricOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @param <T> the metric output
     * @return a {@link ScalarOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends MetricOutput<?, ?>> T getScalarExtendsMetricOutput(final double output,
                                                                                final int sampleSize,
                                                                                final Dimension d)
    {
        return (T)getScalarOutput(output, sampleSize, d);
    }

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @param <T> the metric output
     * @return a {@link ScalarOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends ScalarOutput> T getExtendsScalarOutput(final double output,
                                                                    final int sampleSize,
                                                                    final Dimension d)
    {
        return (T)getScalarOutput(new DoubleScalar(output), new IntegerScalar(sampleSize), d);
    }

    /**
     * Prevent construction.
     */

    private MetricOutputFactory()
    {

    }

}
