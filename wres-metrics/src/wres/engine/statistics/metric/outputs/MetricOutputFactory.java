package wres.engine.statistics.metric.outputs;

import wres.datamodel.DataFactory;
import wres.datamodel.metric.Dimension;
import wres.datamodel.metric.MetricOutput;

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

    public static ScalarOutput ofScalarOutput(final double output, final int sampleSize, final Dimension d)
    {
        return new ScalarOutput(output, sampleSize, d);
    }

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @return a {@link VectorOutput}
     */

    public static VectorOutput ofVectorOutput(final double[] output, final int sampleSize, final Dimension d)
    {
        final DataFactory dataFactory = DataFactory.instance();
        return new VectorOutput(dataFactory.vectorOf(output), sampleSize, d);
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @return a {@link MatrixOutput}
     */

    public static MatrixOutput ofMatrixOutput(final double[][] output, final int sampleSize, final Dimension d)
    {
        final DataFactory dataFactory = DataFactory.instance();
        return new MatrixOutput(dataFactory.matrixOf(output), sampleSize, d);
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
    public static <T extends MetricOutput<?>> T ofMatrixExtendsMetricOutput(final double[][] output,
                                                                            final int sampleSize,
                                                                            final Dimension d)
    {
        return (T)ofMatrixOutput(output, sampleSize, d);
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
    public static <T extends MetricOutput<?>> T ofVectorExtendsMetricOutput(final double[] output,
                                                                            final int sampleSize,
                                                                            final Dimension d)
    {
        return (T)ofVectorOutput(output, sampleSize, d);
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
    public static <T extends MetricOutput<?>> T ofScalarExtendsMetricOutput(final double output,
                                                                            final int sampleSize,
                                                                            final Dimension d)
    {
        return (T)ofScalarOutput(output, sampleSize, d);
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
    public static <T extends ScalarOutput> T ofExtendsScalarOutput(final double output,
                                                                   final int sampleSize,
                                                                   final Dimension d)
    {
        return (T)ofScalarOutput(output, sampleSize, d);
    }

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param sampleSize the sample size
     * @param d the dimension
     * @param <T> the type of output
     * @return a {@link VectorOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends VectorOutput> T ofExtendsVectorOutput(final double[] output,
                                                                   final int sampleSize,
                                                                   final Dimension d)
    {
        return (T)ofVectorOutput(output, sampleSize, d);
    }

    /**
     * Prevent construction.
     */

    private MetricOutputFactory()
    {

    }

}
