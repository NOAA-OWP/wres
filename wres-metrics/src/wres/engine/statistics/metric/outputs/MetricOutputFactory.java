package wres.engine.statistics.metric.outputs;

import java.util.List;

import wres.datamodel.DataFactory;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputMetadata;

/**
 * A factory class for producing metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricOutputFactory
{

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link ScalarOutput}
     */

    public static ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        return new ScalarOutput(output, meta);
    }

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link VectorOutput}
     */

    public static VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta)
    {
        final DataFactory dataFactory = DataFactory.instance();
        return new VectorOutput(dataFactory.vectorOf(output), meta);
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    public static MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta)
    {
        final DataFactory dataFactory = DataFactory.instance();
        return new MatrixOutput(dataFactory.matrixOf(output), meta);
    }

    /**
     * Return a {@link MatrixOutput} wrapped as a {@link MetricOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @param <T> the metric output
     * @return a {@link MatrixOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends MetricOutput<?>> T ofMatrixExtendsMetricOutput(final double[][] output,
                                                                            final MetricOutputMetadata meta)
    {
        return (T)ofMatrixOutput(output, meta);
    }

    /**
     * Return a {@link ScalarOutput} wrapped as a {@link MetricOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @param <T> the metric output
     * @return a {@link ScalarOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends MetricOutput<?>> T ofScalarExtendsMetricOutput(final double output,
                                                                            final MetricOutputMetadata meta)
    {
        return (T)ofScalarOutput(output, meta);
    }

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @param <T> the metric output
     * @return a {@link ScalarOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends ScalarOutput> T ofExtendsScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        return (T)ofScalarOutput(output, meta);
    }

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @param <T> the type of output
     * @return a {@link VectorOutput}
     */

    @SuppressWarnings("unchecked")
    public static <T extends VectorOutput> T ofExtendsVectorOutput(final double[] output,
                                                                   final MetricOutputMetadata meta)
    {
        return (T)ofVectorOutput(output, meta);
    }

    /**
     * Returns a collection of outputs from the prescribed list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a collection of metric outputs
     */

    public static <T extends MetricOutput<?>> MetricOutputCollection<T> ofCollection(final List<T> input)
    {
        final MetricOutputCollection<T> returnMe = new MetricOutputCollection<>();
        returnMe.addAll(input);
        return returnMe;
    }

    /**
     * Prevent construction.
     */

    private MetricOutputFactory()
    {

    }

}
