package wres.datamodel.metric;

import java.util.List;
import java.util.Objects;

import wres.datamodel.DataFactory;

/**
 * A default factory class for producing metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class DefaultMetricOutputFactory implements MetricOutputFactory
{

    /**
     * Instance of the factory.
     */
    
    private static MetricOutputFactory instance = null;
    
    /**
     * Returns an instance of a {@link MetricOutputFactory}.
     * 
     * @return a {@link MetricOutputFactory}
     */
    
    public static MetricOutputFactory of() {
        if(Objects.isNull(instance)) {
            instance = new DefaultMetricOutputFactory();
        }
        return instance;
    }
    
    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link ScalarOutput}
     */
    @Override
    public ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        return new SafeScalarOutput(output, meta);
    }

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link VectorOutput}
     */
    @Override
    public VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta)
    {
        return new SafeVectorOutput(DataFactory.vectorOf(output), meta);
    }

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */
    @Override
    public MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta)
    {
        return new SafeMatrixOutput(DataFactory.matrixOf(output), meta);
    }

    /**
     * Returns a collection of outputs from the prescribed list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a collection of metric outputs
     */
    @Override
    public <T extends MetricOutput<?>> MetricOutputCollection<T> ofCollection(final List<T> input)
    {
        final MetricOutputCollection<T> returnMe = new UnsafeMetricOutputCollection<>();
        returnMe.addAll(input);
        return returnMe;
    }

    /**
     * Prevent construction.
     */

    private DefaultMetricOutputFactory()
    {
    }

}
