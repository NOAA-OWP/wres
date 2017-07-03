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

public class DefaultMetricOutputFactory extends DefaultMetricDataFactory implements MetricOutputFactory
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

    public static MetricOutputFactory getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultMetricOutputFactory();
        }
        return instance;
    }

    @Override
    public ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        return new SafeScalarOutput(output, meta);
    }

    @Override
    public VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta)
    {
        return new SafeVectorOutput(DataFactory.vectorOf(output), meta);
    }

    @Override
    public MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta)
    {
        return new SafeMatrixOutput(DataFactory.matrixOf(output), meta);
    }

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
