package wres.datamodel.metric;

import java.util.List;

/**
 * An abstract factory class for producing metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputFactory
{

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link ScalarOutput}
     */

    ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta);

    /**
     * Return a {@link VectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link VectorOutput}
     */

    VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta);

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta);

    /**
     * Returns a collection of outputs from the prescribed list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a collection of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputCollection<T> ofCollection(final List<T> input);

}

