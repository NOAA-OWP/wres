package wres.datamodel.metric;

import java.util.List;
import java.util.Map;

import wres.datamodel.metric.Threshold.Condition;

/**
 * An abstract factory class for producing metric outputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputFactory extends MetricDataFactory
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
     * Returns a {@link MapBiKey} to map a {@link MetricOutput} by forecast lead time and {@link Threshold}.
     * 
     * @param leadTime the forecast lead time
     * @param threshold the threshold
     * @return a map key
     */

    MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(Integer leadTime, Threshold threshold);

    /**
     * Returns a {@link MapBiKey} to map a {@link MetricOutput} by forecast lead time and {@link Threshold}.
     * 
     * @param leadTime the forecast lead time
     * @param threshold the threshold value
     * @param condition the threshold condition
     * @return a map key
     */

    MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(Integer leadTime, Double threshold, Condition condition);

    /**
     * Returns a {@link MapBiKey} to map a {@link MetricOutput} by forecast lead time and {@link Threshold}.
     * 
     * @param leadTime the forecast lead time
     * @param threshold the threshold value or lower bound of a {@link Condition#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Condition#BETWEEN} or null
     * @param condition the threshold condition
     * @return a map key
     */

    MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(Integer leadTime,
                                                          Double threshold,
                                                          Double thresholdUpper,
                                                          Condition condition);

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound
     * @param condition the threshold condition
     * @return a threshold
     */

    Threshold getThreshold(final Double threshold, final Condition condition);

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound of a {@link Condition#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Condition#BETWEEN} or null
     * @param condition the threshold condition
     * @return a threshold
     */

    Threshold getThreshold(final Double threshold, final Double thresholdUpper, final Condition condition);

    /**
     * Returns a {@link Quantile} from the specified input
     * 
     * @param threshold the threshold value
     * @param probability the probability associated with the threshold
     * @param condition the threshold condition
     * @return a quantile
     */

    Quantile getQuantile(final Double threshold, Double probability, final Condition condition);

    /**
     * Returns a {@link Quantile} from the specified input
     * 
     * @param threshold the threshold value or lower bound of a {@link Condition#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Condition#BETWEEN} or null
     * @param probability the probability associated with the threshold
     * @param probabilityUpper the probability associated with the upper threshold or null
     * @param condition the threshold condition
     * @return a quantile
     */

    Quantile getQuantile(final Double threshold,
                         final Double thresholdUpper,
                         Double probability,
                         Double probabilityUpper,
                         final Condition condition);

    /**
     * Returns a collection of outputs from the raw list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a {@link MetricOutputCollection} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputCollection<T> ofCollection(final List<T> input);

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByLeadThreshold} of metric outputs
     */

    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> ofMap(final Map<MapBiKey<Integer, Threshold>, T> input);

    /**
     * Combines a list of {@link MetricOutputMapByLeadThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByLeadThreshold} of metric outputs
     */

    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> combine(final List<MetricOutputMapByLeadThreshold<T>> input);

}
