package wres.datamodel.metric;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import wres.datamodel.metric.Threshold.Condition;

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
     * Instance of an input factory.
     */

    private final MetricInputFactory inputFactory;

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
        return new SafeVectorOutput(inputFactory.vectorOf(output), meta);
    }

    @Override
    public MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta)
    {
        return new SafeMatrixOutput(inputFactory.matrixOf(output), meta);
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMap(final List<T> input)
    {
        Objects.requireNonNull(input, "Specify a non-null list of inputs.");
        final SafeMetricOutputMapByMetric.Builder<T> builder = new SafeMetricOutputMapByMetric.Builder<>();
        input.forEach(a -> {
            final MapBiKey<MetricConstants, MetricConstants> key = getMapKey(a.getMetadata().getMetricID(),
                                                                             a.getMetadata().getMetricComponentID());
            builder.put(key, a);
        });
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> ofMap(final Map<MapBiKey<Integer, Threshold>, T> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input map.");
        final SafeMetricOutputMapByLeadThreshold.Builder<T> builder =
                                                                    new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach(builder::put);
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> combine(final List<MetricOutputMapByLeadThreshold<T>> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input map.");
        final SafeMetricOutputMapByLeadThreshold.Builder<T> builder =
                                                                    new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach(a -> a.forEach(builder::put));
        builder.setOverrideMetadata(input.get(0).getMetadata());
        return builder.build();
    }

    @Override
    public <S extends Comparable<S>, T extends Comparable<T>> MapBiKey<S, T> getMapKey(final S firstKey,
                                                                                       final T secondKey)
    {

        //Bounds checks
        Objects.requireNonNull(firstKey, "Specify a non-null first key.");
        Objects.requireNonNull(secondKey, "Specify a non-null second key.");

        /**
         * Default implementation of a {@link MapBiKey}.
         */

        class MapKey implements MapBiKey<S, T>
        {

            @Override
            public int compareTo(final MapBiKey<S, T> o)
            {
                //Compare on lead time, then threshold type, then threshold value, then upper threshold value, where 
                //it exists
                Objects.requireNonNull(o, "Specify a non-null map key for comparison.");
                if(!(o instanceof MapKey))
                {
                    return -1;
                }
                final int returnMe = firstKey.compareTo(o.getFirstKey());
                if(returnMe != 0)
                {
                    return returnMe;
                }
                return o.getSecondKey().compareTo(getSecondKey());
            }

            @Override
            public S getFirstKey()
            {
                return firstKey;
            }

            @Override
            public T getSecondKey()
            {
                return secondKey;
            }

        }
        return new MapKey();
    }

    @Override
    public Threshold getThreshold(final Double threshold, final Double thresholdUpper, final Condition condition)
    {
        return new SafeThresholdKey(threshold, thresholdUpper, condition);
    }

    @Override
    public Quantile getQuantile(final Double threshold,
                                final Double thresholdUpper,
                                final Double probability,
                                final Double probabilityUpper,
                                final Condition condition)
    {
        return new SafeQuantileKey(threshold, thresholdUpper, probability, probabilityUpper, condition);
    }

    @Override
    public <S extends MetricOutput<?>> MetricOutputMultiMap.Builder<S> ofMultiMap()
    {
        return new SafeMetricOutputMultiMap.MultiMapBuilder<>();
    }

    /**
     * Hidden constructor.
     */

    private DefaultMetricOutputFactory()
    {
        inputFactory = DefaultMetricInputFactory.getInstance();
    }

}
