package wres.datamodel.metric;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import wres.datamodel.metric.SafeMetricOutputMapByLeadThreshold.Builder;
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
    public <T extends MetricOutput<?>> MetricOutputCollection<T> ofCollection(final List<T> input)
    {
        final MetricOutputCollection<T> returnMe = new UnsafeMetricOutputCollection<>();
        returnMe.addAll(input);
        return returnMe;
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> ofMap(final Map<MapBiKey<Integer, Threshold>, T> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input map.");
        final Builder<T> builder = new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach((key,value) -> builder.put(key, value));
        return builder.build();
    }
    
    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> combine(final List<MetricOutputMapByLeadThreshold<T>> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input map.");
        final Builder<T> builder = new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach(a-> a.forEach((key,value) -> builder.put(key, value)));
        builder.setOverrideMetadata(input.get(0).getMetadata());
        return builder.build();
    }    

    @Override
    public MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(final Integer leadTime, final Threshold threshold)
    {

        //Bounds checks
        Objects.requireNonNull(leadTime, "Specify a non-null lead time for the map key.");

        /**
         * Default implementation of a {@link MapBiKey}.
         */

        class MapKeyByLeadThreshold implements MapBiKey<Integer, Threshold>
        {

            @Override
            public int compareTo(final MapBiKey<Integer, Threshold> o)
            {
                //Compare on lead time, then threshold type, then threshold value, then upper threshold value, where 
                //it exists
                Objects.requireNonNull(o, "Specify a non-null map key for comparison.");
                if(!(o instanceof MapKeyByLeadThreshold))
                {
                    return -1;
                }
                final int returnMe = Integer.compare(leadTime, o.getFirstKey());
                if(returnMe != 0)
                {
                    return returnMe;
                }
                return o.getSecondKey().compareTo(getSecondKey());
            }

            @Override
            public Integer getFirstKey()
            {
                return leadTime;
            }

            @Override
            public Threshold getSecondKey()
            {
                return threshold;
            }

        }
        return new MapKeyByLeadThreshold();
    }

    @Override
    public MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(final Integer leadTime,
                                                                 final Double threshold,
                                                                 final Condition condition)
    {
        return getMapKeyByLeadThreshold(leadTime, threshold, null, condition);
    }

    @Override
    public MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(final Integer leadTime,
                                                                 final Double threshold,
                                                                 final Double thresholdUpper,
                                                                 final Condition condition)
    {
        return getMapKeyByLeadThreshold(leadTime, getThreshold(threshold, thresholdUpper, condition));
    }

    @Override
    public Threshold getThreshold(final Double threshold, final Condition condition)
    {
        return new ThresholdKey(threshold, null, condition);
    }

    @Override
    public Threshold getThreshold(final Double threshold, final Double thresholdUpper, final Condition condition)
    {
        return new ThresholdKey(threshold, thresholdUpper, condition);
    }

    @Override
    public Quantile getQuantile(final Double threshold, final Double probability, final Condition condition)
    {
        return getQuantile(threshold, null, probability, null, condition);
    }

    @Override
    public Quantile getQuantile(final Double threshold,
                                final Double thresholdUpper,
                                final Double probability,
                                final Double probabilityUpper,
                                final Condition condition)
    {
        return new QuantileKey(threshold, thresholdUpper, probability, probabilityUpper, condition);
    }

    /**
     * Hidden constructor.
     */

    private DefaultMetricOutputFactory()
    {
        inputFactory = DefaultMetricInputFactory.getInstance();
    }

}
