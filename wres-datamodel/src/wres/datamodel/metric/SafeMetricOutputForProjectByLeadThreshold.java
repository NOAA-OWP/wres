package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.SafeMetricOutputMultiMapByLeadThreshold.MetricOutputMultiMapByLeadThresholdBuilder;

/**
 * An immutable implementation of a high-level container of {@link MetricOutput} associated with a verification project.
 * The outputs are stored by lead time and threshold in a {@link MetricOutputMultiMapByLeadThreshold}. Retrieve the
 * outputs using the instance methods. If no outputs exist, the instance methods return null.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputForProjectByLeadThreshold implements MetricOutputForProjectByLeadThreshold
{

    /**
     * A {@link MetricOutputMultiMapByLeadThreshold} of {@link ScalarOutput}.
     */

    private final MetricOutputMultiMapByLeadThreshold<ScalarOutput> scalarOutput;

    /**
     * A {@link MetricOutputMultiMapByLeadThreshold} of {@link VectorOutput}.
     */

    private final MetricOutputMultiMapByLeadThreshold<VectorOutput> vectorOutput;

    /**
     * A {@link MetricOutputMultiMapByLeadThreshold} of {@link MultiVectorOutput}.
     */

    private final MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> multiVectorOutput;

    /**
     * A {@link MetricOutputMultiMapByLeadThreshold} of {@link MatrixOutput}.
     */

    private final MetricOutputMultiMapByLeadThreshold<MatrixOutput> matrixOutput;

    @Override
    public MetricOutputMultiMapByLeadThreshold<MetricOutput<?>> getOutput(MetricOutputGroup... outGroup)
    {
        Objects.requireNonNull(outGroup, "Specify one or more output types to return.");
        MetricOutputMultiMapByLeadThresholdBuilder<MetricOutput<?>> builder =
                                                                            new MetricOutputMultiMapByLeadThresholdBuilder<>();
        //Iterate through the types
        for(MetricOutputGroup next: outGroup)
        {
            if(hasOutput(next))
            {
                if(next == MetricOutputGroup.SCALAR)
                {
                    addToBuilder(builder, scalarOutput);
                }
                if(next == MetricOutputGroup.VECTOR)
                {
                    addToBuilder(builder, vectorOutput);
                }
                if(next == MetricOutputGroup.MULTIVECTOR)
                {
                    addToBuilder(builder, multiVectorOutput);
                }
                if(next == MetricOutputGroup.MATRIX)
                {
                    addToBuilder(builder, matrixOutput);
                }
            }
        }
        return builder.build();
    }

    @Override
    public MetricOutputGroup[] getOutputTypes()
    {
        List<MetricOutputGroup> returnMe = new ArrayList<>();
        if(hasScalarOutput())
        {
            returnMe.add(MetricOutputGroup.SCALAR);
        }
        if(hasVectorOutput())
        {
            returnMe.add(MetricOutputGroup.VECTOR);
        }
        if(hasMultiVectorOutput())
        {
            returnMe.add(MetricOutputGroup.MULTIVECTOR);
        }
        if(hasMatrixOutput())
        {
            returnMe.add(MetricOutputGroup.MATRIX);
        }
        return returnMe.toArray(new MetricOutputGroup[returnMe.size()]);
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<ScalarOutput> getScalarOutput()
    {
        return scalarOutput;
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<VectorOutput> getVectorOutput()
    {
        return vectorOutput;
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> getMultiVectorOutput()
    {
        return multiVectorOutput;
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<MatrixOutput> getMatrixOutput()
    {
        return matrixOutput;
    }

    /**
     * Builder.
     */

    static class MetricOutputForProjectByLeadThresholdBuilder implements Builder
    {

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<ScalarOutput>> scalarInternal =
                                                                                                                        new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<VectorOutput>> vectorInternal =
                                                                                                                        new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<MultiVectorOutput>> multiVectorInternal =
                                                                                                                                  new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<MatrixOutput>> matrixInternal =
                                                                                                                        new ConcurrentSkipListMap<>();

        @Override
        public Builder addScalarOutput(Integer leadTime,
                                       Threshold threshold,
                                       MetricOutputMapByMetric<ScalarOutput> result)
        {
            scalarInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public Builder addVectorOutput(Integer leadTime,
                                       Threshold threshold,
                                       MetricOutputMapByMetric<VectorOutput> result)
        {
            vectorInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public Builder addMultiVectorOutput(Integer leadTime,
                                            Threshold threshold,
                                            MetricOutputMapByMetric<MultiVectorOutput> result)
        {
            multiVectorInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public Builder addMatrixOutput(Integer leadTime,
                                       Threshold threshold,
                                       MetricOutputMapByMetric<MatrixOutput> result)
        {
            matrixInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public MetricOutputForProjectByLeadThreshold build()
        {
            return new SafeMetricOutputForProjectByLeadThreshold(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeMetricOutputForProjectByLeadThreshold(MetricOutputForProjectByLeadThresholdBuilder builder)
    {
        DataFactory d = DefaultDataFactory.getInstance();
        scalarOutput = d.ofMultiMap(builder.scalarInternal);
        vectorOutput = d.ofMultiMap(builder.vectorInternal);
        multiVectorOutput = d.ofMultiMap(builder.multiVectorInternal);
        matrixOutput = d.ofMultiMap(builder.matrixInternal);
    }

    /**
     * Helper that adds an existing output collection to an existing map.
     * 
     * @param map the map
     * @param addMe the metric output collection
     */

    private void addToBuilder(MetricOutputMultiMapByLeadThresholdBuilder<MetricOutput<?>> builder,
                              MetricOutputMultiMapByLeadThreshold<?> addMe)
    {
        DataFactory d = DefaultDataFactory.getInstance();
        addMe.forEach((key, value) -> {
            Map<MapBiKey<Integer, Threshold>, MetricOutput<?>> map = new TreeMap<>();
            value.forEach(map::put);
            builder.put(key, d.ofMap(map));
        });
    }

}
