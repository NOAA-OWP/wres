package wres.datamodel.metric;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * An immutable implementation of a high-level container of {@link MetricOutput} associated with a verification project.
 * The outputs are stored by threshold in a {@link MetricOutputMultiMapByThreshold}. Retrieve the outputs using the
 * instance methods. If no outputs exist, the instance methods return null.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputForProjectByThreshold implements MetricOutputForProjectByThreshold
{

    /**
     * A {@link MetricOutputMultiMapByThreshold} of {@link ScalarOutput}.
     */

    private final MetricOutputMultiMapByThreshold<ScalarOutput> scalarOutput;

    /**
     * A {@link MetricOutputMultiMapByThreshold} of {@link VectorOutput}.
     */

    private final MetricOutputMultiMapByThreshold<VectorOutput> vectorOutput;

    /**
     * A {@link MetricOutputMultiMapByThreshold} of {@link MultiVectorOutput}.
     */

    private final MetricOutputMultiMapByThreshold<MultiVectorOutput> multiVectorOutput;

    /**
     * A {@link MetricOutputMultiMapByThreshold} of {@link MatrixOutput}.
     */

    private final MetricOutputMultiMapByThreshold<MatrixOutput> matrixOutput;

    @Override
    public MetricOutputMultiMapByThreshold<ScalarOutput> getScalarOutput()
    {
        return scalarOutput;
    }

    @Override
    public MetricOutputMultiMapByThreshold<VectorOutput> getVectorOutput()
    {
        return vectorOutput;
    }

    @Override
    public MetricOutputMultiMapByThreshold<MultiVectorOutput> getMultiVectorOutput()
    {
        return multiVectorOutput;
    }

    @Override
    public MetricOutputMultiMapByThreshold<MatrixOutput> getMatrixOutput()
    {
        return matrixOutput;
    }

    /**
     * Builder.
     */

    static class MetricOutputForProjectByThresholdBuilder implements Builder
    {

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapKey<Threshold>, MetricOutputMapByMetric<ScalarOutput>> scalarInternal =
                                                                                                             new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapKey<Threshold>, MetricOutputMapByMetric<VectorOutput>> vectorInternal =
                                                                                                             new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapKey<Threshold>, MetricOutputMapByMetric<MultiVectorOutput>> multiVectorInternal =
                                                                                                                       new ConcurrentSkipListMap<>();

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapKey<Threshold>, MetricOutputMapByMetric<MatrixOutput>> matrixInternal =
                                                                                                             new ConcurrentSkipListMap<>();

        /**
         * Adds a new scalar result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public Builder addScalarOutput(Threshold threshold, MetricOutputMapByMetric<ScalarOutput> result)
        {
            scalarInternal.put(DefaultDataFactory.getInstance().getMapKey(threshold), result);
            return this;
        }

        /**
         * Adds a new vector result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public Builder addVectorOutput(Threshold threshold, MetricOutputMapByMetric<VectorOutput> result)
        {
            vectorInternal.put(DefaultDataFactory.getInstance().getMapKey(threshold), result);
            return this;
        }

        /**
         * Adds a new multi-vector result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public Builder addMultiVectorOutput(Threshold threshold, MetricOutputMapByMetric<MultiVectorOutput> result)
        {
            multiVectorInternal.put(DefaultDataFactory.getInstance().getMapKey(threshold), result);
            return this;
        }

        /**
         * Adds a new matrix result for a collection of metrics to the internal store.
         * 
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public Builder addMatrixOutput(Threshold threshold, MetricOutputMapByMetric<MatrixOutput> result)
        {
            matrixInternal.put(DefaultDataFactory.getInstance().getMapKey(threshold), result);
            return this;
        }

        /**
         * Returns a {@link MetricOutputForProjectByThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByThreshold}
         */

        public MetricOutputForProjectByThreshold build()
        {
            return new SafeMetricOutputForProjectByThreshold(this);
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private SafeMetricOutputForProjectByThreshold(MetricOutputForProjectByThresholdBuilder builder)
    {
        DataFactory d = DefaultDataFactory.getInstance();
        scalarOutput = d.ofMultiMap(builder.scalarInternal);
        vectorOutput = d.ofMultiMap(builder.vectorInternal);
        multiVectorOutput = d.ofMultiMap(builder.multiVectorInternal);
        matrixOutput = d.ofMultiMap(builder.matrixInternal);
    }

}
