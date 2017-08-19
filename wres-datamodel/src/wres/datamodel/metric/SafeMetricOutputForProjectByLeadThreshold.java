package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metric.MetricConstants.MetricOutputGroup;
import wres.datamodel.metric.MetricOutputMultiMapByLeadThreshold.MetricOutputMultiMapByLeadThresholdBuilder;
import wres.datamodel.metric.SafeMetricOutputMultiMapByLeadThreshold.SafeMetricOutputMultiMapByLeadThresholdBuilder;

/**
 * <p>
 * An immutable implementation of a {@link MetricOutputForProjectByLeadThreshold}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class SafeMetricOutputForProjectByLeadThreshold implements MetricOutputForProjectByLeadThreshold
{

    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(SafeMetricOutputForProjectByLeadThreshold.class);

    /**
     * Thread safe map for {@link ScalarOutput}.
     */

    private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<ScalarOutput>>> scalar =
                                                                                                                    new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link VectorOutput}.
     */

    private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<VectorOutput>>> vector =
                                                                                                                    new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MultiVectorOutput}.
     */

    private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MultiVectorOutput>>> multiVector =
                                                                                                                              new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MatrixOutput}.
     */

    private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MatrixOutput>>> matrix =
                                                                                                                    new ConcurrentHashMap<>();

    @Override
    public boolean hasOutput(MetricOutputGroup outGroup)
    {
        switch(outGroup)
        {
            case SCALAR:
                return !scalar.isEmpty();
            case VECTOR:
                return !vector.isEmpty();
            case MULTIVECTOR:
                return !multiVector.isEmpty();
            case MATRIX:
                return !matrix.isEmpty();
            default:
                return false;
        }
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<MetricOutput<?>> getOutput(MetricOutputGroup... outGroup) throws InterruptedException,
                                                                                                         ExecutionException
    {
        Objects.requireNonNull(outGroup, "Specify one or more output types to return.");
        SafeMetricOutputMultiMapByLeadThresholdBuilder<MetricOutput<?>> builder =
                                                                            new SafeMetricOutputMultiMapByLeadThresholdBuilder<>();
        //Iterate through the types
        for(MetricOutputGroup next: outGroup)
        {
            if(hasOutput(next))
            {
                if(next == MetricOutputGroup.SCALAR)
                {
                    addToBuilder(builder, getScalarOutput());
                }
                if(next == MetricOutputGroup.VECTOR)
                {
                    addToBuilder(builder, getVectorOutput());
                }
                if(next == MetricOutputGroup.MULTIVECTOR)
                {
                    addToBuilder(builder, getMultiVectorOutput());
                }
                if(next == MetricOutputGroup.MATRIX)
                {
                    addToBuilder(builder, getMatrixOutput());
                }
            }
        }
        return builder.build();
    }

    @Override
    public MetricOutputGroup[] getOutputTypes()
    {
        List<MetricOutputGroup> returnMe = new ArrayList<>();
        if(hasOutput(MetricOutputGroup.SCALAR))
        {
            returnMe.add(MetricOutputGroup.SCALAR);
        }
        if(hasOutput(MetricOutputGroup.VECTOR))
        {
            returnMe.add(MetricOutputGroup.VECTOR);
        }
        if(hasOutput(MetricOutputGroup.MULTIVECTOR))
        {
            returnMe.add(MetricOutputGroup.MULTIVECTOR);
        }
        if(hasOutput(MetricOutputGroup.MATRIX))
        {
            returnMe.add(MetricOutputGroup.MATRIX);
        }
        return returnMe.toArray(new MetricOutputGroup[returnMe.size()]);
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<ScalarOutput> getScalarOutput() throws InterruptedException,
                                                                               ExecutionException
    {
        return unwrap(MetricOutputGroup.SCALAR, scalar);
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<VectorOutput> getVectorOutput() throws InterruptedException,
                                                                               ExecutionException
    {
        return unwrap(MetricOutputGroup.VECTOR, vector);
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> getMultiVectorOutput() throws InterruptedException,
                                                                                         ExecutionException
    {
        return unwrap(MetricOutputGroup.MULTIVECTOR, multiVector);
    }

    @Override
    public MetricOutputMultiMapByLeadThreshold<MatrixOutput> getMatrixOutput() throws InterruptedException,
                                                                               ExecutionException
    {
        return unwrap(MetricOutputGroup.MATRIX, matrix);
    }

    /**
     * Builder.
     */

    static class SafeMetricOutputForProjectByLeadThresholdBuilder implements MetricOutputForProjectByLeadThresholdBuilder
    {

        /**
         * Thread safe map for {@link ScalarOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<ScalarOutput>>> scalarInternal =
                                                                                                                                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link VectorOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<VectorOutput>>> vectorInternal =
                                                                                                                                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MultiVectorOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MultiVectorOutput>>> multiVectorInternal =
                                                                                                                                          new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MatrixOutput}.
         */

        private final ConcurrentMap<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<MatrixOutput>>> matrixInternal =
                                                                                                                                new ConcurrentHashMap<>();

        @Override
        public MetricOutputForProjectByLeadThresholdBuilder addScalarOutput(Integer leadTime,
                                       Threshold threshold,
                                       Future<MetricOutputMapByMetric<ScalarOutput>> result)
        {
            scalarInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public MetricOutputForProjectByLeadThresholdBuilder addVectorOutput(Integer leadTime,
                                       Threshold threshold,
                                       Future<MetricOutputMapByMetric<VectorOutput>> result)
        {
            vectorInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public MetricOutputForProjectByLeadThresholdBuilder addMultiVectorOutput(Integer leadTime,
                                            Threshold threshold,
                                            Future<MetricOutputMapByMetric<MultiVectorOutput>> result)
        {
            multiVectorInternal.put(DefaultDataFactory.getInstance().getMapKey(leadTime, threshold), result);
            return this;
        }

        @Override
        public MetricOutputForProjectByLeadThresholdBuilder addMatrixOutput(Integer leadTime,
                                       Threshold threshold,
                                       Future<MetricOutputMapByMetric<MatrixOutput>> result)
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

    private SafeMetricOutputForProjectByLeadThreshold(SafeMetricOutputForProjectByLeadThresholdBuilder builder)
    {
        scalar.putAll(builder.scalarInternal);
        vector.putAll(builder.vectorInternal);
        multiVector.putAll(builder.multiVectorInternal);
        matrix.putAll(builder.matrixInternal);
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

    /**
     * Unwraps a map of values that are wrapped in {@link Future} by calling {@link Future#get()} on each value and
     * returning a map of the unwrapped entries.
     * 
     * @param outGroup the {@link MetricOutputGroup} for error logging
     * @param wrapped the map of values wrapped in {@link Future}
     * @return the unwrapped map or null if the input is empty
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    private <T extends MetricOutput<?>> MetricOutputMultiMapByLeadThreshold<T> unwrap(MetricOutputGroup outGroup,
                                                                                      Map<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<T>>> wrapped) throws InterruptedException,
                                                                                                                                                                     ExecutionException
    {
        if(wrapped.isEmpty())
        {
            return null;
        }
        DataFactory d = DefaultDataFactory.getInstance();
        Map<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<T>> unwrapped = new HashMap<>();
        for(Map.Entry<MapBiKey<Integer, Threshold>, Future<MetricOutputMapByMetric<T>>> next: wrapped.entrySet())
        {
            try
            {
                unwrapped.put(next.getKey(), next.getValue().get());
            }
            catch(InterruptedException | ExecutionException e)
            {
                LOGGER.error("While retrieving the results for group {} at lead time {} and threshold {}.",
                             outGroup,
                             next.getKey().getFirstKey(),
                             next.getKey().getSecondKey(),
                             e);
                throw e;
            }
        }
        return d.ofMultiMap(unwrapped);
    }

}
