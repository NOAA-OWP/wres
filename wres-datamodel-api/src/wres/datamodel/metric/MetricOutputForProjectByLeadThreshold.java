package wres.datamodel.metric;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.metric.MetricConstants.MetricOutputGroup;

/**
 * <p>
 * A high-level store of {@link MetricOutput} associated with a verification project. The outputs are stored by lead
 * time and threshold in a {@link MetricOutputMultiMapByLeadThreshold}. The {@link MetricOutputMultiMapByLeadThreshold}
 * are further grouped by {@link MetricOutputGroup}, which denotes the atomic type of output stored by the container,
 * For example, the {@link MetricOutputGroup#SCALAR} maps to {@link ScalarOutput}.
 * </p>
 * <p>
 * Retrieve the outputs using the instance methods for particular {@link MetricOutputGroup}. If no outputs exist, the
 * instance methods return null. The store is built with {@link Future} of the {@link MetricOutput} and the instance
 * methods call {@link Future#get()}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricOutputForProjectByLeadThreshold extends MetricOutputForProject<MetricOutputMultiMapByLeadThreshold<?>>
{

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link ScalarOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByLeadThreshold<ScalarOutput> getScalarOutput() throws InterruptedException,
                                                                             ExecutionException;


    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByLeadThreshold<VectorOutput> getVectorOutput() throws InterruptedException,
                                                                             ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByLeadThreshold<MultiVectorOutput> getMultiVectorOutput() throws InterruptedException,
                                                                                       ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByLeadThreshold<MatrixOutput> getMatrixOutput() throws InterruptedException,
                                                                             ExecutionException; 

    /**
     * Builder.
     */

    interface Builder
    {

        /**
         * Adds a new scalar result for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default Builder addScalarOutput(MapBiKey<Integer, Threshold> key,
                                        Future<MetricOutputMapByMetric<ScalarOutput>> result)
        {
            addScalarOutput(key.getFirstKey(), key.getSecondKey(), result);
            return this;
        }

        /**
         * Adds a new vector result for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default Builder addVectorOutput(MapBiKey<Integer, Threshold> key,
                                        Future<MetricOutputMapByMetric<VectorOutput>> result)
        {
            addVectorOutput(key.getFirstKey(), key.getSecondKey(), result);
            return this;
        }

        /**
         * Adds a new multi-vector result for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default Builder addMultiVectorOutput(MapBiKey<Integer, Threshold> key,
                                             Future<MetricOutputMapByMetric<MultiVectorOutput>> result)
        {
            addMultiVectorOutput(key.getFirstKey(), key.getSecondKey(), result);
            return this;
        }

        /**
         * Adds a new matrix result for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default Builder addMatrixOutput(MapBiKey<Integer, Threshold> key,
                                        Future<MetricOutputMapByMetric<MatrixOutput>> result)
        {
            addMatrixOutput(key.getFirstKey(), key.getSecondKey(), result);
            return this;
        }

        /**
         * Adds a new scalar result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addScalarOutput(Integer leadTime,
                                Threshold threshold,
                                Future<MetricOutputMapByMetric<ScalarOutput>> result);

        /**
         * Adds a new vector result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addVectorOutput(Integer leadTime,
                                Threshold threshold,
                                Future<MetricOutputMapByMetric<VectorOutput>> result);

        /**
         * Adds a new multi-vector result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMultiVectorOutput(Integer leadTime,
                                     Threshold threshold,
                                     Future<MetricOutputMapByMetric<MultiVectorOutput>> result);

        /**
         * Adds a new matrix result for a collection of metrics to the internal store.
         * 
         * @param leadTime the lead time
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        Builder addMatrixOutput(Integer leadTime,
                                Threshold threshold,
                                Future<MetricOutputMapByMetric<MatrixOutput>> result);

        /**
         * Returns a {@link MetricOutputForProjectByLeadThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByLeadThreshold}
         */

        MetricOutputForProjectByLeadThreshold build();

    }

}
