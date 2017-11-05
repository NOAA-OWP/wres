package wres.datamodel;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.MetricConstants.MetricOutputGroup;

/**
 * <p>
 * A high-level store of {@link MetricOutput} associated with a verification project. The outputs are stored by 
 * {@link TimeWindow} and {@link Threshold} in a {@link MetricOutputMultiMapByTimeAndThreshold}. The 
 * {@link MetricOutputMultiMapByTimeAndThreshold} are further grouped by {@link MetricOutputGroup}, which denotes the 
 * atomic type of output stored by the container. For example, the {@link MetricOutputGroup#SCALAR} maps to 
 * {@link ScalarOutput}.
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

public interface MetricOutputForProjectByTimeAndThreshold
        extends MetricOutputForProject<MetricOutputMultiMapByTimeAndThreshold<?>>
{

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link ScalarOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> getScalarOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByTimeAndThreshold<VectorOutput> getVectorOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> getMultiVectorOutput()
            throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> getMatrixOutput() throws InterruptedException, ExecutionException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws InterruptedException if the retrieval of {@link MetricOutput} is cancelled
     * @throws ExecutionException if the retrieval of {@link MetricOutput} fails
     */

    MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotOutput()
            throws InterruptedException, ExecutionException;

    /**
     * Builder.
     */

    interface MetricOutputForProjectByTimeAndThresholdBuilder
    {

        /**
         * Adds a new {@link ScalarOutput} for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addScalarOutput( MapBiKey<TimeWindow, Threshold> key,
                                                                              Future<MetricOutputMapByMetric<ScalarOutput>> result )
        {
            addScalarOutput( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new {@link VectorOutput} for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addVectorOutput( MapBiKey<TimeWindow, Threshold> key,
                                                                              Future<MetricOutputMapByMetric<VectorOutput>> result )
        {
            addVectorOutput( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addMultiVectorOutput( MapBiKey<TimeWindow, Threshold> key,
                                                                                   Future<MetricOutputMapByMetric<MultiVectorOutput>> result )
        {
            addMultiVectorOutput( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new {@link MatrixOutput} result for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addMatrixOutput( MapBiKey<TimeWindow, Threshold> key,
                                                                              Future<MetricOutputMapByMetric<MatrixOutput>> result )
        {
            addMatrixOutput( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addBoxPlotOutput( MapBiKey<TimeWindow, Threshold> key,
                                                                               Future<MetricOutputMapByMetric<BoxPlotOutput>> result )
        {
            addBoxPlotOutput( key.getFirstKey(), key.getSecondKey(), result );
            return this;
        }

        /**
         * Adds a new {@link ScalarOutput} for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addScalarOutput( TimeWindow timeWindow,
                                                                      Threshold threshold,
                                                                      Future<MetricOutputMapByMetric<ScalarOutput>> result );

        /**
         * Adds a new {@link VectorOutput} for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addVectorOutput( TimeWindow timeWindow,
                                                                      Threshold threshold,
                                                                      Future<MetricOutputMapByMetric<VectorOutput>> result );

        /**
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addMultiVectorOutput( TimeWindow timeWindow,
                                                                           Threshold threshold,
                                                                           Future<MetricOutputMapByMetric<MultiVectorOutput>> result );

        /**
         * Adds a new {@link MatrixOutput} for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addMatrixOutput( TimeWindow timeWindow,
                                                                      Threshold threshold,
                                                                      Future<MetricOutputMapByMetric<MatrixOutput>> result );

        /**
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addBoxPlotOutput( TimeWindow timeWindow,
                                                                       Threshold threshold,
                                                                       Future<MetricOutputMapByMetric<BoxPlotOutput>> result );

        /**
         * Returns a {@link MetricOutputForProjectByTimeAndThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByTimeAndThreshold}
         */

        MetricOutputForProjectByTimeAndThreshold build();

    }

}
