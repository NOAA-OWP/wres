package wres.datamodel.outputs;

import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.Threshold;

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
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<ScalarOutput> getScalarOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link VectorOutput} or null if no output exists.
     * 
     * @return the vector output or null
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<VectorOutput> getVectorOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> getMultiVectorOutput()
            throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> getMatrixOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputAccessException if the retrieval of {@link MetricOutput} fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotOutput() throws MetricOutputAccessException;

    /**
     * Builder.
     */

    interface MetricOutputForProjectByTimeAndThresholdBuilder
    {

        /**
         * Adds a new {@link ScalarOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addScalarOutput( Pair<TimeWindow, Threshold> key,
                                                                                 Future<MetricOutputMapByMetric<ScalarOutput>> result )
        {
            addScalarOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link VectorOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addVectorOutput( Pair<TimeWindow, Threshold> key,
                                                                                 Future<MetricOutputMapByMetric<VectorOutput>> result )
        {
            addVectorOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder
                addMultiVectorOutput( Pair<TimeWindow, Threshold> key,
                                      Future<MetricOutputMapByMetric<MultiVectorOutput>> result )
        {
            addMultiVectorOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link MatrixOutput} result for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addMatrixOutput( Pair<TimeWindow, Threshold> key,
                                                                                 Future<MetricOutputMapByMetric<MatrixOutput>> result )
        {
            addMatrixOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addBoxPlotOutput( Pair<TimeWindow, Threshold> key,
                                                                                  Future<MetricOutputMapByMetric<BoxPlotOutput>> result )
        {
            addBoxPlotOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link ScalarOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
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
         * Adds a new {@link VectorOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
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
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
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
         * Adds a new {@link MatrixOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
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
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
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
