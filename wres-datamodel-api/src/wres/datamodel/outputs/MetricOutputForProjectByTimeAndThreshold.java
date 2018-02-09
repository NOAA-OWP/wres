package wres.datamodel.outputs;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.TimeWindow;

/**
 * <p>
 * A high-level store of {@link MetricOutput} associated with a verification project. The outputs are stored by 
 * {@link TimeWindow} and {@link Threshold} in a {@link MetricOutputMultiMapByTimeAndThreshold}. The 
 * {@link MetricOutputMultiMapByTimeAndThreshold} are further grouped by {@link MetricOutputGroup}, which denotes the 
 * atomic type of output stored by the container. For example, the {@link MetricOutputGroup#DOUBLE_SCORE} maps to 
 * {@link DoubleScoreOutput}.
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
     * Returns a {@link MetricOutputMultiMap} of {@link DoubleScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> getDoubleScoreOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link DurationScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> getDurationScoreOutput()
            throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> getMultiVectorOutput()
            throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> getMatrixOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotOutput() throws MetricOutputAccessException;

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link PairedOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputAccessException if the retrieval fails for any reason
     */

    MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> getPairedOutput()
            throws MetricOutputAccessException;

    /**
     * Builder.
     */

    interface MetricOutputForProjectByTimeAndThresholdBuilder
    {

        /**
         * Adds a new {@link DoubleScoreOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addDoubleScoreOutput( Pair<TimeWindow, Threshold> key,
                                                                                      Future<MetricOutputMapByMetric<DoubleScoreOutput>> result )
        {
            addDoubleScoreOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link DurationScoreOutput} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addDurationScoreOutput( Pair<TimeWindow, Threshold> key,
                                                                                        Future<MetricOutputMapByMetric<DurationScoreOutput>> result )
        {
            addDurationScoreOutput( key.getLeft(), key.getRight(), result );
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
         * Adds a new {@link PairedOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        default MetricOutputForProjectByTimeAndThresholdBuilder addPairedOutput( Pair<TimeWindow, Threshold> key,
                                                                                 Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> result )
        {
            addPairedOutput( key.getLeft(), key.getRight(), result );
            return this;
        }

        /**
         * Adds a new {@link DoubleScoreOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addDoubleScoreOutput( TimeWindow timeWindow,
                                                                              Threshold threshold,
                                                                              Future<MetricOutputMapByMetric<DoubleScoreOutput>> result );

        /**
         * Adds a new {@link DurationScoreOutput} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addDurationScoreOutput( TimeWindow timeWindow,
                                                                                Threshold threshold,
                                                                                Future<MetricOutputMapByMetric<DurationScoreOutput>> result );

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
         * Adds a new {@link PairedOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        MetricOutputForProjectByTimeAndThresholdBuilder addPairedOutput( TimeWindow timeWindow,
                                                                         Threshold threshold,
                                                                         Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> result );

        /**
         * Returns a {@link MetricOutputForProjectByTimeAndThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByTimeAndThreshold}
         */

        MetricOutputForProjectByTimeAndThreshold build();

    }

}
