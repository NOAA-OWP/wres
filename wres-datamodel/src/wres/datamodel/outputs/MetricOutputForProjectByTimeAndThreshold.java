package wres.datamodel.outputs;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * <p>
 * An immutable store of {@link MetricOutput} associated with a verification project. The outputs are stored by 
 * {@link TimeWindow} and {@link OneOrTwoThresholds} in a {@link MetricOutputMultiMapByTimeAndThreshold}. The 
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
 */

public class MetricOutputForProjectByTimeAndThreshold
        implements MetricOutputForProject<MetricOutputMultiMapByTimeAndThreshold<?>>
{

    /**
     * Thread safe map for {@link DoubleScoreOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>>> doubleScore =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link DurationScoreOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DurationScoreOutput>>>> durationScore =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MultiVectorOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVector =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link MatrixOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrix =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link BoxPlotOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
            new ConcurrentHashMap<>();

    /**
     * Thread safe map for {@link PairedOutput}.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>>> paired =
            new ConcurrentHashMap<>();

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link DoubleScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<DoubleScoreOutput> getDoubleScoreOutput()
            throws InterruptedException
    {
        return unwrap( MetricOutputGroup.DOUBLE_SCORE, doubleScore );
    }

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link DurationScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> getDurationScoreOutput()
            throws InterruptedException
    {
        return unwrap( MetricOutputGroup.DURATION_SCORE, durationScore );
    }

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<MultiVectorOutput> getMultiVectorOutput()
            throws InterruptedException
    {
        return unwrap( MetricOutputGroup.MULTIVECTOR, multiVector );
    }

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<MatrixOutput> getMatrixOutput() throws InterruptedException
    {
        return unwrap( MetricOutputGroup.MATRIX, matrix );
    }

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<BoxPlotOutput> getBoxPlotOutput() throws InterruptedException
    {
        return unwrap( MetricOutputGroup.BOXPLOT, boxplot );
    }

    /**
     * Returns a {@link MetricOutputMultiMap} of {@link PairedOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> getPairedOutput()
            throws InterruptedException
    {
        return unwrap( MetricOutputGroup.PAIRED, paired );
    }

    @Override
    public boolean hasOutput( MetricOutputGroup outGroup )
    {
        switch ( outGroup )
        {
            case DOUBLE_SCORE:
                return !doubleScore.isEmpty();
            case DURATION_SCORE:
                return !durationScore.isEmpty();
            case MULTIVECTOR:
                return !multiVector.isEmpty();
            case MATRIX:
                return !matrix.isEmpty();
            case BOXPLOT:
                return !boxplot.isEmpty();
            case PAIRED:
                return !paired.isEmpty();
            default:
                return false;
        }
    }

    @Override
    public MetricOutputMultiMapByTimeAndThreshold<MetricOutput<?>> getOutput( MetricOutputGroup... outGroup )
            throws InterruptedException
    {
        Objects.requireNonNull( outGroup, "Specify one or more output types to return." );
        MetricOutputMultiMapByTimeAndThresholdBuilder<MetricOutput<?>> builder =
                new MetricOutputMultiMapByTimeAndThresholdBuilder<>();
        //Iterate through the types
        for ( MetricOutputGroup next : outGroup )
        {
            if ( hasOutput( next ) )
            {
                switch ( next )
                {
                    case DOUBLE_SCORE:
                        addToBuilder( builder, getDoubleScoreOutput() );
                        break;
                    case DURATION_SCORE:
                        addToBuilder( builder, getDurationScoreOutput() );
                        break;
                    case MULTIVECTOR:
                        addToBuilder( builder, getMultiVectorOutput() );
                        break;
                    case MATRIX:
                        addToBuilder( builder, getMatrixOutput() );
                        break;
                    case BOXPLOT:
                        addToBuilder( builder, getBoxPlotOutput() );
                        break;
                    case PAIRED:
                        addToBuilder( builder, getPairedOutput() );
                        break;
                    default:
                        break;
                }
            }
        }
        return builder.build();
    }

    @Override
    public Set<MetricOutputGroup> getOutputTypes()
    {
        Set<MetricOutputGroup> returnMe = new HashSet<>();

        if ( hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
        {
            returnMe.add( MetricOutputGroup.DOUBLE_SCORE );
        }

        if ( hasOutput( MetricOutputGroup.DURATION_SCORE ) )
        {
            returnMe.add( MetricOutputGroup.DURATION_SCORE );
        }

        if ( hasOutput( MetricOutputGroup.MULTIVECTOR ) )
        {
            returnMe.add( MetricOutputGroup.MULTIVECTOR );
        }

        if ( hasOutput( MetricOutputGroup.MATRIX ) )
        {
            returnMe.add( MetricOutputGroup.MATRIX );
        }

        if ( hasOutput( MetricOutputGroup.BOXPLOT ) )
        {
            returnMe.add( MetricOutputGroup.BOXPLOT );
        }

        if ( hasOutput( MetricOutputGroup.PAIRED ) )
        {
            returnMe.add( MetricOutputGroup.PAIRED );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Builder.
     */

    public static class MetricOutputForProjectByTimeAndThresholdBuilder
    {

        /**
         * Thread safe map for {@link DoubleScoreOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>>> doubleScoreInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link DurationScoreOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DurationScoreOutput>>>> durationScoreInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MultiVectorOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVectorInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link MatrixOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrixInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link BoxPlotOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplotInternal =
                new ConcurrentHashMap<>();

        /**
         * Thread safe map for {@link PairedOutput}.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>>> pairedInternal =
                new ConcurrentHashMap<>();

        /**
         * Adds a new {@link DoubleScoreOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param key the key
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addDoubleScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addDurationScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addMultiVectorOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addMatrixOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addBoxPlotOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder
                addPairedOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
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

        public MetricOutputForProjectByTimeAndThresholdBuilder addDoubleScoreOutput( TimeWindow timeWindow,
                                                                                     OneOrTwoThresholds threshold,
                                                                                     Future<MetricOutputMapByMetric<DoubleScoreOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>> existing =
                    doubleScoreInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                     new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Adds a new {@link DurationScoreOutput} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder addDurationScoreOutput( TimeWindow timeWindow,
                                                                                       OneOrTwoThresholds threshold,
                                                                                       Future<MetricOutputMapByMetric<DurationScoreOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<DurationScoreOutput>>> existing =
                    durationScoreInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                       new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder addMultiVectorOutput( TimeWindow timeWindow,
                                                                                     OneOrTwoThresholds threshold,
                                                                                     Future<MetricOutputMapByMetric<MultiVectorOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<MultiVectorOutput>>> existing =
                    multiVectorInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                     new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Adds a new {@link MatrixOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder addMatrixOutput( TimeWindow timeWindow,
                                                                                OneOrTwoThresholds threshold,
                                                                                Future<MetricOutputMapByMetric<MatrixOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<MatrixOutput>>> existing =
                    matrixInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder addBoxPlotOutput( TimeWindow timeWindow,
                                                                                 OneOrTwoThresholds threshold,
                                                                                 Future<MetricOutputMapByMetric<BoxPlotOutput>> result )
        {
            List<Future<MetricOutputMapByMetric<BoxPlotOutput>>> existing =
                    boxplotInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                 new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Adds a new {@link PairedOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param timeWindow the time window
         * @param threshold the threshold
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectByTimeAndThresholdBuilder addPairedOutput( TimeWindow timeWindow,
                                                                                OneOrTwoThresholds threshold,
                                                                                Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> result )
        {
            List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>> existing =
                    pairedInternal.putIfAbsent( Pair.of( timeWindow, threshold ),
                                                new ArrayList<>( Arrays.asList( result ) ) );
            if ( Objects.nonNull( existing ) )
            {
                existing.add( result );
            }
            return this;
        }

        /**
         * Returns a {@link MetricOutputForProjectByTimeAndThreshold}.
         * 
         * @return a {@link MetricOutputForProjectByTimeAndThreshold}
         */

        public MetricOutputForProjectByTimeAndThreshold build()
        {
            return new MetricOutputForProjectByTimeAndThreshold( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MetricOutputForProjectByTimeAndThreshold( MetricOutputForProjectByTimeAndThresholdBuilder builder )
    {
        doubleScore.putAll( builder.doubleScoreInternal );
        durationScore.putAll( builder.durationScoreInternal );
        multiVector.putAll( builder.multiVectorInternal );
        matrix.putAll( builder.matrixInternal );
        boxplot.putAll( builder.boxplotInternal );
        paired.putAll( builder.pairedInternal );
    }

    /**
     * Helper that adds an existing output collection to an existing map.
     *
     * @param builder the builder
     * @param addMe the metric output collection
     */

    private void addToBuilder( MetricOutputMultiMapByTimeAndThresholdBuilder<MetricOutput<?>> builder,
                               MetricOutputMultiMapByTimeAndThreshold<?> addMe )
    {
        addMe.forEach( ( key, value ) -> {
            Map<Pair<TimeWindow, OneOrTwoThresholds>, MetricOutput<?>> map = new TreeMap<>();
            value.forEach( map::put );
            builder.put( key, DataFactory.ofMetricOutputMapByTimeAndThreshold( map ) );
        } );
    }

    /**
     * Unwraps a map of values that are wrapped in {@link Future} by calling {@link Future#get()} on each value and
     * returning a map of the unwrapped entries.
     * 
     * @param <T> the type of output
     * @param outGroup the {@link MetricOutputGroup} for error logging
     * @param wrapped the map of values wrapped in {@link Future}
     * @return the unwrapped map or null if the input is empty
     * @throws InterruptedException if the retrieval is interrupted
     * @throws MetricOutputException if the result could not be produced
     */

    private <T extends MetricOutput<?>> MetricOutputMultiMapByTimeAndThreshold<T> unwrap( MetricOutputGroup outGroup,
                                                                                          Map<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<T>>>> wrapped )
            throws InterruptedException
    {
        if ( wrapped.isEmpty() )
        {
            return null;
        }
        Map<Pair<TimeWindow, OneOrTwoThresholds>, List<MetricOutputMapByMetric<T>>> unwrapped = new HashMap<>();
        for ( Map.Entry<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<T>>>> next : wrapped.entrySet() )
        {
            try
            {
                for ( Future<MetricOutputMapByMetric<T>> nextEntry : next.getValue() )
                {
                    List<MetricOutputMapByMetric<T>> existing =
                            unwrapped.putIfAbsent( next.getKey(), new ArrayList<>( Arrays.asList( nextEntry.get() ) ) );
                    if ( Objects.nonNull( existing ) )
                    {
                        existing.add( nextEntry.get() );
                    }
                }
            }
            catch ( InterruptedException e )
            {
                // Propagate status
                Thread.currentThread().interrupt();

                // Decorate for context
                throw new InterruptedException( "Interrupted while retrieving the results for group " + outGroup
                                                + " "
                                                + "at lead time "
                                                + next.getKey().getLeft()
                                                + " and threshold "
                                                + next.getKey().getRight()
                                                + "." );
            }
            catch ( ExecutionException e )
            {
                // Throw an unchecked exception here, as this is not recoverable
                throw new MetricOutputException( "While retrieving the results for group " + outGroup
                                                 + " at lead time "
                                                 + next.getKey().getLeft()
                                                 + " and threshold "
                                                 + next.getKey().getRight()
                                                 + ".",
                                                 e );
            }
        }
        return DataFactory.ofMetricOutputMultiMapByTimeAndThreshold( unwrapped );
    }


}
