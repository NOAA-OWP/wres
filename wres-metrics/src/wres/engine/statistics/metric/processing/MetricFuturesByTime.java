package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * Store of metric futures for each output type. Use {@link #getMetricOutput()} to obtain the processed
 * {@link MetricOutputForProjectByTimeAndThreshold}.
 * 
 * @author james.brown@hydrosolved.com
 */

class MetricFuturesByTime
{

    /**
     * Instance of a {@link DataFactory}
     */

    private DataFactory dataFactory;

    /**
     * {@link DoubleScoreOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>>> doubleScore =
            new ConcurrentHashMap<>();

    /**
     * {@link DurationScoreOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DurationScoreOutput>>>> durationScore =
            new ConcurrentHashMap<>();

    /**
     * {@link MultiVectorOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVector =
            new ConcurrentHashMap<>();

    /**
     * {@link BoxPlotOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
            new ConcurrentHashMap<>();

    /**
     * {@link PairedOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>>> paired =
            new ConcurrentHashMap<>();

    /**
     * {@link MatrixOutput} results.
     */

    private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrix =
            new ConcurrentHashMap<>();

    /**
     * Returns the results associated with the futures.
     * 
     * @return the metric results
     */

    MetricOutputForProjectByTimeAndThreshold getMetricOutput()
    {
        MetricOutputForProjectByTimeAndThresholdBuilder builder =
                dataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        //Add outputs for current futures
        doubleScore.forEach( ( key, list ) -> list.forEach( value -> builder.addDoubleScoreOutput( key, value ) ) );
        durationScore.forEach( ( key,
                                 list ) -> list.forEach( value -> builder.addDurationScoreOutput( key, value ) ) );
        multiVector.forEach( ( key, list ) -> list.forEach( value -> builder.addMultiVectorOutput( key, value ) ) );
        boxplot.forEach( ( key, list ) -> list.forEach( value -> builder.addBoxPlotOutput( key, value ) ) );
        paired.forEach( ( key, list ) -> list.forEach( value -> builder.addPairedOutput( key, value ) ) );
        matrix.forEach( ( key, list ) -> list.forEach( value -> builder.addMatrixOutput( key, value ) ) );
        return builder.build();
    }

    /**
     * Returns the {@link MetricOutputGroup} for which futures exist.
     * 
     * @return the set of output types for which futures exist
     */

    Set<MetricOutputGroup> getOutputTypes()
    {
        Set<MetricOutputGroup> returnMe = new HashSet<>();

        if ( !this.doubleScore.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.DOUBLE_SCORE );
        }

        if ( !this.durationScore.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.DURATION_SCORE );
        }

        if ( !this.multiVector.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.MULTIVECTOR );
        }

        if ( !this.boxplot.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.BOXPLOT );
        }

        if ( !this.paired.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.PAIRED );
        }

        if ( !this.matrix.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.MATRIX );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns true if one or more future outputs is available, false otherwise.
     * 
     * @return true if one or more future outputs is available, false otherwise
     */

    boolean hasFutureOutputs()
    {
        return !this.getOutputTypes().isEmpty();
    }

    /**
     * A builder for the metric futures.
     */

    static class MetricFuturesByTimeBuilder
    {

        /**
         * Instance of a {@link DataFactory}
         */

        private DataFactory dataFactory;

        /**
         * {@link DoubleScoreOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>>> doubleScore =
                new ConcurrentHashMap<>();

        /**
         * {@link DurationScoreOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<DurationScoreOutput>>>> durationScore =
                new ConcurrentHashMap<>();

        /**
         * {@link MultiVectorOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multiVector =
                new ConcurrentHashMap<>();

        /**
         * {@link BoxPlotOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
                new ConcurrentHashMap<>();

        /**
         * {@link PairedOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>>> paired =
                new ConcurrentHashMap<>();

        /**
         * {@link MatrixOutput} results.
         */

        private final ConcurrentMap<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<MatrixOutput>>>> matrix =
                new ConcurrentHashMap<>();

        /**
         * Adds a data factory.
         * 
         * @param dataFactory the data factory
         * @return the builder
         */

        MetricFuturesByTimeBuilder setDataFactory( DataFactory dataFactory )
        {
            this.dataFactory = dataFactory;
            return this;
        }

        /**
         * Adds a set of future {@link DoubleScoreOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<MetricOutputMapByMetric<DoubleScoreOutput>> value )
        {
            List<Future<MetricOutputMapByMetric<DoubleScoreOutput>>> result =
                    doubleScore.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }
            return this;
        }

        /**
         * Adds a set of future {@link DurationScoreOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                           Future<MetricOutputMapByMetric<DurationScoreOutput>> value )
        {
            List<Future<MetricOutputMapByMetric<DurationScoreOutput>>> result =
                    durationScore.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }
            return this;
        }

        /**
         * Adds a set of future {@link MultiVectorOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMultiVectorOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<MetricOutputMapByMetric<MultiVectorOutput>> value )
        {
            List<Future<MetricOutputMapByMetric<MultiVectorOutput>>> result =
                    multiVector.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }
            return this;
        }

        /**
         * Adds a set of future {@link BoxPlotOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                     Future<MetricOutputMapByMetric<BoxPlotOutput>> value )
        {
            List<Future<MetricOutputMapByMetric<BoxPlotOutput>>> result =
                    boxplot.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }
            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addPairedOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                    Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> value )
        {
            List<Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>>> result =
                    paired.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }

            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param key the key
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMatrixOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                    Future<MetricOutputMapByMetric<MatrixOutput>> value )
        {
            List<Future<MetricOutputMapByMetric<MatrixOutput>>> result =
                    matrix.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
            if ( Objects.nonNull( result ) )
            {
                result.add( value );
            }
            return this;
        }

        /**
         * Build the metric futures.
         * 
         * @return the metric futures
         */

        MetricFuturesByTime build()
        {
            return new MetricFuturesByTime( this );
        }

        /**
         * Adds the outputs from an existing {@link MetricFuturesByTime} for the outputs that are included in the
         * merge list.
         * 
         * @param futures the input futures
         * @param mergeSet the merge list
         * @return the builder
         * @throws MetricOutputMergeException if the outputs cannot be merged across calls
         */

        MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures )
        {
            this.addFutures( futures, MetricOutputGroup.set() );
            return this;
        }

        /**
         * Adds the outputs from an existing {@link MetricFuturesByTime} for the outputs that are included in the
         * merge list.
         * 
         * @param futures the input futures
         * @param mergeSet the merge list
         * @return the builder
         * @throws MetricOutputMergeException if the outputs cannot be merged across calls
         */

        MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures,
                                                       Set<MetricOutputGroup> mergeSet )
        {
            if ( Objects.nonNull( mergeSet ) )
            {
                for ( MetricOutputGroup nextGroup : mergeSet )
                {
                    if ( nextGroup == MetricOutputGroup.DOUBLE_SCORE )
                    {
                        this.safePut( MetricOutputGroup.DOUBLE_SCORE, doubleScore, futures.doubleScore );
                    }
                    else if ( nextGroup == MetricOutputGroup.DURATION_SCORE )
                    {
                        this.safePut( MetricOutputGroup.DURATION_SCORE, durationScore, futures.durationScore );
                    }
                    else if ( nextGroup == MetricOutputGroup.MULTIVECTOR )
                    {
                        this.safePut( MetricOutputGroup.MULTIVECTOR, multiVector, futures.multiVector );
                    }
                    else if ( nextGroup == MetricOutputGroup.BOXPLOT )
                    {
                        this.safePut( MetricOutputGroup.BOXPLOT, boxplot, futures.boxplot );
                    }
                    else if ( nextGroup == MetricOutputGroup.PAIRED )
                    {
                        this.safePut( MetricOutputGroup.PAIRED, paired, futures.paired );
                    }
                    else if ( nextGroup == MetricOutputGroup.MATRIX )
                    {
                        this.safePut( MetricOutputGroup.MATRIX, matrix, futures.matrix );
                    }
                }
            }
            return this;
        }

        /**
         * Adds the specified input to the specified store, throwing an exception if the store already contains
         * this input.
         * 
         * @throws MetricOutputMergeException if the outputs cannot be merged across calls
         */

        private <T extends MetricOutput<?>> void
                safePut( MetricOutputGroup outGroup,
                         Map<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<T>>>> mutate,
                         Map<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<T>>>> newInput )
        {
            for ( Entry<Pair<TimeWindow, OneOrTwoThresholds>, List<Future<MetricOutputMapByMetric<T>>>> next : newInput.entrySet() )
            {
                Object returned = mutate.putIfAbsent( next.getKey(), next.getValue() );
                if ( Objects.nonNull( returned ) )
                {
                    throw new MetricOutputMergeException( "A metric result already exists in this processor for "
                                                          + "metric output group '"
                                                          + outGroup
                                                          + "' at time window '"
                                                          + next.getKey().getLeft()
                                                          + "' and threshold '"
                                                          + next.getKey().getRight()
                                                          + "': change the input data or corresponding metadata "
                                                          + "to ensure that a unique time window and threshold "
                                                          + "is provided for each metric output." );
                }
            }
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MetricFuturesByTime( MetricFuturesByTimeBuilder builder )
    {
        Objects.requireNonNull( builder.dataFactory,
                                "Specify a non-null data factory from which to construct the metric futures." );

        // Builder does not allow for overwrites.
        // Thus, putAll is safe in this context.
        doubleScore.putAll( builder.doubleScore );
        durationScore.putAll( builder.durationScore );
        multiVector.putAll( builder.multiVector );
        boxplot.putAll( builder.boxplot );
        paired.putAll( builder.paired );
        matrix.putAll( builder.matrix );
        dataFactory = builder.dataFactory;
    }

}
