package wres.engine.statistics.metric.processing;

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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.event.Level;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.ThresholdConstants.ThresholdGroup;
import wres.datamodel.ThresholdsByMetric;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
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
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class MetricProcessorByTime<S extends MetricInput<?>>
        extends MetricProcessor<S, MetricOutputForProjectByTimeAndThreshold>
{

    /**
     * The metric futures from previous calls, indexed by {@link TimeWindow}.
     */

    List<MetricFuturesByTime> futures = new CopyOnWriteArrayList<>();

    @Override
    public boolean hasCachedMetricOutput()
    {
        return futures.stream().anyMatch( MetricFuturesByTime::hasFutureOutputs );
    }

    @Override
    MetricOutputForProjectByTimeAndThreshold getCachedMetricOutputInternal()
    {
        MetricOutputForProjectByTimeAndThreshold returnMe = null;
        if ( this.hasCachedMetricOutput() )
        {
            MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                    new MetricFuturesByTime.MetricFuturesByTimeBuilder();
            builder.addDataFactory( dataFactory );
            for ( MetricFuturesByTime future : futures )
            {
                builder.addFutures( future );
            }
            returnMe = builder.build().getMetricOutput();
        }
        return returnMe;
    }

    /**
     * Adds the input {@link MetricFuturesByTime} to the internal store of existing {@link MetricFuturesByTime} 
     * defined for this processor.
     * 
     * @param mergeFutures the futures to add
     */

    void addToMergeList( MetricFuturesByTime mergeFutures )
    {
        Objects.requireNonNull( mergeFutures, "Specify non-null futures for merging." );

        //Merge futures if cached outputs identified
        Set<MetricOutputGroup> cacheMe = this.getMetricOutputTypesToCache();
        if ( !cacheMe.isEmpty() )
        {
            MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                    new MetricFuturesByTime.MetricFuturesByTimeBuilder();
            builder.addDataFactory( dataFactory );
            builder.addFutures( mergeFutures, cacheMe );
            this.futures.add( builder.build() );
        }
    }

    /**
     * Store of metric futures for each output type. Use {@link #getMetricOutput()} to obtain the processed
     * {@link MetricOutputForProjectByTimeAndThreshold}.
     */

    static class MetricFuturesByTime
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

            MetricFuturesByTimeBuilder addDataFactory( DataFactory dataFactory )
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
             */

            private MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures )
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
             */

            private MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures,
                                                           Set<MetricOutputGroup> mergeSet )
            {
                if ( Objects.nonNull( mergeSet ) )
                {
                    for ( MetricOutputGroup nextGroup : mergeSet )
                    {
                        if ( nextGroup == MetricOutputGroup.DOUBLE_SCORE )
                        {
                            doubleScore.putAll( futures.doubleScore );
                        }
                        else if ( nextGroup == MetricOutputGroup.DURATION_SCORE )
                        {
                            durationScore.putAll( futures.durationScore );
                        }
                        else if ( nextGroup == MetricOutputGroup.MULTIVECTOR )
                        {
                            multiVector.putAll( futures.multiVector );
                        }
                        else if ( nextGroup == MetricOutputGroup.BOXPLOT )
                        {
                            boxplot.putAll( futures.boxplot );
                        }
                        else if ( nextGroup == MetricOutputGroup.PAIRED )
                        {
                            paired.putAll( futures.paired );
                        }
                        else if ( nextGroup == MetricOutputGroup.MATRIX )
                        {
                            matrix.putAll( futures.matrix );
                        }
                    }
                }
                return this;
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
            doubleScore.putAll( builder.doubleScore );
            durationScore.putAll( builder.durationScore );
            multiVector.putAll( builder.multiVector );
            boxplot.putAll( builder.boxplot );
            paired.putAll( builder.paired );
            matrix.putAll( builder.matrix );
            dataFactory = builder.dataFactory;
        }

    }

    /**
     * Processes a set of metric futures for {@link SingleValuedPairs}.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs( TimeWindow timeWindow,
                                   SingleValuedPairs input,
                                   MetricFuturesByTime.MetricFuturesByTimeBuilder futures )
    {

        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processSingleValuedPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ) )
        {
            processSingleValuedPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link DichotomousPairs}. 
     * 
     * @param key the key against which to store the metric results
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    void processDichotomousPairs( Pair<TimeWindow, OneOrTwoThresholds> key,
                                  DichotomousPairs input,
                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                  MetricOutputGroup outGroup,
                                  Set<MetricConstants> ignoreTheseMetrics )
    {

        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( key, processDichotomousPairs( input,
                                                                        dichotomousScalar,
                                                                        ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MATRIX )
        {
            futures.addMatrixOutput( key, processDichotomousPairs( input,
                                                                   dichotomousMatrix,
                                                                   ignoreTheseMetrics ) );
        }

    }

    /**
     * Logs failures at individual thresholds due to insufficient data.
     * 
     * @param failures a map of failures and associated {@link MetricCalculationException}
     * @param thresholdCount the total number of thresholds attempted
     * @param meta the {@link Metadata} used to help focus messaging
     * @param inGroup the {@link MetricInputGroup} consumed by the metrics on which the failure occurred, used to 
     *            focus messaging
     * @param level the logging level           
     */

    static void logThresholdFailures( Map<OneOrTwoThresholds, MetricCalculationException> failures,
                                      int thresholdCount,
                                      Metadata meta,
                                      MetricInputGroup inGroup,
                                      Level level )
    {
        if ( Objects.isNull( failures ) || failures.isEmpty() )
        {
            return;
        }

        // Prepare log
        String message = "WARN: failed to compute {} of {} thresholds at time window {} for metrics that consume {} "
                + "inputs. ";
        
        Object[] arguments = new Object[] { failures.get( failures.keySet().iterator().next() ).getMessage(),
                                            failures.size(),
                                            thresholdCount,
                                            meta.getTimeWindow(),
                                            inGroup };
        // Log
        switch ( level )
        {
            case DEBUG:
                LOGGER.debug( message, arguments );
                break;
            case ERROR:
                LOGGER.error( message, arguments );
                break;
            case INFO:
                LOGGER.info( message, arguments );
                break;
            case TRACE:
                LOGGER.trace( message, arguments );
                break;
            case WARN:
                LOGGER.warn( message, arguments );
                break;
            default:
                break;
        }
    }

    /**
     * Helper that returns a predicate for filtering {@link SingleValuedPairs} based on the 
     * {@link Threshold#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws UnsupportedOperationException if the threshold data type is unrecognized
     */

    static Predicate<PairOfDoubles> getFilterForSingleValuedPairs( Threshold input )
    {
        switch ( input.getDataType() )
        {
            case LEFT:
                return Slicer.left( input::test );
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                return Slicer.leftAndRight( input::test );
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                return Slicer.right( input::test );
            default:
                throw new UnsupportedOperationException( "Cannot map the threshold data type '"
                                                         + input.getDataType()
                                                         + "'." );
        }
    }
    
    /**
     * Helper that returns a predicate for filtering {@link TimeSeriesOfSinglevaluedPairs} based on the 
     * {@link Threshold#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws UnsupportedOperationException if the threshold data type is unrecognized
     */

    static Predicate<TimeSeriesOfSingleValuedPairs> getFilterForTimeSeriesOfSingleValuedPairs( Threshold input )
    {
        switch ( input.getDataType() )
        {
            case LEFT:
                return Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( input::test );
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                return Slicer.anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( input::test );
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                return Slicer.anyOfRightInTimeSeriesOfSingleValuedPairs( input::test );
            default:
                throw new UnsupportedOperationException( "Cannot map the threshold data type '"
                                                         + input.getDataType()
                                                         + "'." );
        }
    }    

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param externalThresholds an optional set of canonical thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}  
     * @param mergeSet a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    MetricProcessorByTime( final DataFactory dataFactory,
                           final ProjectConfig config,
                           final ThresholdsByMetric externalThresholds,
                           final ExecutorService thresholdExecutor,
                           final ExecutorService metricExecutor,
                           final Set<MetricOutputGroup> mergeSet )
            throws MetricConfigException, MetricParameterException
    {
        super( dataFactory, config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );
    }

    /**
     * Processes all thresholds for metrics that consume {@link SingleValuedPairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairsByThreshold( TimeWindow timeWindow,
                                                      SingleValuedPairs input,
                                                      MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                      MetricOutputGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.SINGLE_VALUED, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );
        Map<OneOrTwoThresholds, MetricCalculationException> failures = new HashMap<>();

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = this.addQuantilesToThreshold( threshold, sorted );

            try
            {
                SingleValuedPairs pairs = input;

                // Filter the data if required
                if ( useMe.isFinite() )
                {
                    Predicate<PairOfDoubles> filter = MetricProcessorByTime.getFilterForSingleValuedPairs( useMe );

                    pairs = dataFactory.getSlicer().filter( input, filter, null );

                }

                processSingleValuedPairs( Pair.of( timeWindow, OneOrTwoThresholds.of( useMe ) ),
                                          pairs,
                                          futures,
                                          outGroup,
                                          ignoreTheseMetrics );

            }
            // Insufficient data for one threshold: log failures collectively and proceed
            // Such failures are routine and should not be propagated
            catch ( MetricInputSliceException | InsufficientDataException e )
            {
                // TODO: is there a way to prevent the above exceptions from
                // occurring in the first place? Then can remove the
                // logThresholdFailures method too. Refs #46369

                // Decorate failure
                failures.put( OneOrTwoThresholds.of( useMe ),
                              new MetricCalculationException( "While processing threshold " + threshold + ":", e ) );
            }
        }

        // Log failures collectively
        logThresholdFailures( Collections.unmodifiableMap( failures ),
                              union.size(),
                              input.getMetadata(),
                              MetricInputGroup.SINGLE_VALUED,
                              Level.WARN );
    }

    /**
     * Processes one threshold for metrics that consume {@link SingleValuedPairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param key the key against which to store the metric results
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processSingleValuedPairs( Pair<TimeWindow, OneOrTwoThresholds> key,
                                           SingleValuedPairs input,
                                           MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                           MetricOutputGroup outGroup,
                                           Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == MetricOutputGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( key, processSingleValuedPairs( input,
                                                                         singleValuedScore,
                                                                         ignoreTheseMetrics ) );
        }
        else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( key, processSingleValuedPairs( input,
                                                                         singleValuedMultiVector,
                                                                         ignoreTheseMetrics ) );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific 
     * {@link TimeWindow} and {@link Threshold}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should ignored
     * @return the future result
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processSingleValuedPairs( SingleValuedPairs pairs,
                                      MetricCollection<SingleValuedPairs, T, T> collection,
                                      Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }


    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DichotomousPairs}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     * @return true if the future was added successfully
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDichotomousPairs( DichotomousPairs pairs,
                                     MetricCollection<DichotomousPairs, MatrixOutput, T> collection,
                                     Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }

}
