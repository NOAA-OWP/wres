package wres.engine.statistics.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.datamodel.outputs.VectorOutput;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public abstract class MetricProcessorByTime extends MetricProcessor<MetricOutputForProjectByTimeAndThreshold>
{

    /**
     * The metric futures from previous calls, indexed by {@link TimeWindow}.
     */

    ConcurrentMap<TimeWindow, MetricFuturesByTime> futures = new ConcurrentSkipListMap<>();

    /**
     * Returns a {@link MetricOutputForProjectByTimeAndThreshold} for the last available results or null if
     * {@link #hasStoredMetricOutput()} returns false.
     * 
     * @return a {@link MetricOutputForProjectByTimeAndThreshold} or null
     */

    @Override
    public MetricOutputForProjectByTimeAndThreshold getStoredMetricOutput()
    {
        MetricOutputForProjectByTimeAndThreshold returnMe = null;
        if ( hasStoredMetricOutput() )
        {
            MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                    new MetricFuturesByTime.MetricFuturesByTimeBuilder();
            builder.addDataFactory( dataFactory );
            for ( MetricFuturesByTime future : futures.values() )
            {
                builder.addFutures( future, mergeList );
            }
            returnMe = builder.build().getMetricOutput();
        }
        return returnMe;
    }

    /**
     * Returns true if a prior call led to the caching of metric outputs.
     * 
     * @return true if stored results are available, false otherwise
     */
    @Override
    public boolean hasStoredMetricOutput()
    {
        return futures.values().stream().anyMatch( MetricFuturesByTime::hasFutureOutputs );
    }

    /**
     * Adds the input {@link MetricFuturesByTime} to the internal store of existing {@link MetricFuturesByTime} 
     * defined for this processor.
     * 
     * @param timeWindow the time window
     * @param mergeFutures the futures to add
     */

    void addToMergeMap( TimeWindow timeWindow, MetricFuturesByTime mergeFutures )
    {
        Objects.requireNonNull( mergeFutures, "Specify non-null futures for merging." );
        //Merge futures if cached outputs identified
        if ( willStoreMetricOutput() )
        {
            futures.put( timeWindow, mergeFutures );
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
         * Scalar results.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<ScalarOutput>>>> scalar =
                new ConcurrentHashMap<>();
        /**
         * Vector results.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<VectorOutput>>>> vector =
                new ConcurrentHashMap<>();
        /**
         * Multivector results.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multivector =
                new ConcurrentHashMap<>();

        /**
         * Box plot results.
         */

        private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
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
            scalar.forEach( ( key, list ) -> list.forEach( value -> builder.addScalarOutput( key, value ) ) );
            vector.forEach( ( key, list ) -> list.forEach( value -> builder.addVectorOutput( key, value ) ) );
            multivector.forEach( ( key, list ) -> list.forEach( value -> builder.addMultiVectorOutput( key, value ) ) );
            boxplot.forEach( ( key, list ) -> list.forEach( value -> builder.addBoxPlotOutput( key, value ) ) );
            return builder.build();
        }

        /**
         * Returns true if one or more future outputs is available, false otherwise.
         * 
         * @return true if one or more future outputs is available, false otherwise
         */

        boolean hasFutureOutputs()
        {
            return ! ( scalar.isEmpty() && vector.isEmpty() && multivector.isEmpty() );
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
             * Scalar results.
             */

            private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<ScalarOutput>>>> scalar =
                    new ConcurrentHashMap<>();
            /**
             * Vector results.
             */

            private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<VectorOutput>>>> vector =
                    new ConcurrentHashMap<>();
            /**
             * Multivector results.
             */

            private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<MultiVectorOutput>>>> multivector =
                    new ConcurrentHashMap<>();

            /**
             * Box plot results.
             */

            private final ConcurrentMap<Pair<TimeWindow, Threshold>, List<Future<MetricOutputMapByMetric<BoxPlotOutput>>>> boxplot =
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
             * Adds a set of future {@link ScalarOutput} to the appropriate internal store.
             * 
             * @param key the key
             * @param value the future result
             * @return the builder
             */

            MetricFuturesByTimeBuilder addScalarOutput( Pair<TimeWindow, Threshold> key,
                                                        Future<MetricOutputMapByMetric<ScalarOutput>> value )
            {
                List<Future<MetricOutputMapByMetric<ScalarOutput>>> result =
                        scalar.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
                if ( Objects.nonNull( result ) )
                {
                    result.add( value );
                }
                return this;
            }

            /**
             * Adds a set of future {@link VectorOutput} to the appropriate internal store.
             * 
             * @param key the key
             * @param value the future result
             * @return the builder
             */

            MetricFuturesByTimeBuilder addVectorOutput( Pair<TimeWindow, Threshold> key,
                                                        Future<MetricOutputMapByMetric<VectorOutput>> value )
            {
                List<Future<MetricOutputMapByMetric<VectorOutput>>> result =
                        vector.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
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

            MetricFuturesByTimeBuilder addMultiVectorOutput( Pair<TimeWindow, Threshold> key,
                                                             Future<MetricOutputMapByMetric<MultiVectorOutput>> value )
            {
                List<Future<MetricOutputMapByMetric<MultiVectorOutput>>> result =
                        multivector.putIfAbsent( key, new ArrayList<>( Arrays.asList( value ) ) );
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

            MetricFuturesByTimeBuilder addBoxPlotOutput( Pair<TimeWindow, Threshold> key,
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
             * Build the metric futures.
             * 
             * @return the metric futures
             */

            MetricFuturesByTime build()
            {
                return new MetricFuturesByTime( this );
            }

            /**
             * Adds the outputs from an existing {@link MetricFuturesByTime} for the outputs that are included in the merge
             * list.
             * 
             * @param futures the input futures
             * @param mergeList the merge list
             * @return the builder
             */

            private MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures,
                                                           MetricOutputGroup[] mergeList )
            {
                if ( Objects.nonNull( mergeList ) )
                {
                    for ( MetricOutputGroup nextGroup : mergeList )
                    {
                        if ( nextGroup == MetricOutputGroup.SCALAR )
                        {
                            scalar.putAll( futures.scalar );
                        }
                        else if ( nextGroup == MetricOutputGroup.VECTOR )
                        {
                            vector.putAll( futures.vector );
                        }
                        else if ( nextGroup == MetricOutputGroup.MULTIVECTOR )
                        {
                            multivector.putAll( futures.multivector );
                        }
                        else if ( nextGroup == MetricOutputGroup.BOXPLOT )
                        {
                            boxplot.putAll( futures.boxplot );
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
            scalar.putAll( builder.scalar );
            vector.putAll( builder.vector );
            multivector.putAll( builder.multivector );
            boxplot.putAll( builder.boxplot );
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

        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR ) )
        {
            processSingleValuedThresholds( timeWindow, input, futures, MetricOutputGroup.SCALAR );
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR ) )
        {
            processSingleValuedThresholds( timeWindow, input, futures, MetricOutputGroup.VECTOR );
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ) )
        {
            processSingleValuedThresholds( timeWindow, input, futures, MetricOutputGroup.MULTIVECTOR );
        }
    }

    /**
     * Handles failures at individual thresholds due to insufficient data. Some failures are allowed, and are logged
     * as warnings. However, if all thresholds fail, a {@link MetricCalculationException} is thrown to terminate 
     * further processing, as this indicates a serious failure.
     * 
     * @param failures a map of failures and associated {@link MetricCalculationException}
     * @param thresholdCount the total number of thresholds attempted
     * @param meta the {@link Metadata} used to help focus messaging
     * @param inGroup the {@link MetricInputGroup} consumed by the metrics on which the failure occurred, used to 
     *            focus messaging
     * @throws InsufficientDataException if all thresholds fail
     */

    static void handleThresholdFailures( Map<Threshold, MetricCalculationException> failures,
                                         int thresholdCount,
                                         Metadata meta,
                                         MetricInputGroup inGroup )
    {
        if ( failures.isEmpty() )
        {
            return;
        }
        //All failed: not allowed
        if ( failures.size() == thresholdCount )
        {
            // Set the first failure as the cause
            throw new InsufficientDataException( "Failed to compute metrics at all " + thresholdCount
                                                  + " available thresholds at time window '"
                                                  + meta.getTimeWindow()
                                                  + "' as insufficient data was available.",
                                                  failures.get( failures.keySet().iterator().next() ) );
        }
        else
        {
            // Aggregate, and report first instance
            LOGGER.warn( "WARN: failed to compute {} of {} thresholds at time window {} for metrics that consume {} "
                         + "inputs. "
                         +
                         failures.get( failures.keySet().iterator().next() ).getMessage(),
                         failures.size(),
                         thresholdCount,
                         meta.getTimeWindow(),
                         inGroup );
        }
    }

    /**
     * Constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}  
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    MetricProcessorByTime( DataFactory dataFactory,
                           ProjectConfig config,
                           ExecutorService thresholdExecutor,
                           ExecutorService metricExecutor,
                           MetricOutputGroup[] mergeList )
            throws MetricConfigurationException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );
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

    private void processSingleValuedThresholds( TimeWindow timeWindow,
                                                SingleValuedPairs input,
                                                MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                MetricOutputGroup outGroup )
    {
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Deal with global thresholds
        if ( hasGlobalThresholds( MetricInputGroup.SINGLE_VALUED ) )
        {
            List<Threshold> global = globalThresholds.get( MetricInputGroup.SINGLE_VALUED );
            double[] sorted = getSortedClimatology( input, global );
            Map<Threshold, MetricCalculationException> failures = new HashMap<>();
            global.forEach( threshold -> {
                Threshold useMe = getThreshold( threshold, sorted );
                MetricCalculationException result =
                        processSingleValuedThreshold( timeWindow, input, futures, outGroup, useMe );
                if ( !Objects.isNull( result ) )
                {
                    failures.put( useMe, result );
                }
            } );
            //Handle any failures
            handleThresholdFailures( failures, global.size(), input.getMetadata(), MetricInputGroup.SINGLE_VALUED );
        }
        //Deal with metric-local thresholds
        else
        {
            //Hook for future logic
            throw new MetricCalculationException( unsupportedException );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link SingleValuedPairs} and produce a specified 
     * {@link MetricOutputGroup}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param threshold the threshold
     * @return a MetricCalculationException for information if the metrics cannot be computed
     */

    private MetricCalculationException processSingleValuedThreshold( TimeWindow timeWindow,
                                                                     SingleValuedPairs input,
                                                                     MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                                     MetricOutputGroup outGroup,
                                                                     Threshold threshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            if ( outGroup == MetricOutputGroup.SCALAR )
            {
                futures.addScalarOutput( Pair.of( timeWindow, threshold ),
                                         processSingleValuedThreshold( threshold,
                                                                       input,
                                                                       singleValuedScalar ) );
            }
            else if ( outGroup == MetricOutputGroup.VECTOR )
            {
                futures.addVectorOutput( Pair.of( timeWindow, threshold ),
                                         processSingleValuedThreshold( threshold,
                                                                       input,
                                                                       singleValuedVector ) );
            }
            else if ( outGroup == MetricOutputGroup.MULTIVECTOR )
            {
                futures.addMultiVectorOutput( Pair.of( timeWindow, threshold ),
                                              processSingleValuedThreshold( threshold,
                                                                            input,
                                                                            singleValuedMultiVector ) );
            }
        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException | InsufficientDataException e )
        {
            returnMe = new MetricCalculationException( e.getMessage(), e );
        }
        return returnMe;
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific 
     * {@link TimeWindow} and {@link Threshold}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @return the future result
     * @throws MetricInputSliceException if the threshold fails to slice sufficient data to compute the metrics
     * @throws InsufficientDataException if the pairs contain only missing values after slicing
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processSingleValuedThreshold( Threshold threshold,
                                          SingleValuedPairs pairs,
                                          MetricCollection<SingleValuedPairs, T> collection )
                    throws MetricInputSliceException
    {
        //Slice the pairs, if required
        SingleValuedPairs subset = pairs;
        if ( threshold.isFinite() )
        {
            subset = dataFactory.getSlicer().filterByLeft( pairs, threshold );
        }
        checkSlice( subset, threshold );
        SingleValuedPairs finalPairs = subset;
        return CompletableFuture.supplyAsync( () -> collection.apply( finalPairs ), thresholdExecutor );
    }

}
