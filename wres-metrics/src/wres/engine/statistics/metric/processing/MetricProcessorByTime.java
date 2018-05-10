package wres.engine.statistics.metric.processing;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class MetricProcessorByTime<S extends MetricInput<?>>
        extends MetricProcessor<S, MetricOutputForProjectByTimeAndThreshold>
{
    
    /**
     * Message that indicates processing is complete.
     */
    
    static final String PROCESSING_COMPLETE_MESSAGE = "Completed processing of metrics for feature '{}' "
            + "at time window '{}'.";

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
            builder.setDataFactory( dataFactory );

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
     * @throws MetricOutputMergeException if the outputs cannot be merged across calls
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
            builder.setDataFactory( dataFactory );
            builder.addFutures( mergeFutures, cacheMe );
            this.futures.add( builder.build() );
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

    static Predicate<TimeSeries<PairOfDoubles>> getFilterForTimeSeriesOfSingleValuedPairs( Threshold input )
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
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
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

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = this.addQuantilesToThreshold( threshold, sorted );

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
