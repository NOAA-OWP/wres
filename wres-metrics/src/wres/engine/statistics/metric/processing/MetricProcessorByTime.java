package wres.engine.statistics.metric.processing;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.Slicer;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
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

public abstract class MetricProcessorByTime<S extends SampleData<?>>
        extends MetricProcessor<S, StatisticsForProject>
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
    StatisticsForProject getCachedMetricOutputInternal()
    {
        MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                new MetricFuturesByTime.MetricFuturesByTimeBuilder();
        if ( this.hasCachedMetricOutput() )
        {
            for ( MetricFuturesByTime future : futures )
            {
                builder.addFutures( future );
            }
        }
        return builder.build().getMetricOutput();
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
        Set<StatisticGroup> cacheMe = this.getMetricOutputTypesToCache();
        if ( !cacheMe.isEmpty() )
        {
            MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                    new MetricFuturesByTime.MetricFuturesByTimeBuilder();
            builder.addFutures( mergeFutures, cacheMe );
            this.futures.add( builder.build() );
        }
    }

    /**
     * Processes a set of metric futures for {@link SingleValuedPairs}.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs( SingleValuedPairs input,
                                   MetricFuturesByTime.MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ) )
        {
            processSingleValuedPairsByThreshold( input, futures, StatisticGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.MULTIVECTOR ) )
        {
            processSingleValuedPairsByThreshold( input, futures, StatisticGroup.MULTIVECTOR );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link DichotomousPairs}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    void processDichotomousPairs( DichotomousPairs input,
                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                  StatisticGroup outGroup,
                                  Set<MetricConstants> ignoreTheseMetrics )
    {

        if ( outGroup == StatisticGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( processDichotomousPairs( input,
                                                                   dichotomousScalar,
                                                                   ignoreTheseMetrics ) );
        }
        else if ( outGroup == StatisticGroup.MATRIX )
        {
            futures.addMatrixOutput( processDichotomousPairs( input,
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
     */

    static Predicate<SingleValuedPair> getFilterForSingleValuedPairs( Threshold input )
    {
        Predicate<SingleValuedPair> returnMe = null;

        switch ( input.getDataType() )
        {
            case LEFT:
                returnMe = Slicer.left( input::test );
                break;
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                returnMe = Slicer.leftAndRight( input::test );
                break;
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                returnMe = Slicer.right( input::test );
                break;
        }

        return returnMe;
    }

    /**
     * Helper that returns a predicate for filtering {@link TimeSeriesOfSinglevaluedPairs} based on the 
     * {@link Threshold#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     */

    static Predicate<TimeSeries<SingleValuedPair>> getFilterForTimeSeriesOfSingleValuedPairs( Threshold input )
    {
        Predicate<TimeSeries<SingleValuedPair>> returnMe = null;

        switch ( input.getDataType() )
        {
            case LEFT:
                returnMe = Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( input::test );
                break;
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                returnMe = Slicer.anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( input::test );
                break;
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                returnMe = Slicer.anyOfRightInTimeSeriesOfSingleValuedPairs( input::test );
                break;
        }

        return returnMe;
    }

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of canonical thresholds, may be null
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link StatisticGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    MetricProcessorByTime( final ProjectConfig config,
                           final ThresholdsByMetric externalThresholds,
                           final ExecutorService thresholdExecutor,
                           final ExecutorService metricExecutor,
                           final Set<StatisticGroup> mergeSet )
            throws MetricParameterException
    {
        super( config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );
    }

    /**
     * Processes all thresholds for metrics that consume {@link SingleValuedPairs} and produce a specified 
     * {@link StatisticGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairsByThreshold( SingleValuedPairs input,
                                                      MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                      StatisticGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( SampleDataGroup.SINGLE_VALUED, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add the quantiles to the threshold
            Threshold useMe = this.addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            SampleMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = SampleMetadata.of( input.getBaselineData().getMetadata(), oneOrTwo );
            }

            SingleValuedPairs pairs = SingleValuedPairs.of( input,
                                                            SampleMetadata.of( input.getMetadata(), oneOrTwo ),
                                                            baselineMeta );

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<SingleValuedPair> filter = MetricProcessorByTime.getFilterForSingleValuedPairs( useMe );

                pairs = Slicer.filter( pairs, filter, null );

            }

            processSingleValuedPairs( pairs,
                                      futures,
                                      outGroup,
                                      ignoreTheseMetrics );
        }
    }

    /**
     * Processes one threshold for metrics that consume {@link SingleValuedPairs} and produce a specified 
     * {@link StatisticGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processSingleValuedPairs( SingleValuedPairs input,
                                           MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                           StatisticGroup outGroup,
                                           Set<MetricConstants> ignoreTheseMetrics )
    {
        if ( outGroup == StatisticGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( processSingleValuedPairs( input,
                                                                    singleValuedScore,
                                                                    ignoreTheseMetrics ) );
        }
        else if ( outGroup == StatisticGroup.MULTIVECTOR )
        {
            futures.addMultiVectorOutput( processSingleValuedPairs( input,
                                                                    singleValuedMultiVector,
                                                                    ignoreTheseMetrics ) );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link SingleValuedPairs} at a specific 
     * {@link TimeWindow} and {@link Threshold}.
     * 
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should ignored
     * @return the future result
     */

    private <T extends Statistic<?>> Future<ListOfStatistics<T>>
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
     * @param <T> the type of {@link Statistic}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     * @return true if the future was added successfully
     */

    private <T extends Statistic<?>> Future<ListOfStatistics<T>>
            processDichotomousPairs( DichotomousPairs pairs,
                                     MetricCollection<DichotomousPairs, MatrixStatistic, T> collection,
                                     Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }

}
