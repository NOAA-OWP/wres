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
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic.SampleDataBasicBuilder;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindow}.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class MetricProcessorByTime<S extends SampleData<?>>
        extends MetricProcessor<S>
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
     * Processes a set of metric futures for single-valued pairs.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param singleValued the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs( SampleData<Pair<Double, Double>> singleValued,
                                   MetricFuturesByTime.MetricFuturesByTimeBuilder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticGroup.DOUBLE_SCORE );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.MULTIVECTOR ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticGroup.MULTIVECTOR );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticGroup.BOXPLOT_PER_POOL ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticGroup.BOXPLOT_PER_POOL );
        }

    }

    /**
     * Processes one threshold for metrics that consume dichotomous pairs. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    void processDichotomousPairs( SampleData<Pair<Boolean, Boolean>> input,
                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                  StatisticGroup outGroup,
                                  Set<MetricConstants> ignoreTheseMetrics )
    {

        if ( outGroup == StatisticGroup.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( this.processDichotomousPairs( input,
                                                                        dichotomousScalar,
                                                                        ignoreTheseMetrics ) );
        }
        else if ( outGroup == StatisticGroup.MATRIX )
        {
            futures.addMatrixOutput( this.processDichotomousPairs( input,
                                                                   dichotomousMatrix,
                                                                   ignoreTheseMetrics ) );
        }

    }

    /**
     * Helper that returns a predicate for filtering single-valued pairs based on the 
     * {@link Threshold#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link Threshold#getDataType()} is null
     * @throws IllegalStateException if the {@link Threshold#getDataType()} is not recognized
     */

    static Predicate<Pair<Double, Double>> getFilterForSingleValuedPairs( Threshold input )
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
                throw new IllegalStateException( "Unrecognized threshold type '" + input.getDataType() + "'." );
        }
    }

    /**
     * Helper that returns a predicate for filtering {@link TimeSeriesOfSinglevaluedPairs} based on the 
     * {@link Threshold#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link Threshold#getDataType()} is null
     * @throws IllegalStateException if the {@link Threshold#getDataType()} is not recognized
     */

    static Predicate<TimeSeries<Pair<Double, Double>>> getFilterForTimeSeriesOfSingleValuedPairs( Threshold input )
    {
        switch ( input.getDataType() )
        {
            case LEFT:
                return TimeSeriesSlicer.anyOfLeftInTimeSeries( input::test );
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                return TimeSeriesSlicer.anyOfLeftAndAnyOfRightInTimeSeries( input::test );
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                return TimeSeriesSlicer.anyOfRightInTimeSeries( input::test );
            default:
                throw new IllegalStateException( "Unrecognized threshold type '" + input.getDataType() + "'." );
        }
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
     * Processes all thresholds for metrics that consume single-valued pairs and produce a specified 
     * {@link StatisticGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairsByThreshold( SampleData<Pair<Double, Double>> input,
                                                      MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                      StatisticGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( SampleDataGroup.SINGLE_VALUED, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = this.getSortedClimatology( input, union );

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

            SampleDataBasicBuilder<Pair<Double, Double>> builder = new SampleDataBasicBuilder<>();

            SampleData<Pair<Double, Double>> pairs = builder.addData( input )
                                                            .setMetadata( SampleMetadata.of( input.getMetadata(),
                                                                                             oneOrTwo ) )
                                                            .setMetadataForBaseline( baselineMeta )
                                                            .build();

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<Pair<Double, Double>> filter = MetricProcessorByTime.getFilterForSingleValuedPairs( useMe );

                pairs = Slicer.filter( pairs, filter, null );

            }

            this.processSingleValuedPairs( pairs,
                                           futures,
                                           outGroup,
                                           ignoreTheseMetrics );
        }
    }

    /**
     * Processes one threshold for metrics that consume single-valued pairs and produce a specified 
     * {@link StatisticGroup}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     */

    private void processSingleValuedPairs( SampleData<Pair<Double, Double>> input,
                                           MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                           StatisticGroup outGroup,
                                           Set<MetricConstants> ignoreTheseMetrics )
    {
        switch ( outGroup )
        {
            case DOUBLE_SCORE:
                futures.addDoubleScoreOutput( this.processSingleValuedPairs( input,
                                                                             this.singleValuedScore,
                                                                             ignoreTheseMetrics ) );
                break;

            case MULTIVECTOR:
                futures.addMultiVectorOutput( this.processSingleValuedPairs( input,
                                                                             this.singleValuedMultiVector,
                                                                             ignoreTheseMetrics ) );
                break;
            case BOXPLOT_PER_POOL:
                futures.addBoxPlotOutputPerPool( this.processSingleValuedPairs( input,
                                                                                this.singleValuedBoxPlot,
                                                                                ignoreTheseMetrics ) );
                break;
            default:
                throw new IllegalStateException( "The statistic group '" + outGroup
                                                 + "' is not supported in this context." );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes single-valued pairs at a specific 
     * {@link TimeWindow} and {@link Threshold}.
     * 
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should ignored
     * @return the future result
     */

    private <T extends Statistic<?>> Future<ListOfStatistics<T>>
            processSingleValuedPairs( SampleData<Pair<Double, Double>> pairs,
                                      MetricCollection<SampleData<Pair<Double, Double>>, T, T> collection,
                                      Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }


    /**
     * Builds a metric future for a {@link MetricCollection} that consumes dichotomous pairs.
     * 
     * @param <T> the type of {@link Statistic}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetrics a set of metrics within the prescribed group that should be ignored
     * @return true if the future was added successfully
     */

    private <T extends Statistic<?>> Future<ListOfStatistics<T>>
            processDichotomousPairs( SampleData<Pair<Boolean, Boolean>> pairs,
                                     MetricCollection<SampleData<Pair<Boolean, Boolean>>, MatrixStatistic, T> collection,
                                     Set<MetricConstants> ignoreTheseMetrics )
    {
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ignoreTheseMetrics ),
                                              thresholdExecutor );
    }

}
