package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.inputs.InsufficientDataException;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.outputs.ScoreOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.engine.statistics.metric.config.MetricConfigurationException;
import wres.engine.statistics.metric.processing.MetricProcessorByTime.MetricFuturesByTime.MetricFuturesByTimeBuilder;
import wres.engine.statistics.metric.timeseries.TimeToPeakErrorStatistics;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link SingleValuedPairs} and configured transformations of {@link SingleValuedPairs}. For example, metrics that
 * consume {@link DichotomousPairs} may be processed after transforming the {@link SingleValuedPairs} with an
 * appropriate mapping function.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricProcessorByTimeSingleValuedPairs extends MetricProcessorByTime<SingleValuedPairs>
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScoreOutput}.
     */

    private final MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput> dichotomousScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link TimeSeriesOfSingleValuedPairs} and produce
     * {@link PairedOutput}.
     */

    private final MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>> timeSeries;

    /**
     * An instance of {@link TimeToPeakErrorStatistics}.
     */

    private final TimeToPeakErrorStatistics timeToPeakErrorStats;

    @Override
    public MetricOutputForProjectByTimeAndThreshold apply( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );
        TimeWindow timeWindow = input.getMetadata().getTimeWindow();
        Objects.requireNonNull( timeWindow, "Expected a non-null time window in the input metadata." );

        //Slicer
        Slicer slicer = dataFactory.getSlicer();

        //Remove missing values, except for ordered input, such as time-series
        SingleValuedPairs inputNoMissing = input;
        try
        {
            if ( ! ( input instanceof TimeSeriesOfSingleValuedPairs ) )
            {
                inputNoMissing = slicer.filter( input, ADMISSABLE_DATA, true );
            }
        }
        catch ( MetricInputSliceException e )
        {
            throw new MetricCalculationException( "While attempting to remove missing values: ", e );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();
        futures.addDataFactory( dataFactory );

        //Process the metrics that consume single-valued pairs
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            processSingleValuedPairs( timeWindow, inputNoMissing, futures );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            processDichotomousPairs( timeWindow, inputNoMissing, futures );
        }
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            processTimeSeriesPairs( timeWindow, (TimeSeriesOfSingleValuedPairs) inputNoMissing, futures );
        }

        // Log
        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Completed processing of metrics for feature '{}' at time window '{}'.",
                          input.getMetadata().getIdentifier().getGeospatialID(),
                          input.getMetadata().getTimeWindow() );
        }

        //Process and return the result       
        MetricFuturesByTime futureResults = futures.build();
        //Add for merge with existing futures, if required
        addToMergeList( futureResults );
        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(SingleValuedPairs)}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public MetricProcessorByTimeSingleValuedPairs( final DataFactory dataFactory,
                                                   final ProjectConfig config,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final MetricOutputGroup... mergeList )
            throws MetricConfigurationException, MetricParameterException
    {
        super( dataFactory, config, thresholdExecutor, metricExecutor, mergeList );
        //Construct the metrics
        //Dichotomous scores
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCORE ) )
        {
            dichotomousScalar =
                    metricFactory.ofDichotomousScoreCollection( metricExecutor,
                                                                getSelectedMetrics( metrics,
                                                                                    MetricInputGroup.DICHOTOMOUS,
                                                                                    MetricOutputGroup.SCORE ) );
        }
        else
        {
            dichotomousScalar = null;
        }
        //Time-series 
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ) )
        {
            timeSeries = metricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
                                                                           getSelectedMetrics( metrics,
                                                                                               MetricInputGroup.SINGLE_VALUED_TIME_SERIES,
                                                                                               MetricOutputGroup.PAIRED ) );
            //Summary statistics, currently done for time-to-peak only
            //TODO: replace with a collection if/when other measures of the same type are added                    
            if ( MetricConfigHelper.hasSummaryStatisticsFor( config, MetricConfigName.TIME_TO_PEAK_ERROR ) )
            {
                timeToPeakErrorStats =
                        metricFactory.ofTimeToPeakErrorStatistics( MetricConfigHelper.getSummaryStatisticsFor( config,
                                                                                                               MetricConfigName.TIME_TO_PEAK_ERROR ) );
            }
            else
            {
                timeToPeakErrorStats = null;
            }
        }
        else
        {
            timeSeries = null;
            timeToPeakErrorStats = null;
        }
    }

    @Override
    void validate( ProjectConfig config ) throws MetricConfigurationException
    {
        //Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : metrics )
        {
            if ( ! ( next.isInGroup( MetricInputGroup.SINGLE_VALUED )
                     || next.isInGroup( MetricInputGroup.SINGLE_VALUED_TIME_SERIES )
                     || next.isInGroup( MetricInputGroup.DICHOTOMOUS ) ) )
            {
                throw new MetricConfigurationException( "Cannot configure '" + next
                                                        + "' for "
                                                        + MetricInputGroup.SINGLE_VALUED
                                                        + ": correct the configuration '"
                                                        + config.getLabel()
                                                        + "'." );
            }
        }
    }

    @Override
    void completeCachedOutput() throws MetricOutputAccessException
    {
        //Add the summary statistics for the cached time-to-peak errors if these statistics do not already exist
        if ( hasCachedMetricOutput() && getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.PAIRED )
             && !getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.DURATION_SCORE )
             && getCachedMetricOutputInternal().getPairedOutput()
                                               .containsKey( dataFactory.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR ) ) )
        {
            MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> output =
                    getCachedMetricOutputInternal().getPairedOutput().get( MetricConstants.TIME_TO_PEAK_ERROR );
            PairedOutput<Instant, Duration> union = dataFactory.unionOf( output.values() );
            TimeWindow unionWindow = union.getMetadata().getTimeWindow();
            Pair<TimeWindow, Threshold> key =
                    Pair.of( unionWindow, dataFactory.getThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) );
            //Build the future result
            Supplier<MetricOutputMapByMetric<DurationScoreOutput>> supplier = () -> {
                DurationScoreOutput result = timeToPeakErrorStats.aggregate( union );
                List<DurationScoreOutput> in = new ArrayList<>();
                in.add( result );
                return dataFactory.ofMap( in );
            };
            Future<MetricOutputMapByMetric<DurationScoreOutput>> addMe =
                    CompletableFuture.supplyAsync( supplier, thresholdExecutor );
            //Add the future result to the store
            //Metric futures 
            MetricFuturesByTimeBuilder addFutures = new MetricFuturesByTimeBuilder();
            addFutures.addDataFactory( dataFactory );
            addFutures.addDurationScoreOutput( key, addMe );
            futures.add( addFutures.build() );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function. Skips any thresholds for which
     * {@link Double#isFinite(double)} returns <code>false</code> on the threshold value(s).
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processDichotomousPairs( TimeWindow timeWindow,
                                          SingleValuedPairs input,
                                          MetricFuturesByTimeBuilder futures )
    {
        //Process thresholds
        Set<Threshold> global = getThresholds( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCORE );
        double[] sorted = getSortedClimatology( input, global );
        Map<Threshold, MetricCalculationException> failures = new HashMap<>();
        global.forEach( threshold -> {
            Threshold useMe = getThreshold( threshold, sorted );
            Set<MetricConstants> ignoreTheseMetricsForThisThreshold =
                    doNotComputeTheseMetricsForThisThreshold( MetricInputGroup.DICHOTOMOUS,
                                                              MetricOutputGroup.SCORE,
                                                              threshold );
            MetricCalculationException result =
                    processDichotomousThreshold( timeWindow,
                                                 input,
                                                 futures,
                                                 useMe,
                                                 ignoreTheseMetricsForThisThreshold );
            if ( !Objects.isNull( result ) )
            {
                failures.put( useMe, result );
            }
        } );
        //Handle any failures
        handleThresholdFailures( failures, global.size(), input.getMetadata(), MetricInputGroup.DICHOTOMOUS );
    }

    /**
     * Processes a set of metric futures that consume {@link TimeSeriesOfSingleValuedPairs}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     * @throws InsufficientDataException if there is insufficient data to compute any metrics
     */

    private void processTimeSeriesPairs( TimeWindow timeWindow,
                                         TimeSeriesOfSingleValuedPairs input,
                                         MetricFuturesByTimeBuilder futures )
    {
        //Build the future result
        Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> output =
                CompletableFuture.supplyAsync( () -> timeSeries.apply( input ),
                                               thresholdExecutor );
        //Add the future result to the store
        futures.addPairedOutput( Pair.of( timeWindow,
                                          dataFactory.getThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) ),
                                 output );
    }

    /**
     * Processes one threshold for metrics that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function, and produce a {@link MetricOutputGroup#SCORE}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param threshold the threshold
     * @param ignoreTheseMetricsForThisThreshold a set of metrics within the prescribed group that should be 
     *            ignored for this threshold
     * @return a MetricCalculationException for information if the threshold cannot be computed
     */

    private MetricCalculationException processDichotomousThreshold( TimeWindow timeWindow,
                                                                    SingleValuedPairs input,
                                                                    MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                                    Threshold threshold,
                                                                    Set<MetricConstants> ignoreTheseMetricsForThisThreshold )
    {
        MetricCalculationException returnMe = null;
        try
        {
            futures.addDoubleScoreOutput( Pair.of( timeWindow, threshold ),
                                          processDichotomousThreshold( threshold,
                                                                       input,
                                                                       dichotomousScalar,
                                                                       ignoreTheseMetricsForThisThreshold ) );
        }
        //Insufficient data for one threshold: log, but allow
        catch ( MetricInputSliceException e )
        {
            returnMe = new MetricCalculationException( e.getMessage(), e );
        }
        return returnMe;
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DichotomousPairs} at a specific lead
     * time and {@link Threshold}.
     * 
     * @param <T> the type of {@link MetricOutput}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @param ignoreTheseMetricsForThisThreshold a set of metrics within the prescribed group that should be 
     *            ignored for this threshold
     * @return true if the future was added successfully
     * @throws MetricInputSliceException if the pairs contain insufficient data to compute the metrics
     */

    private <T extends MetricOutput<?>> Future<MetricOutputMapByMetric<T>>
            processDichotomousThreshold( Threshold threshold,
                                         SingleValuedPairs pairs,
                                         MetricCollection<DichotomousPairs, MatrixOutput, T> collection,
                                         Set<MetricConstants> ignoreTheseMetricsForThisThreshold )
                    throws MetricInputSliceException
    {
        //Check the data before transformation
        checkSlice( pairs, threshold );
        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<PairOfDoubles, PairOfBooleans> mapper =
                pair -> dataFactory.pairOf( threshold.test( pair.getItemOne() ),
                                            threshold.test( pair.getItemTwo() ) );
        //Slice the pairs
        DichotomousPairs transformed = dataFactory.getSlicer().transformPairs( pairs, mapper );
        //Check the data after transformation
        checkDichotomousSlice( transformed, threshold );
        return CompletableFuture.supplyAsync( () -> collection.apply( transformed, ignoreTheseMetricsForThisThreshold ),
                                              thresholdExecutor );
    }

    /**
     * Validates the {@link DichotomousPairs} and throws an exception if the smaller of the number of 
     * occurrences or non-occurrences is less than the {@link #minimumSampleSize}.
     * 
     * @param subset the data to validate
     * @param threshold the threshold used to localize the error message
     * @throws MetricInputSliceException if the input contains insufficient data for metric calculation 
     */

    private void checkDichotomousSlice( DichotomousPairs subset, Threshold threshold )
            throws MetricInputSliceException
    {
        long occurrences = subset.getData().stream().filter( a -> a.getBooleans()[0] ).count();
        double min = Math.min( occurrences, subset.getData().size() - occurrences );
        if ( min < minimumSampleSize )
        {
            throw new MetricInputSliceException( "Failed to compute one or more metrics for threshold '"
                                                 + threshold
                                                 + "', as the (smaller of the) number of observed occurrences and "
                                                 + "non-occurrences was less than the prescribed minimum of '"
                                                 + minimumSampleSize
                                                 + "'." );
        }
    }

}
