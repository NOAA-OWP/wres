package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.event.Level;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
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
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;
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
                if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
                {
                    throw new MetricCalculationException( " The project configuration includes time-series metrics. "
                                                          + "Expected a time-series of single-valued pairs as input." );
                }
                inputNoMissing = slicer.filter( input, Slicer.leftAndRight( ADMISSABLE_DATA ), ADMISSABLE_DATA );
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
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            this.processSingleValuedPairs( timeWindow, inputNoMissing, futures );
        }
        if ( this.hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            this.processDichotomousPairs( timeWindow, inputNoMissing, futures );
        }
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            this.processTimeSeriesPairs( timeWindow,
                                    (TimeSeriesOfSingleValuedPairs) inputNoMissing,
                                    futures,
                                    MetricOutputGroup.PAIRED );
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
        this.addToMergeList( futureResults );

        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeSet a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(SingleValuedPairs)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public MetricProcessorByTimeSingleValuedPairs( final DataFactory dataFactory,
                                                   final ProjectConfig config,
                                                   final ThresholdsByMetric externalThresholds,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final Set<MetricOutputGroup> mergeSet )
            throws MetricConfigException, MetricParameterException
    {
        super( dataFactory, config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );

        //Construct the metrics

        //Time-series 
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ) )
        {
            timeSeries = metricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
                                                                           getMetrics( metrics,
                                                                                       MetricInputGroup.SINGLE_VALUED_TIME_SERIES,
                                                                                       MetricOutputGroup.PAIRED ) );
            //Summary statistics, currently done for time-to-peak only
            //TODO: replace with a collection if/when other measures of the same type are added                    
            if ( MetricConfigHelper.hasSummaryStatisticsFor( config, TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR ) )
            {
                Set<MetricConstants> ts = MetricConfigHelper.getSummaryStatisticsFor( config,
                                                                                      TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR );
                timeToPeakErrorStats =
                        metricFactory.ofTimeToPeakErrorStatistics( ts );
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
    void validate( ProjectConfig config ) throws MetricConfigException
    {
        //Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : this.metrics )
        {
            if ( ! ( next.isInGroup( MetricInputGroup.SINGLE_VALUED )
                     || next.isInGroup( MetricInputGroup.SINGLE_VALUED_TIME_SERIES )
                     || next.isInGroup( MetricInputGroup.DICHOTOMOUS ) ) )
            {
                throw new MetricConfigException( "Cannot configure '" + next
                                                 + "' for single-valued inputs: correct the configuration "
                                                 + "labelled '"
                                                 + config.getLabel()
                                                 + "'." );
            }

            // Thresholds required for dichotomous metrics
            if ( next.isInGroup( MetricInputGroup.DICHOTOMOUS )
                 && !this.getThresholdsByMetric().hasThresholdsForThisMetricAndTheseTypes( next,
                                                                                           ThresholdGroup.PROBABILITY,
                                                                                           ThresholdGroup.VALUE ) )
            {
                throw new MetricConfigException( "Cannot configure '" + next
                                                 + "' without thresholds to define the events: correct the "
                                                 + "configuration labelled '"
                                                 + config.getLabel()
                                                 + "'." );
            }

        }

        // Check that time-series metrics are not combined with other metrics
        String message = "Cannot configure time-series metrics together with non-time-series "
                         + "metrics: correct the configuration labelled '"
                         + config.getLabel()
                         + "'.";

        // Metrics that are explicitly configured as time-series
        if ( ProjectConfigs.hasTimeSeriesMetrics( config )
             && ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) || hasMetrics( MetricInputGroup.DICHOTOMOUS ) ) )
        {
            throw new MetricConfigException( message );
        }

        // Time-series metrics that are configured as regular metrics, not time-series
        if ( hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES )
             && ( hasMetrics( MetricInputGroup.SINGLE_VALUED ) || hasMetrics( MetricInputGroup.DICHOTOMOUS ) ) )
        {
            throw new MetricConfigException( message );
        }
    }

    @Override
    void completeCachedOutput() throws MetricOutputAccessException
    {
        // Determine whether to compute summary statistics
        boolean proceed = this.hasCachedMetricOutput()
                          && this.getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.PAIRED )
                          && this.getCachedMetricOutputInternal()
                                 .getPairedOutput()
                                 .containsKey( dataFactory.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR ) );    

        // Summary statistics required
        proceed = proceed && Objects.nonNull( this.timeToPeakErrorStats );
        
        // Summary statistics not already computed
        proceed = proceed && ! this.getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.DURATION_SCORE );   
        
        //Add the summary statistics for the cached time-to-peak errors if these statistics do not already exist
        if ( proceed )
        {

            // Obtain the paired output
            MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> output =
                    getCachedMetricOutputInternal().getPairedOutput().get( MetricConstants.TIME_TO_PEAK_ERROR );

            // Iterate through the thresholds
            for ( OneOrTwoThresholds threshold : output.setOfThresholdKey() )
            {
                // Slice  
                MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> sliced =
                        output.filterByThreshold( threshold );
                
                // Find the union of the paired output
                PairedOutput<Instant, Duration> union = dataFactory.unionOf( sliced.values() );

                // Find the union of the metadata
                TimeWindow unionWindow = union.getMetadata().getTimeWindow();

                Pair<TimeWindow, OneOrTwoThresholds> key =
                        Pair.of( unionWindow, threshold );

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
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input
     * pairs, {@link SingleValuedPairs}, using a configured mapping function.
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( TimeWindow timeWindow,
                                          SingleValuedPairs input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDichotomousPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.MATRIX ) )
        {
            processDichotomousPairsByThreshold( timeWindow, input, futures, MetricOutputGroup.MATRIX );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( TimeWindow timeWindow,
                                                     SingleValuedPairs input,
                                                     MetricFuturesByTimeBuilder futures,
                                                     MetricOutputGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.DICHOTOMOUS, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<Threshold> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {

            Threshold useMe = addQuantilesToThreshold( threshold, sorted );
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            //Define a mapper to convert the single-valued pairs to dichotomous pairs
            Function<PairOfDoubles, PairOfBooleans> mapper =
                    pair -> dataFactory.pairOf( useMe.test( pair.getItemOne() ),
                                                useMe.test( pair.getItemTwo() ) );
            //Transform the pairs
            DichotomousPairs transformed = dataFactory.getSlicer().transform( input, mapper );

            processDichotomousPairs( Pair.of( timeWindow, OneOrTwoThresholds.of( useMe ) ),
                                     transformed,
                                     futures,
                                     outGroup,
                                     ignoreTheseMetrics );

        }
    }

    /**
     * Processes a set of metric futures that consume {@link TimeSeriesOfSingleValuedPairs}. 
     * 
     * @param timeWindow the time window
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the output group
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processTimeSeriesPairs( TimeWindow timeWindow,
                                         TimeSeriesOfSingleValuedPairs input,
                                         MetricFuturesByTimeBuilder futures,
                                         MetricOutputGroup outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, outGroup )
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
                final TimeSeriesOfSingleValuedPairs pairs;

                // Filter the data if required
                if ( useMe.isFinite() )
                {
                    Predicate<TimeSeriesOfSingleValuedPairs> filter =
                            MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( useMe );

                    pairs = dataFactory.getSlicer().filter( input, filter, null );
                }
                else
                {
                    pairs = input;
                }

                // Build the future result
                Future<MetricOutputMapByMetric<PairedOutput<Instant, Duration>>> output =
                        CompletableFuture.supplyAsync( () -> timeSeries.apply( pairs, ignoreTheseMetrics ),
                                                       thresholdExecutor );

                // Add the future result to the store
                futures.addPairedOutput( Pair.of( timeWindow, OneOrTwoThresholds.of( threshold ) ),
                                         output );

            }
            // Insufficient data for one threshold: log failures collectively and proceed
            // Such failures are routine and should not be propagated
            catch ( MetricInputSliceException | InsufficientDataException e )
            {
                // Decorate failure
                failures.put( OneOrTwoThresholds.of( useMe ),
                              new MetricCalculationException( "While processing threshold " + threshold + ":", e ) );
            }
        }

        // Log failures collectively at DEBUG level, because time-series metrics are
        // processed iteratively and failures will be extremely common
        logThresholdFailures( Collections.unmodifiableMap( failures ),
                              union.size(),
                              input.getMetadata(),
                              MetricInputGroup.SINGLE_VALUED_TIME_SERIES,
                              Level.DEBUG );

    }

}
