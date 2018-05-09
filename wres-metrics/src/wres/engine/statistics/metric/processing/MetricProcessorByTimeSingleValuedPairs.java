package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Map.Entry;
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

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;
import wres.engine.statistics.metric.processing.MetricProcessorByTime.MetricFuturesByTime.MetricFuturesByTimeBuilder;
import wres.engine.statistics.metric.timeseries.TimingErrorDurationStatistics;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link SingleValuedPairs} and configured transformations of {@link SingleValuedPairs}. For example, metrics that
 * consume {@link DichotomousPairs} may be processed after transforming the {@link SingleValuedPairs} with an
 * appropriate mapping function.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricProcessorByTimeSingleValuedPairs extends MetricProcessorByTime<SingleValuedPairs>
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link TimeSeriesOfSingleValuedPairs} and produce
     * {@link PairedOutput}.
     */

    private final MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>> timeSeries;

    /**
     * An instance of {@link TimingErrorDurationStatistics} for each timing error metric that requires 
     * summary statistics.
     */

    private final Map<MetricConstants, TimingErrorDurationStatistics> timingErrorDurationStatistics;

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

        if ( ! ( input instanceof TimeSeriesOfSingleValuedPairs ) )
        {
            if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
            {
                throw new MetricCalculationException( " The project configuration includes time-series metrics. "
                                                      + "Expected a time-series of single-valued pairs as input." );
            }
            inputNoMissing = slicer.filter( input, Slicer.leftAndRight( ADMISSABLE_DATA ), ADMISSABLE_DATA );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();
        futures.setDataFactory( dataFactory );

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
            // For time-series inputs, use the actual time period as the time window; there is no need to trust the 
            // metadata. This should avoid unnecessary merge exceptions when the caller is using the same metadata to
            // identify different data across multiple, incremental, calls. Such calls are common for time-series.
            TimeSeriesOfSingleValuedPairs data = (TimeSeriesOfSingleValuedPairs) inputNoMissing;
            TimeWindow actualTimeWindow = TimeWindow.of( data.getEarliestBasisTime(),
                                                         data.getLatestBasisTime(),
                                                         timeWindow.getReferenceTime(),
                                                         timeWindow.getEarliestLeadTime(),
                                                         timeWindow.getLatestLeadTime() );

            this.processTimeSeriesPairs( actualTimeWindow,
                                         data,
                                         futures,
                                         MetricOutputGroup.PAIRED );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      input.getMetadata().getIdentifier().getGeospatialID(),
                      input.getMetadata().getTimeWindow() );

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
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ) )
        {
            MetricConstants[] timingErrorMetrics = this.getMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES,
                                                                    MetricOutputGroup.PAIRED );
            this.timeSeries = metricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
                                                                                timingErrorMetrics );
            //Summary statistics
            Map<MetricConstants, TimingErrorDurationStatistics> localStatistics =
                    new EnumMap<>( MetricConstants.class );

            // Iterate the timing error metrics
            for ( MetricConstants nextMetric : timingErrorMetrics )
            {

                if ( MetricConfigHelper.hasSummaryStatisticsFor( config,
                                                                 name -> nextMetric.name().equals( name.name() ) ) )
                {
                    Set<MetricConstants> ts = MetricConfigHelper.getSummaryStatisticsFor( config,
                                                                                          name -> nextMetric.name()
                                                                                                            .equals( name.name() ) );

                    // Find the identifier for the summary statistics
                    MetricConstants identifier = MetricFactory.ofSummaryStatisticsForTimingErrorMetric( nextMetric );

                    TimingErrorDurationStatistics stats =
                            this.metricFactory.ofTimingErrorDurationStatistics( identifier, ts );

                    localStatistics.put( nextMetric, stats );
                }
            }

            this.timingErrorDurationStatistics = Collections.unmodifiableMap( localStatistics );
        }
        else
        {
            this.timeSeries = null;
            this.timingErrorDurationStatistics = Collections.unmodifiableMap( new EnumMap<>( MetricConstants.class ) );
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
             && ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED )
                  || this.hasMetrics( MetricInputGroup.DICHOTOMOUS ) ) )
        {
            throw new MetricConfigException( message );
        }
    }

    @Override
    void completeCachedOutput() throws InterruptedException
    {
        // Determine whether to compute summary statistics
        boolean proceed = this.hasCachedMetricOutput()
                          && this.getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.PAIRED );

        // Summary statistics not already computed
        proceed = proceed && !this.getCachedMetricOutputInternal().hasOutput( MetricOutputGroup.DURATION_SCORE );

        //Add the summary statistics for the cached time-to-peak errors if these statistics do not already exist
        if ( proceed )
        {

            MetricFuturesByTimeBuilder addFutures = new MetricFuturesByTimeBuilder();
            addFutures.setDataFactory( dataFactory );

            // Iterate through the timing error metrics
            for ( Entry<MetricConstants, TimingErrorDurationStatistics> nextStats : this.timingErrorDurationStatistics.entrySet() )
            {
                // Output available
                if ( this.getCachedMetricOutputInternal()
                         .getPairedOutput()
                         .containsKey( dataFactory.getMapKey( nextStats.getKey() ) ) )
                {
                    // Obtain the paired output
                    MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> output =
                            getCachedMetricOutputInternal().getPairedOutput().get( nextStats.getKey() );

                    // Compute the collection of statistics for the next timing error metric
                    TimingErrorDurationStatistics timeToPeakErrorStats = nextStats.getValue();

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
                            DurationScoreOutput result = timeToPeakErrorStats.apply( union );
                            Map<MetricConstants, DurationScoreOutput> input =
                                    Collections.singletonMap( result.getMetadata().getMetricID(), result );
                            return dataFactory.ofMetricOutputMapByMetric( input );
                        };

                        // Execute
                        Future<MetricOutputMapByMetric<DurationScoreOutput>> addMe =
                                CompletableFuture.supplyAsync( supplier, thresholdExecutor );

                        // Add the future result to the store
                        addFutures.addDurationScoreOutput( key, addMe );
                    }
                }
            }

            // Build the store of futures
            futures.add( addFutures.build() );
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

        // Iterate the thresholds
        for ( Threshold threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            Threshold useMe = this.addQuantilesToThreshold( threshold, sorted );

            final TimeSeriesOfSingleValuedPairs pairs;

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<TimeSeries<PairOfDoubles>> filter =
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

    }

}
