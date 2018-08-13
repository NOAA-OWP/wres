package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Slicer;
import wres.datamodel.inputs.pairs.DichotomousPair;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.outputs.MetricOutputForProject;
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
import wres.engine.statistics.metric.processing.MetricFuturesByTime.MetricFuturesByTimeBuilder;
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
    public MetricOutputForProject apply( SingleValuedPairs input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );

        Objects.requireNonNull( input.getMetadata().getTimeWindow(),
                                "Expected a non-null time window in the input metadata." );

        //Remove missing values, except for ordered input, such as time-series
        SingleValuedPairs inputNoMissing = input;

        if ( ! ( input instanceof TimeSeriesOfSingleValuedPairs ) )
        {
            if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
            {
                throw new MetricCalculationException( " The project configuration includes time-series metrics. "
                                                      + "Expected a time-series of single-valued pairs as input." );
            }
            inputNoMissing = Slicer.filter( input, Slicer.leftAndRight( ADMISSABLE_DATA ), ADMISSABLE_DATA );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();

        //Process the metrics that consume single-valued pairs
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED ) )
        {
            this.processSingleValuedPairs( inputNoMissing, futures );
        }
        if ( this.hasMetrics( MetricInputGroup.DICHOTOMOUS ) )
        {
            this.processDichotomousPairs( inputNoMissing, futures );
        }
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            TimeSeriesOfSingleValuedPairs data = (TimeSeriesOfSingleValuedPairs) inputNoMissing;
            TimeWindow timeWindow = input.getMetadata().getTimeWindow();
            TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
            data = (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( data )
                                                          .setMetadata( Metadata.of( data.getMetadata(),
                                                                                     timeWindow ) )
                                                          .build();

            this.processTimeSeriesPairs( data,
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
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link MetricOutputGroup} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public MetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                   final ThresholdsByMetric externalThresholds,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final Set<MetricOutputGroup> mergeSet )
            throws MetricParameterException
    {
        super( config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );

        //Construct the metrics

        //Time-series 
        if ( this.hasMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ) )
        {
            MetricConstants[] timingErrorMetrics = this.getMetrics( MetricInputGroup.SINGLE_VALUED_TIME_SERIES,
                                                                    MetricOutputGroup.PAIRED );
            this.timeSeries = MetricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
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
                            TimingErrorDurationStatistics.of( identifier, ts );

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
    void validate( ProjectConfig config )
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

            // Iterate through the timing error metrics
            for ( Entry<MetricConstants, TimingErrorDurationStatistics> nextStats : this.timingErrorDurationStatistics.entrySet() )
            {
                // Obtain the output for the current statistic
                ListOfMetricOutput<PairedOutput<Instant, Duration>> output =
                        Slicer.filter( this.getCachedMetricOutputInternal().getPairedOutput(), nextStats.getKey() );

                // Compute the collection of statistics for the next timing error metric
                TimingErrorDurationStatistics timeToPeakErrorStats = nextStats.getValue();

                SortedSet<OneOrTwoThresholds> thresholds =
                        Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );

                // Iterate through the thresholds
                for ( OneOrTwoThresholds threshold : thresholds )
                {
                    // Filter by current threshold  
                    ListOfMetricOutput<PairedOutput<Instant, Duration>> sliced =
                            Slicer.filter( output, next -> next.getThresholds().equals( threshold ) );

                    // Find the union of the paired output
                    PairedOutput<Instant, Duration> union = DataFactory.unionOf( sliced.getData() );

                    //Build the future result
                    Supplier<ListOfMetricOutput<DurationScoreOutput>> supplier = () -> {
                        DurationScoreOutput result = timeToPeakErrorStats.apply( union );
                        List<DurationScoreOutput> input = Collections.singletonList( result );
                        return ListOfMetricOutput.of( input );
                    };

                    // Execute
                    Future<ListOfMetricOutput<DurationScoreOutput>> addMe =
                            CompletableFuture.supplyAsync( supplier, thresholdExecutor );

                    // Add the future result to the store
                    addFutures.addDurationScoreOutput( addMe );
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
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( SingleValuedPairs input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ) )
        {
            processDichotomousPairsByThreshold( input, futures, MetricOutputGroup.DOUBLE_SCORE );
        }
        if ( hasMetrics( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.MATRIX ) )
        {
            processDichotomousPairsByThreshold( input, futures, MetricOutputGroup.MATRIX );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( SingleValuedPairs input,
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
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            //Define a mapper to convert the single-valued pairs to dichotomous pairs
            Function<SingleValuedPair, DichotomousPair> mapper =
                    pair -> DichotomousPair.of( useMe.test( pair.getLeft() ),
                                                useMe.test( pair.getRight() ) );
            //Transform the pairs
            DichotomousPairs transformed = Slicer.toDichotomousPairs( input, mapper );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            Metadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = Metadata.of( transformed.getMetadataForBaseline(), oneOrTwo );
            }

            transformed = DichotomousPairs.ofDichotomousPairs( transformed,
                                                               Metadata.of( transformed.getMetadata(), oneOrTwo ),
                                                               baselineMeta );

            this.processDichotomousPairs( transformed,
                                          futures,
                                          outGroup,
                                          ignoreTheseMetrics );

        }
    }

    /**
     * Processes a set of metric futures that consume {@link TimeSeriesOfSingleValuedPairs}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the output group
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processTimeSeriesPairs( TimeSeriesOfSingleValuedPairs input,
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
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            TimeSeriesOfSingleValuedPairs pairs;

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<TimeSeries<SingleValuedPair>> filter =
                        MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( useMe );

                pairs = Slicer.filter( input, filter, null );
            }
            else
            {
                pairs = input;
            }

            // Add the threshold to the metadata, in order to fully qualify the pairs
            Metadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = Metadata.of( pairs.getMetadataForBaseline(), oneOrTwo );
            }

            TimeSeriesOfSingleValuedPairsBuilder builder = new TimeSeriesOfSingleValuedPairsBuilder();
            pairs = (TimeSeriesOfSingleValuedPairs) builder.addTimeSeries( pairs )
                                                           .setMetadata( Metadata.of( pairs.getMetadata(),
                                                                                      oneOrTwo ) )
                                                           .setMetadataForBaseline( baselineMeta )
                                                           .build();

            // Build the future result
            final TimeSeriesOfSingleValuedPairs finalPairs = pairs;
            Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>> output =
                    CompletableFuture.supplyAsync( () -> timeSeries.apply( finalPairs, ignoreTheseMetrics ),
                                                   thresholdExecutor );

            // Add the future result to the store
            futures.addPairedOutput( output );
        }

    }

}
