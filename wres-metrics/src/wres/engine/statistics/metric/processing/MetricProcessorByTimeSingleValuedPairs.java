package wres.engine.statistics.metric.processing;

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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigs;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
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
 * single-valued pairs and configured transformations thereof. For example, metrics that consume dichotomous pairs may 
 * be processed after transforming the single-valued pairs with an appropriate mapping function.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricProcessorByTimeSingleValuedPairs extends MetricProcessorByTime<Pool<Pair<Double, Double>>>
{

    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessorByTimeSingleValuedPairs.class );

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs 
     * and produce {@link DurationDiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Double>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter> timeSeries;

    /**
     * An instance of {@link TimingErrorDurationStatistics} for each timing error metric that requires 
     * summary statistics.
     */

    private final Map<MetricConstants, TimingErrorDurationStatistics> timingErrorDurationStatistics;

    @Override
    public StatisticsForProject apply( Pool<Pair<Double, Double>> input )
    {
        Objects.requireNonNull( input, "Expected non-null input to the metric processor." );

        Objects.requireNonNull( input.getMetadata().getTimeWindow(),
                                "Expected a non-null time window in the input metadata." );

        //Remove missing values, except for ordered input, such as time-series
        Pool<Pair<Double, Double>> inputNoMissing = input;

        if ( !this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            LOGGER.debug( "Removing any single-valued pairs with missing left or right values for feature {} at "
                          + "time window {}, since time-series metrics are not required.",
                          MessageFactory.parse( input.getMetadata().getPool().getGeometryTuples( 0 ) ),
                          inputNoMissing.getMetadata().getTimeWindow() );
            
            inputNoMissing = TimeSeriesSlicer.filter( input,
                                                      Slicer.leftAndRight( MetricProcessor.ADMISSABLE_DATA ),
                                                      MetricProcessor.ADMISSABLE_DATA );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();

        //Process the metrics that consume single-valued pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) )
        {
            this.processSingleValuedPairs( inputNoMissing, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            this.processDichotomousPairs( inputNoMissing, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            this.processTimeSeriesPairs( inputNoMissing,
                                         futures,
                                         StatisticType.DURATION_DIAGRAM );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      MessageFactory.parse( input.getMetadata().getPool().getGeometryTuples( 0 ) ),
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
     * @param mergeSet a list of {@link StatisticType} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public MetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                   final ThresholdsByMetric externalThresholds,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final Set<StatisticType> mergeSet )
            throws MetricParameterException
    {
        super( config, externalThresholds, thresholdExecutor, metricExecutor, mergeSet );

        //Construct the metrics

        //Time-series 
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ) )
        {
            MetricConstants[] timingErrorMetrics = this.getMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                                    StatisticType.DURATION_DIAGRAM );
            this.timeSeries = MetricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
                                                                                timingErrorMetrics );

            LOGGER.debug( "Created the timing-error metrics for processing. {}", this.timeSeries );

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
        Objects.requireNonNull( config, MetricConfigHelper.NULL_CONFIGURATION_ERROR );

        // Annotate any configuration error, if possible
        String configurationLabel = ".";
        if ( Objects.nonNull( config.getLabel() ) )
        {
            configurationLabel = " labelled '"
                                 + config.getLabel()
                                 + "'.";
        }

        //Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : this.metrics )
        {
            if ( ! ( next.isInGroup( SampleDataGroup.SINGLE_VALUED )
                     || next.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES )
                     || next.isInGroup( SampleDataGroup.DICHOTOMOUS ) ) )
            {
                throw new MetricConfigException( "Cannot configure '" + next
                                                 + "' for single-valued inputs: correct the configuration"
                                                 + configurationLabel );
            }

            // Thresholds required for dichotomous metrics
            if ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                 && !this.getThresholdsByMetric()
                         .hasThresholdsForThisMetricAndTheseTypes( next,
                                                                   ThresholdGroup.PROBABILITY,
                                                                   ThresholdGroup.VALUE ) )
            {
                throw new MetricConfigException( "Cannot configure '" + next
                                                 + "' without thresholds to define the events: add one "
                                                 + "or more thresholds to the configuration"
                                                 + configurationLabel );
            }

        }

        // Check that time-series metrics are not combined with other metrics
        String message = "Cannot configure time-series metrics together with non-time-series "
                         + "metrics: correct the configuration"
                         + configurationLabel;

        // Metrics that are explicitly configured as time-series
        if ( ProjectConfigs.hasTimeSeriesMetrics( config )
             && ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED )
                  || this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) ) )
        {
            throw new MetricConfigException( message );
        }
    }

    @Override
    void completeCachedOutput() throws InterruptedException
    {
        // Determine whether to compute summary statistics
        boolean proceed = this.hasCachedMetricOutput()
                          && this.getCachedMetricOutputInternal().hasStatistic( StatisticType.DURATION_DIAGRAM );

        // Summary statistics not already computed
        proceed = proceed && !this.getCachedMetricOutputInternal().hasStatistic( StatisticType.DURATION_SCORE );

        //Add the summary statistics for the cached time-to-peak errors if these statistics do not already exist
        if ( proceed )
        {

            MetricFuturesByTimeBuilder addFutures = new MetricFuturesByTimeBuilder();

            // Iterate through the timing error metrics
            for ( Entry<MetricConstants, TimingErrorDurationStatistics> nextStats : this.timingErrorDurationStatistics.entrySet() )
            {
                // Obtain the output for the current statistic
                List<DurationDiagramStatisticOuter> output =
                        Slicer.filter( this.getCachedMetricOutputInternal().getInstantDurationPairStatistics(),
                                       nextStats.getKey() );

                // Compute the collection of statistics for the next timing error metric
                TimingErrorDurationStatistics timeToPeakErrorStats = nextStats.getValue();

                SortedSet<OneOrTwoThresholds> thresholds =
                        Slicer.discover( output, meta -> meta.getMetadata().getThresholds() );

                // Iterate through the thresholds
                for ( OneOrTwoThresholds threshold : thresholds )
                {
                    // Filter by current threshold  
                    List<DurationDiagramStatisticOuter> sliced =
                            Slicer.filter( output,
                                           next -> next.getMetadata().getThresholds().equals( threshold ) );

                    // Find the union of the paired output
                    DurationDiagramStatisticOuter union = DataFactory.unionOf( sliced );

                    //Build the future result
                    Supplier<List<DurationScoreStatisticOuter>> supplier = () -> {
                        DurationScoreStatisticOuter result = timeToPeakErrorStats.apply( union );
                        return Collections.singletonList( result );
                    };

                    // Execute
                    Future<List<DurationScoreStatisticOuter>> addMe =
                            CompletableFuture.supplyAsync( supplier, thresholdExecutor );

                    // Add the future result to the store
                    addFutures.addDurationScoreOutput( addMe );
                }
            }

            // Build the store of futures
            this.futures.add( addFutures.build() );
        }
    }

    /**
     * Processes a set of metric futures that consume dichotomous pairs, which are mapped from single-valued pairs 
     * using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairs( Pool<Pair<Double, Double>> input,
                                          MetricFuturesByTimeBuilder futures )
    {
        if ( hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            this.processDichotomousPairsByThreshold( input, futures, StatisticType.DOUBLE_SCORE );
        }
    }

    /**
     * Processes a set of metric futures that consume dichotomous pairs, which are mapped from single-valued pairs 
     * using a configured mapping function. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( Pool<Pair<Double, Double>> input,
                                                     MetricFuturesByTimeBuilder futures,
                                                     StatisticType outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( SampleDataGroup.DICHOTOMOUS, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<ThresholdOuter> union = filtered.union();

        double[] sorted = this.getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {

            ThresholdOuter useMe = this.addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            //Define a mapper to convert the single-valued pairs to dichotomous pairs
            Function<Pair<Double, Double>, Pair<Boolean, Boolean>> mapper =
                    pair -> Pair.of( useMe.test( pair.getLeft() ),
                                     useMe.test( pair.getRight() ) );
            //Transform the pairs
            Pool<Pair<Boolean, Boolean>> transformed = Slicer.transform( input, mapper );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            PoolMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = PoolMetadata.of( transformed.getBaselineData().getMetadata(), oneOrTwo );
            }

            BasicPool.Builder<Pair<Boolean, Boolean>> builder = new BasicPool.Builder<>();

            transformed = builder.addData( transformed )
                                 .setMetadata( PoolMetadata.of( input.getMetadata(), oneOrTwo ) )
                                 .setMetadataForBaseline( baselineMeta )
                                 .build();

            this.processDichotomousPairs( transformed,
                                          futures,
                                          outGroup,
                                          ignoreTheseMetrics );

        }
    }

    /**
     * Processes a set of metric futures that consume {@link Pool} with single-valued pairs. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the output group
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processTimeSeriesPairs( Pool<Pair<Double, Double>> input,
                                         MetricFuturesByTimeBuilder futures,
                                         StatisticType outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = this.getThresholdsByMetric()
                                          .filterByGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, outGroup )
                                          .filterByType( ThresholdGroup.PROBABILITY, ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<ThresholdOuter> union = filtered.union();

        double[] sorted = getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {
            Set<MetricConstants> ignoreTheseMetrics = filtered.doesNotHaveTheseMetricsForThisThreshold( threshold );

            // Add quantiles to threshold
            ThresholdOuter useMe = this.addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            Pool<Pair<Double, Double>> pairs;

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<TimeSeries<Pair<Double, Double>>> filter =
                        MetricProcessorByTime.getFilterForTimeSeriesOfSingleValuedPairs( useMe );

                pairs = TimeSeriesSlicer.filterPerSeries( input, filter, null );
            }
            else
            {
                pairs = input;
            }

            // Add the threshold to the metadata, in order to fully qualify the pairs
            PoolMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = PoolMetadata.of( pairs.getBaselineData().getMetadata(), oneOrTwo );
            }

            PoolOfPairs.Builder<Double, Double> builder = new PoolOfPairs.Builder<>();
            pairs = builder.addPoolOfPairs( pairs )
                           .setMetadata( PoolMetadata.of( pairs.getMetadata(), oneOrTwo ) )
                           .setMetadataForBaseline( baselineMeta )
                           .build();

            // Build the future result
            final Pool<Pair<Double, Double>> finalPairs = pairs;
            Future<List<DurationDiagramStatisticOuter>> output =
                    CompletableFuture.supplyAsync( () -> this.timeSeries.apply( finalPairs, ignoreTheseMetrics ),
                                                   this.thresholdExecutor );
            
            // Add the future result to the store
            futures.addDurationDiagramOutput( output );
        }

    }

}
