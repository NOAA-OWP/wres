package wres.pipeline.statistics;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeSeries;
import wres.engine.statistics.metric.Metric;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.pipeline.statistics.MetricFuturesByTime.MetricFuturesByTimeBuilder;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * single-valued pairs and configured transformations thereof. For example, metrics that consume dichotomous pairs may 
 * be processed after transforming the single-valued pairs with an appropriate mapping function.
 * 
 * @author James Brown
 */

public class MetricProcessorByTimeSingleValuedPairs
        extends MetricProcessorByTime<Pool<TimeSeries<Pair<Double, Double>>>>
{

    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessorByTimeSingleValuedPairs.class );

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs 
     * and produce {@link DurationDiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter> timeSeries;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs 
     * and produce {@link DurationScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter> timeSeriesStatistics;

    @Override
    public StatisticsForProject apply( Pool<TimeSeries<Pair<Double, Double>>> pool )
    {
        Objects.requireNonNull( pool, "Expected non-null input to the metric processor." );

        Objects.requireNonNull( pool.getMetadata().getTimeWindow(),
                                "Expected a non-null time window in the input metadata." );

        //Remove missing values from pairs that do not preserver time order
        Pool<Pair<Double, Double>> unpackedNoMissing = null;
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) || this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            LOGGER.debug( "Removing any single-valued pairs with missing left or right values for pool {}.",
                          pool.getMetadata() );
            Pool<Pair<Double, Double>> unpacked = PoolSlicer.unpack( pool );
            unpackedNoMissing = PoolSlicer.filter( unpacked,
                                                   Slicer.leftAndRight( MetricProcessor.ADMISSABLE_DATA ),
                                                   MetricProcessor.ADMISSABLE_DATA );
        }

        //Metric futures 
        MetricFuturesByTimeBuilder futures = new MetricFuturesByTimeBuilder();

        //Process the metrics that consume single-valued pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) )
        {
            super.processSingleValuedPairs( unpackedNoMissing, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            this.processDichotomousPairs( unpackedNoMissing, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            this.processTimeSeriesPairs( pool,
                                         futures,
                                         StatisticType.DURATION_DIAGRAM );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      MessageFactory.parse( pool.getMetadata().getPool().getGeometryTuples( 0 ) ),
                      pool.getMetadata().getTimeWindow() );

        //Process and return the result       
        MetricFuturesByTime futureResults = futures.build();

        return futureResults.getMetricOutput();
    }

    /**
     * Hidden constructor.
     * 
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public MetricProcessorByTimeSingleValuedPairs( ThresholdsByMetricAndFeature metrics,
                                                   ExecutorService thresholdExecutor,
                                                   ExecutorService metricExecutor )
    {
        super( metrics, thresholdExecutor, metricExecutor );

        //Construct the metrics
        //Time-series 
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ) )
        {
            MetricConstants[] timingErrorMetrics = this.getMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                                    StatisticType.DURATION_DIAGRAM );
            this.timeSeries = MetricFactory.ofSingleValuedTimeSeriesCollection( metricExecutor,
                                                                                timingErrorMetrics );

            LOGGER.debug( "Created the timing-error metrics for processing. {}", this.timeSeries );
        }
        else
        {
            this.timeSeries = null;
        }

        //Time-series summary statistics
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_SCORE ) )
        {
            Map<MetricConstants, Set<MetricConstants>> localStatistics =
                    new EnumMap<>( MetricConstants.class );

            MetricConstants[] timingErrorSummaryMetrics = this.getMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                                           StatisticType.DURATION_SCORE );

            for ( MetricConstants next : timingErrorSummaryMetrics )
            {
                MetricConstants parent = next.getParent();

                if ( Objects.isNull( parent ) )
                {
                    throw new MetricConfigException( "The timing error summary statistic '" + next
                                                     + "' does not have a parent metric set, which is not allowed." );
                }

                Set<MetricConstants> nextStats = localStatistics.get( parent );
                if ( Objects.isNull( nextStats ) )
                {
                    nextStats = new HashSet<>();
                    localStatistics.put( parent, nextStats );
                }

                nextStats.add( next );
            }

            this.timeSeriesStatistics = MetricFactory.ofSummaryStatisticsForTimingErrorMetrics( metricExecutor,
                                                                                                localStatistics );

        }
        else
        {
            this.timeSeriesStatistics = null;
        }

        // Validate the state
        this.validate();
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
     * @param pool the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( Pool<Pair<Double, Double>> pool,
                                                     MetricFuturesByTimeBuilder futures,
                                                     StatisticType outGroup )
    {
        // Filter the thresholds for this group and for the required types
        Map<FeatureTuple, ThresholdsByMetric> filtered = super.getMetrics().getThresholdsByMetricAndFeature();
        filtered = ThresholdSlicer.filterByGroup( filtered,
                                                  SampleDataGroup.DICHOTOMOUS,
                                                  outGroup,
                                                  ThresholdGroup.PROBABILITY,
                                                  ThresholdGroup.VALUE );

        // Unpack the thresholds and add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> unpacked = ThresholdSlicer.unpack( filtered );
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles =
                ThresholdSlicer.addQuantiles( unpacked, pool, PoolSlicer.getFeatureMapper() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );
        
        // Decompose the thresholds by common type across features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<ThresholdOuter, Function<Pair<Double, Double>, Pair<Boolean, Boolean>>> transformerGenerator =
                threshold -> pair -> Pair.of( threshold.test( pair.getLeft() ),
                                              threshold.test( pair.getRight() ) );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Function<Pair<Double, Double>, Pair<Boolean, Boolean>>> transformers =
                    ThresholdSlicer.getTransformersFromThresholds( thresholds,
                                                                   transformerGenerator );

            //Transform the pairs
            Pool<Pair<Boolean, Boolean>> transformed = PoolSlicer.transform( pool,
                                                                             transformers,
                                                                             PoolSlicer.getFeatureMapper() );
            
            // Add the threshold to the metadata
            ThresholdOuter composed = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            transformed = this.addThresholdToPoolMetadata( transformed, OneOrTwoThresholds.of( composed ) );

            super.processDichotomousPairs( transformed,
                                           futures,
                                           outGroup );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link Pool} with single-valued pairs. 
     * 
     * @param pool the input pairs
     * @param futures the metric futures
     * @param outGroup the output group
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processTimeSeriesPairs( Pool<TimeSeries<Pair<Double, Double>>> pool,
                                         MetricFuturesByTimeBuilder futures,
                                         StatisticType outGroup )
    {
        // Filter the thresholds for this group and for the required types
        Map<FeatureTuple, ThresholdsByMetric> filtered = super.getMetrics().getThresholdsByMetricAndFeature();
        filtered = ThresholdSlicer.filterByGroup( filtered,
                                                  SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                  outGroup,
                                                  ThresholdGroup.PROBABILITY,
                                                  ThresholdGroup.VALUE );

        // Unpack the thresholds and add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> unpacked = ThresholdSlicer.unpack( filtered );
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles =
                ThresholdSlicer.addQuantiles( unpacked, pool, PoolSlicer.getFeatureMapper() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );
        
        // Decompose the thresholds by common type across features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Predicate<TimeSeries<Pair<Double, Double>>>> slicers =
                    ThresholdSlicer.getFiltersFromThresholds( thresholds,
                                                              MetricProcessorByTime::getFilterForTimeSeriesOfSingleValuedPairs );

            Pool<TimeSeries<Pair<Double, Double>>> sliced = PoolSlicer.filter( pool, 
                                                                               slicers, 
                                                                               PoolSlicer.getFeatureMapper() );
            
            // Add the threshold to the metadata
            ThresholdOuter composed = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            sliced = this.addThresholdToPoolMetadata( sliced, OneOrTwoThresholds.of( composed ) );

            // Build the future result
            Future<List<DurationDiagramStatisticOuter>> output = this.processTimeSeriesPairs( sliced,
                                                                                              this.timeSeries );

            // Add the future result to the store
            futures.addDurationDiagramOutput( output );

            // Summary statistics?
            if ( Objects.nonNull( this.timeSeriesStatistics ) )
            {
                Future<List<DurationScoreStatisticOuter>> summary = this.processTimeSeriesSummaryPairs( sliced,
                                                                                                        this.timeSeriesStatistics );

                futures.addDurationScoreOutput( summary );
            }
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes single-valued pairs and produces 
     * {@link DurationDiagramStatisticOuter}.
     * 
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @return the future result
     */

    private Future<List<DurationDiagramStatisticOuter>>
            processTimeSeriesPairs( Pool<TimeSeries<Pair<Double, Double>>> pairs,
                                    MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get().size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While processing time-series for pool {}, discovered {} time-series, which is fewer "
                              + "than the minimum sample size of {} time-series. The following metrics will not be "
                              + "computed for this pool: {}.",
                              pairs.getMetadata(),
                              pairs.get().size(),
                              minimumSampleSize,
                              collection.getMetrics() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        return this.processMetricsRequiredForThisPool( pairs, collection );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes single-valued pairs and produces 
     * {@link DurationScoreStatisticOuter}.
     * 
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @return the future result
     */

    private Future<List<DurationScoreStatisticOuter>>
            processTimeSeriesSummaryPairs( Pool<TimeSeries<Pair<Double, Double>>> pairs,
                                           MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get().size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While processing time-series for pool {}, discovered {} time-series, which is fewer "
                              + "than the minimum sample size of {} time-series. The following metrics will not be "
                              + "computed for this pool: {}.",
                              pairs.getMetadata(),
                              pairs.get().size(),
                              minimumSampleSize,
                              collection.getMetrics() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        return this.processMetricsRequiredForThisPool( pairs, collection );
    }

    /**
     * Validates the state of the processor.
     * @throw MetricConfigException if the state is invalid for any reason
     */

    private void validate()
    {
        //Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : super.getMetrics().getMetrics() )
        {
            if ( ! ( next.isInGroup( SampleDataGroup.SINGLE_VALUED )
                     || next.isInGroup( SampleDataGroup.SINGLE_VALUED_TIME_SERIES )
                     || next.isInGroup( SampleDataGroup.DICHOTOMOUS ) ) )
            {
                throw new MetricConfigException( "Cannot configure '"
                                                 + next
                                                 + "' for single-valued inputs: correct the configuration." );
            }

            // Thresholds required for dichotomous metrics
            if ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                 && super.getMetrics().getThresholdsByMetricAndFeature()
                                      .values()
                                      .stream()
                                      .noneMatch( thresholds -> thresholds.hasThresholdsForThisMetricAndTheseTypes( next,
                                                                                                                    ThresholdGroup.PROBABILITY,
                                                                                                                    ThresholdGroup.VALUE ) ) )
            {
                throw new MetricConfigException( "Cannot configure '"
                                                 + next
                                                 + "' without thresholds to define the events: add one "
                                                 + "or more thresholds to the configuration for each instance of '"
                                                 + next
                                                 + "'." );
            }
        }
    }

}