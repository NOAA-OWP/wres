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
import java.util.function.UnaryOperator;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.config.DeclarationException;
import wres.config.components.ThresholdType;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.Metric;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricCollection;
import wres.metrics.MetricFactory;
import wres.metrics.MetricParameterException;

/**
 * Builds and processes all {@link MetricCollection} associated with an evaluation for metrics that consume
 * single-valued pairs and configured transformations thereof. For example, metrics that consume dichotomous pairs may 
 * be processed after transforming the single-valued pairs with an appropriate mapping function.
 *
 * @author James Brown
 */

public class SingleValuedStatisticsProcessor extends StatisticsProcessor<Pool<TimeSeries<Pair<Double, Double>>>>
{
    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SingleValuedStatisticsProcessor.class );

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs 
     * and produce {@link DurationDiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
            timeSeries;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs 
     * and produce {@link DurationScoreStatisticOuter}.
     */

    private final MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter>
            timeSeriesStatistics;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce {@link ScoreStatistic}.
     */

    private final MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            singleValuedScore;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce 
     * {@link DiagramStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter>
            singleValuedDiagrams;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume single-valued pairs and produce
     * {@link BoxplotStatisticOuter}.
     */

    private final MetricCollection<Pool<Pair<Double, Double>>, BoxplotStatisticOuter, BoxplotStatisticOuter>
            singleValuedBoxPlot;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume a {@link Pool} with single-valued pairs
     * and produce {@link PairsStatisticOuter}.
     */

    private final MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, PairsStatisticOuter, PairsStatisticOuter>
            singleValuedPairs;

    /** Small sample size message for logging. */
    private static final String SMALL_SAMPLE_MESSAGE = "While processing time-series for pool {}, discovered {} "
                                                       + "time-series, which is fewer than the minimum sample size of "
                                                       + "{} time-series. The following metrics will not be computed "
                                                       + "for this pool: {}.";

    /**
     * Constructor.
     *
     * @param metricsAndThresholds the metrics and thresholds
     * @param slicingExecutor an {@link ExecutorService} for executing slicing and dicing of pools, cannot be null
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws DeclarationException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    public SingleValuedStatisticsProcessor( MetricsAndThresholds metricsAndThresholds,
                                            ExecutorService slicingExecutor,
                                            ExecutorService metricExecutor )
    {
        super( metricsAndThresholds, slicingExecutor, metricExecutor );

        // Construct the metrics
        // Time-series
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ) )
        {
            MetricConstants[] timingErrorMetrics = this.getMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                                    StatisticType.DURATION_DIAGRAM );
            this.timeSeries = MetricFactory.ofSingleValuedTimeSeriesMetrics( metricExecutor,
                                                                             timingErrorMetrics );

            LOGGER.debug( "Created the timing-error metrics for processing. {}", this.timeSeries );
        }
        else
        {
            this.timeSeries = null;
        }

        // Pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.PAIRS ) )
        {
            MetricConstants[] pairsMetrics = this.getMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                              StatisticType.PAIRS );
            this.singleValuedPairs = MetricFactory.ofSingleValuedPairsMetrics( metricExecutor,
                                                                               pairsMetrics );

            LOGGER.debug( "Created the single-valued pairs metrics for processing. {}", this.singleValuedPairs );
        }
        else
        {
            this.singleValuedPairs = null;
        }

        // Time-series summary statistics
        this.timeSeriesStatistics = this.getTimeSeriesSummaryStatistics( metricExecutor );

        // Construct the metrics that are common to more than one type of input pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) )
        {
            MetricConstants[] scores = this.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE );
            this.singleValuedScore = MetricFactory.ofSingleValuedScores( metricExecutor, scores );

            LOGGER.debug( "Created the single-valued scores for processing. {}", this.singleValuedScore );
        }
        else
        {
            this.singleValuedScore = null;
        }

        // Diagrams
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM ) )
        {
            MetricConstants[] diagrams = this.getMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM );
            this.singleValuedDiagrams = MetricFactory.ofSingleValuedDiagrams( metricExecutor, diagrams );

            LOGGER.debug( "Created the single-valued diagrams for processing. {}", this.singleValuedDiagrams );
        }
        else
        {
            this.singleValuedDiagrams = null;
        }

        // Box plots
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ) )
        {
            MetricConstants[] boxplots = this.getMetrics( SampleDataGroup.SINGLE_VALUED,
                                                          StatisticType.BOXPLOT_PER_POOL );
            this.singleValuedBoxPlot = MetricFactory.ofSingleValuedBoxplots( metricExecutor, boxplots );

            LOGGER.debug( "Created the single-valued box plots for processing. {}", this.singleValuedBoxPlot );
        }
        else
        {
            this.singleValuedBoxPlot = null;
        }

        // Validate the state
        this.validate();
    }

    @Override
    public StatisticsStore apply( Pool<TimeSeries<Pair<Double, Double>>> pool )
    {
        Objects.requireNonNull( pool, "Expected a non-null pool as input to the metric processor." );

        Objects.requireNonNull( pool.getMetadata()
                                    .getTimeWindow(),
                                "Expected a non-null time window in the pool metadata." );

        LOGGER.debug( "Computing single-valued statistics for pool: {}.", pool.getMetadata() );

        // Metrics to compute?
        if ( !this.hasMetrics( SampleDataGroup.SINGLE_VALUED )
             && !this.hasMetrics( SampleDataGroup.DICHOTOMOUS )
             && !this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            LOGGER.debug( "There were no single-valued or dichotomous statistics to compute." );

            return new StatisticsStore.Builder().build();
        }

        LOGGER.debug( "Computing single-valued statistics from {} time-series.",
                      pool.get()
                          .size() );

        // Unpack pairs as needed
        Pool<Pair<Double, Double>> unpacked = null;
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED )
             || this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            unpacked = PoolSlicer.unpack( pool );
        }

        // Metric futures
        StatisticsStore.Builder futures = new StatisticsStore.Builder();

        // Process the metrics that consume single-valued pairs
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED ) )
        {
            this.processSingleValuedPairs( unpacked, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS ) )
        {
            this.processPairsForDichotomousMetrics( unpacked, futures );
        }
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
        {
            this.processTimeSeriesPairs( pool, futures );
        }

        // Log
        LOGGER.debug( PROCESSING_COMPLETE_MESSAGE,
                      pool.getMetadata().getFeatureGroup(),
                      pool.getMetadata().getTimeWindow() );

        // Process and return the result
        return futures.build();
    }

    /**
     * Helper that returns a predicate for filtering single-valued pairs based on the 
     * {@link ThresholdOuter#getOrientation()} of the input threshold.
     *
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getOrientation()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getOrientation()} is not recognized
     */

    private static Predicate<Pair<Double, Double>> getFilterForSingleValuedPairs( ThresholdOuter threshold )
    {
        return switch ( threshold.getOrientation() )
        {
            case OBSERVED -> Slicer.left( threshold );
            case OBSERVED_AND_PREDICTED, OBSERVED_AND_ANY_PREDICTED, OBSERVED_AND_PREDICTED_MEAN ->
                    Slicer.leftAndRight( threshold );
            case PREDICTED, ANY_PREDICTED, PREDICTED_MEAN -> Slicer.right( threshold );
        };
    }

    /**
     * Helper that returns a predicate for filtering time-series of single-valued pairs based on the
     * {@link ThresholdOuter#getOrientation()} of the input threshold.
     *
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getOrientation()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getOrientation()} is not recognized
     */

    private static Predicate<TimeSeries<Pair<Double, Double>>>
    getFilterForTimeSeriesOfSingleValuedPairs( ThresholdOuter threshold )
    {
        return switch ( threshold.getOrientation() )
        {
            case OBSERVED -> TimeSeriesSlicer.anyOfLeftInTimeSeries( threshold::test );
            case OBSERVED_AND_PREDICTED, OBSERVED_AND_ANY_PREDICTED, OBSERVED_AND_PREDICTED_MEAN ->
                    TimeSeriesSlicer.anyOfLeftAndAnyOfRightInTimeSeries( threshold::test );
            case PREDICTED, ANY_PREDICTED, PREDICTED_MEAN -> TimeSeriesSlicer.anyOfRightInTimeSeries( threshold::test );
        };
    }

    /**
     * Processes a set of metric futures for single-valued pairs.
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairs( Pool<Pair<Double, Double>> pool,
                                           StatisticsStore.Builder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) )
        {
            this.processSingleValuedPairsByThreshold( pool, futures, StatisticType.DOUBLE_SCORE );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM ) )
        {
            this.processSingleValuedPairsByThreshold( pool, futures, StatisticType.DIAGRAM );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ) )
        {
            this.processSingleValuedPairsByThreshold( pool, futures, StatisticType.BOXPLOT_PER_POOL );
        }
    }

    /**
     * Processes all thresholds for metrics that consume single-valued pairs and produce a specified 
     * {@link StatisticType}. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairsByThreshold( Pool<Pair<Double, Double>> pool,
                                                      StatisticsStore.Builder futures,
                                                      StatisticType outGroup )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholds();
        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        if ( filteredThresholds.isEmpty() )
        {
            throw new MetricCalculationException( "Could not find any thresholds for feature tuples within feature "
                                                  + "group "
                                                  + featureGroup
                                                  + ". Thresholds were available for these feature tuples: "
                                                  + thresholdsByFeature.keySet()
                                                  + "." );
        }

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Predicate<Pair<Double, Double>>> slicers =
                    ThresholdSlicer.getFiltersFromThresholds( thresholds,
                                                              SingleValuedStatisticsProcessor::getFilterForSingleValuedPairs );

            // Add the threshold to the pool metadata            
            ThresholdOuter outer = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            OneOrTwoThresholds composed = OneOrTwoThresholds.of( outer );

            UnaryOperator<PoolMetadata> metaTransformer =
                    untransformed -> PoolMetadata.of( untransformed, composed );

            // Decompose by feature
            Map<FeatureTuple, Pool<Pair<Double, Double>>> pools =
                    PoolSlicer.decompose( pool, PoolSlicer.getFeatureMapper() );

            // Filter by threshold using the feature as a hook to tie a pool to a threshold
            Pool<Pair<Double, Double>> sliced =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.filter( pools,
                                                                             slicers,
                                                                             pool.getMetadata(),
                                                                             super.getBaselineMetadata( pool ),
                                                                             metaTransformer,
                                                                             outer.getLabel() ) );

            this.processSingleValuedPairs( sliced,
                                           futures,
                                           outGroup );
        }
    }

    /**
     * Processes one threshold for metrics that consume single-valued pairs and produce a specified 
     * {@link StatisticType}. 
     *
     * @param pairs the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    private void processSingleValuedPairs( Pool<Pair<Double, Double>> pairs,
                                           StatisticsStore.Builder futures,
                                           StatisticType outGroup )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get()
                  .isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        switch ( outGroup )
        {
            case DOUBLE_SCORE -> futures.addDoubleScoreStatistics( this.processSingleValuedPairs( pairs,
                                                                                                  this.singleValuedScore ) );
            case DIAGRAM -> futures.addDiagramStatistics( this.processSingleValuedPairs( pairs,
                                                                                         this.singleValuedDiagrams ) );
            case BOXPLOT_PER_POOL -> futures.addBoxPlotStatisticsPerPool( this.processSingleValuedPairs( pairs,
                                                                                                         this.singleValuedBoxPlot ) );
            default -> throw new IllegalStateException( "The statistic group '" + outGroup
                                                        + "' is not supported in this context." );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes single-valued pairs at a specific 
     * {@link TimeWindowOuter} and {@link ThresholdOuter}.
     *
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @return the future result
     */

    private <T extends Statistic<?>> Future<List<T>>
    processSingleValuedPairs( Pool<Pair<Double, Double>> pairs,
                              MetricCollection<Pool<Pair<Double, Double>>, T, T> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get()
                  .size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                Set<MetricConstants> collected = new HashSet<>( collection.getMetrics() );
                collected.remove( MetricConstants.SAMPLE_SIZE );

                LOGGER.debug( "While processing pairs for pool {}, discovered {} pairs, which is fewer than the "
                              + "minimum sample size of {} pairs. The following metrics will not be computed for this "
                              + "pool: {}.",
                              pairs.getMetadata(),
                              pairs.get().size(),
                              minimumSampleSize,
                              collected );
            }

            // Allow the sample size through without constraint
            if ( collection.getMetrics().contains( MetricConstants.SAMPLE_SIZE ) )
            {
                Set<MetricConstants> ss = Set.of( MetricConstants.SAMPLE_SIZE );
                // Submit on a different executor than the metric executor that does the work
                return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ss ),
                                                      this.getSlicingExecutor() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        return this.processMetricsRequiredForThisPool( pairs, collection );
    }

    /**
     * Processes a set of metric futures that consume dichotomous pairs, which are mapped from single-valued pairs 
     * using a configured mapping function.
     *
     * @param input the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processPairsForDichotomousMetrics( Pool<Pair<Double, Double>> input,
                                                    StatisticsStore.Builder futures )
    {
        if ( hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            this.processDichotomousPairsByThreshold( input, futures );
        }
    }

    /**
     * Processes a set of metric futures that consume dichotomous pairs, which are mapped from single-valued pairs 
     * using a configured mapping function. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processDichotomousPairsByThreshold( Pool<Pair<Double, Double>> pool,
                                                     StatisticsStore.Builder futures )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholdsWithoutAllData();
        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Define a mapper to convert the single-valued pairs to dichotomous pairs
        Function<ThresholdOuter, Function<Pair<Double, Double>, Pair<Boolean, Boolean>>> transformerGenerator =
                threshold -> pair -> Pair.of( threshold.test( pair.getLeft() ),
                                              threshold.test( pair.getRight() ) );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Function<Pair<Double, Double>, Pair<Boolean, Boolean>>> transformers =
                    ThresholdSlicer.getTransformersFromThresholds( thresholds,
                                                                   transformerGenerator );

            // Add the threshold to the metadata
            ThresholdOuter outer = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            OneOrTwoThresholds composed = OneOrTwoThresholds.of( outer );
            UnaryOperator<PoolMetadata> metaTransformer =
                    untransformed -> PoolMetadata.of( untransformed, composed );

            // Decompose by feature
            Map<FeatureTuple, Pool<Pair<Double, Double>>> pools =
                    PoolSlicer.decompose( pool, PoolSlicer.getFeatureMapper() );

            // Transform by threshold using the feature as a hook to tie a pool to a threshold
            Pool<Pair<Boolean, Boolean>> transformed =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.transform( pools,
                                                                                transformers,
                                                                                pool.getMetadata(),
                                                                                this.getBaselineMetadata( pool ),
                                                                                metaTransformer ) );

            super.processDichotomousPairs( transformed,
                                           futures );
        }
    }

    /**
     * Processes a set of metric futures that consume {@link Pool} with single-valued pairs. 
     *
     * @param pool the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processTimeSeriesPairs( Pool<TimeSeries<Pair<Double, Double>>> pool,
                                         StatisticsStore.Builder futures )
    {
        // Filter the thresholds for the feature group associated with this pool and for the required types
        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = super.getThresholds();
        FeatureGroup featureGroup = pool.getMetadata()
                                        .getFeatureGroup();
        Map<FeatureTuple, Set<ThresholdOuter>> filteredThresholds =
                super.getFilteredThresholds( thresholdsByFeature,
                                             featureGroup,
                                             ThresholdType.PROBABILITY,
                                             ThresholdType.VALUE );

        // Add the quantiles
        Map<FeatureTuple, Set<ThresholdOuter>> withQuantiles = ThresholdSlicer.addQuantiles( filteredThresholds,
                                                                                             pool.getClimatology() );

        // Find the unique thresholds by value
        Map<FeatureTuple, Set<ThresholdOuter>> unique =
                ThresholdSlicer.filter( withQuantiles, ThresholdSlicer::filter );

        // Decompose the thresholds into groups whereby each group contains one "logical" threshold per feature. For
        // example, a logical threshold is a threshold that is consistently named "banana" across all features
        List<Map<FeatureTuple, ThresholdOuter>> decomposedThresholds = ThresholdSlicer.decompose( unique );

        // Iterate the thresholds
        for ( Map<FeatureTuple, ThresholdOuter> thresholds : decomposedThresholds )
        {
            Map<FeatureTuple, Predicate<TimeSeries<Pair<Double, Double>>>> slicers =
                    ThresholdSlicer.getFiltersFromThresholds( thresholds,
                                                              SingleValuedStatisticsProcessor::getFilterForTimeSeriesOfSingleValuedPairs );

            // Add the threshold to the pool metadata            
            ThresholdOuter outer = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            OneOrTwoThresholds composed = OneOrTwoThresholds.of( outer );
            UnaryOperator<PoolMetadata> metaTransformer =
                    untransformed -> PoolMetadata.of( untransformed, composed );

            // Decompose by feature
            Map<FeatureTuple, Pool<TimeSeries<Pair<Double, Double>>>> pools =
                    PoolSlicer.decompose( pool, PoolSlicer.getFeatureMapper() );

            // Filter by threshold using the feature as a hook to tie a pool to a threshold
            Pool<TimeSeries<Pair<Double, Double>>> sliced =
                    this.doWorkWithSlicingExecutor( () -> PoolSlicer.filter( pools,
                                                                             slicers,
                                                                             pool.getMetadata(),
                                                                             super.getBaselineMetadata( pool ),
                                                                             metaTransformer,
                                                                             outer.getLabel() ) );

            // Build the future result
            if ( Objects.nonNull( this.timeSeries ) )
            {
                Future<List<DurationDiagramStatisticOuter>> durationDiagrams = this.processTimeSeriesPairs( sliced,
                                                                                                            this.timeSeries );
                futures.addDurationDiagramStatistics( durationDiagrams );
            }

            if ( Objects.nonNull( this.singleValuedPairs ) )
            {
                Future<List<PairsStatisticOuter>> pairsStatistics = this.processPairsStatistics( sliced,
                                                                                                 this.singleValuedPairs );
                futures.addPairsStatistics( pairsStatistics );
            }

            // Summary statistics?
            if ( Objects.nonNull( this.timeSeriesStatistics ) )
            {
                Future<List<DurationScoreStatisticOuter>> summary =
                        this.processTimeSeriesSummaryPairs( sliced,
                                                            this.timeSeriesStatistics );

                futures.addDurationScoreStatistics( summary );
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
        int minimumSampleSize = super.getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get()
                  .size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( SMALL_SAMPLE_MESSAGE,
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
     * {@link DurationDiagramStatisticOuter}.
     *
     * @param pairs the pairs
     * @param collection the collection of metrics
     * @return the future result
     */

    private Future<List<PairsStatisticOuter>>
    processPairsStatistics( Pool<TimeSeries<Pair<Double, Double>>> pairs,
                            MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, PairsStatisticOuter, PairsStatisticOuter> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = super.getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get()
                  .size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( SMALL_SAMPLE_MESSAGE,
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
        int minimumSampleSize = super.getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get()
                  .size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( SMALL_SAMPLE_MESSAGE,
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
     * Creates a collection of metrics that compute time-series summary metrics.
     * @param metricExecutor the metric executor
     * @return the time-series summary statistics
     */

    private MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter>
    getTimeSeriesSummaryStatistics( ExecutorService metricExecutor )
    {
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
                    throw new DeclarationException( "The timing error summary statistic '" + next
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

            return MetricFactory.ofSummaryStatisticsForTimingErrorMetrics( metricExecutor, localStatistics );
        }

        return null;
    }

    /**
     * Validates the state of the processor.
     * @throws DeclarationException if the state is invalid for any reason
     */

    private void validate()
    {
        // Check the metrics individually, as some may belong to multiple groups
        for ( MetricConstants next : super.getMetrics() )
        {
            // Thresholds required for dichotomous metrics
            if ( next.isInGroup( SampleDataGroup.DICHOTOMOUS )
                 && super.getThresholds()
                         .values()
                         .stream()
                         .flatMap( Set::stream )
                         // Ignore the "all data" threshold in this check
                         .filter( t -> !ThresholdOuter.ALL_DATA.equals( t ) )
                         .noneMatch( threshold -> threshold.getType() == ThresholdType.VALUE
                                                  || threshold.getType() == ThresholdType.PROBABILITY ) )
            {
                throw new DeclarationException( "Cannot configure '"
                                                + next
                                                + "' without thresholds to define the events: add one "
                                                + "or more thresholds to the configuration for the '"
                                                + next
                                                + "'." );
            }
        }
    }

}
