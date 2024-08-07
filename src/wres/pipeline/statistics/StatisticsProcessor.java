package wres.pipeline.statistics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.ThresholdType;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.config.MetricConstants.StatisticType;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.metrics.Metric;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricCollection;
import wres.metrics.MetricFactory;
import wres.metrics.MetricParameterException;
import wres.pipeline.WresProcessingException;

/**
 * Creates statistics by applying a {@link Metric} to a {@link Pool} and stores them in a {@link StatisticsStore}.
 *
 * @author James Brown
 */

public abstract class StatisticsProcessor<S extends Pool<?>> implements Function<S, StatisticsStore>
{
    /** Message that indicates processing is complete. */
    static final String PROCESSING_COMPLETE_MESSAGE = "Completed processing of metrics for feature group '{}' "
                                                      + "at time window '{}'.";

    /** Logger instance. */
    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsProcessor.class );

    /** A {@link MetricCollection} of {@link Metric} that consume dichotomous pairs and produce {@link ScoreStatistic}. */
    private final MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            dichotomousScalar;

    /** An {@link ExecutorService} used to perform slicing and dicing of pools. */
    private final ExecutorService slicingExecutor;

    /** The all data threshold. */
    private final ThresholdOuter allDataThreshold;

    /** The thresholds by feature. */
    private final Map<FeatureTuple, Set<ThresholdOuter>> thresholds;

    /** The thresholds by feature. */
    private final Map<FeatureTuple, Set<ThresholdOuter>> thresholdsWithoutAllData;

    /** The raw metrics (the union for all thresholds and features). */
    private final Set<MetricConstants> metrics;

    /** Minimum sample size for metric calculation. */
    private final int minimumSampleSize;

    /**
     * @return the thresholds
     */

    Map<FeatureTuple, Set<ThresholdOuter>> getThresholds()
    {
        return this.thresholds;
    }

    /**
     * @return the thresholds without the all data threshold, which cannot be used by dichotomous metrics.
     */

    Map<FeatureTuple, Set<ThresholdOuter>> getThresholdsWithoutAllData()
    {
        return this.thresholdsWithoutAllData;
    }

    /**
     * @return the metrics
     */

    Set<MetricConstants> getMetrics()
    {
        return this.metrics;
    }

    /**
     * Processes one threshold for metrics that consume dichotomous pairs. 
     *
     * @param pairs the input pairs
     * @param futures the metric futures
     */

    void processDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs,
                                  StatisticsStore.Builder futures )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get()
                  .isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        Future<List<DoubleScoreStatisticOuter>> scores = this.processDichotomousPairs( pairs,
                                                                                       this.dichotomousScalar );
        futures.addDoubleScoreStatistics( scores );
    }

    /**
     * Inspects the minimum sample size and, if skill metrics are present and the minimum sample size is not met, 
     * removes the skill metrics from consideration, otherwise leaves them. Also, inspects the threshold and eliminates
     * any metrics from consideration for which {@link MetricConstants#isAThresholdMetric()} indicates that the metric
     * is not a threshold metric, but only when the threshold is not the "all data" threshold.
     *
     * @param <U> the type of pooled data
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    <U, T extends Statistic<?>> Future<List<T>> processMetricsRequiredForThisPool( Pool<U> pairs,
                                                                                   MetricCollection<Pool<U>, T, T> collection )
    {
        int minimumSampleSizeInner = this.getMinimumSampleSize();

        // The metrics to compute
        Set<MetricConstants> all = new HashSet<>( collection.getMetrics() );

        LOGGER.debug( "Considering whether to compute these metrics: {}.", all );

        Set<MetricConstants> filterFirst =
                this.getSkillMetricsWhereBaselineHasInsufficientSampleSize( all, pairs, minimumSampleSizeInner );
        all.removeAll( filterFirst );

        Set<MetricConstants> filterSecond =
                this.getMetricsThatDoNotAcceptThresholdWhenThresholdPresent( all, pairs );
        all.removeAll( filterSecond );

        Set<MetricConstants> filterThird =
                this.getMetricsThatRequireExplicitBaselineAndNoneAvailable( all, pairs );
        all.removeAll( filterThird );

        LOGGER.debug( "Computing these metrics: {}.", all );

        // All metrics skipped for this pool?
        if ( all.isEmpty() )
        {
            LOGGER.debug( "None of the supplied metrics are required for this pool. The (skipped) metrics are: {}. "
                          + "The pool is: {}.", all, pairs.getMetadata() );

            return CompletableFuture.completedFuture( Collections.emptyList() );
        }

        // Dispatch from a different executor than the metric executor, which does the underlying work
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, all ),
                                              this.getSlicingExecutor() );
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     * otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link SampleDataGroup} and {@link StatisticType}, false
     *         otherwise
     */

    boolean hasMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        return this.getMetrics()
                   .stream()
                   .anyMatch( next -> next.isInGroup( inGroup ) && next.isInGroup( outGroup ) );
    }

    /**
     * Returns true if metrics are available for the input {@link SampleDataGroup}, false otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @return true if metrics are available for the input {@link SampleDataGroup} false otherwise
     */

    boolean hasMetrics( SampleDataGroup inGroup )
    {
        return this.getMetrics()
                   .stream()
                   .anyMatch( next -> next.isInGroup( inGroup ) );
    }

    /**
     * Returns true if metrics are available for the {@link StatisticType#DOUBLE_SCORE}, false otherwise.
     *
     * @return true if metrics are available for the {@link StatisticType#DOUBLE_SCORE} false otherwise
     */

    boolean hasDoubleScoreMetrics()
    {
        return this.getMetrics()
                   .stream()
                   .anyMatch( next -> next.isInGroup( StatisticType.DOUBLE_SCORE ) );
    }

    /**
     * Returns the all data threshold.
     *
     * @return the all data threshold
     */

    ThresholdOuter getAllDataThreshold()
    {
        return this.allDataThreshold;
    }

    /**
     * @return the threshold executor
     */

    ExecutorService getSlicingExecutor()
    {
        return this.slicingExecutor;
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}.
     * Individual elements of the contingency table are not considered.
     *
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticType}, may be null
     * @return an array of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    MetricConstants[] getMetrics( SampleDataGroup inGroup,
                                  StatisticType outGroup )
    {
        // Create a filter based on input
        Predicate<MetricConstants> tester;
        if ( Objects.nonNull( inGroup ) && Objects.nonNull( outGroup ) )
        {
            tester = a -> a.isInGroup( inGroup ) && a.isInGroup( outGroup );
        }
        else if ( Objects.nonNull( inGroup ) )
        {
            tester = a -> a.isInGroup( inGroup );
        }
        else if ( Objects.nonNull( outGroup ) )
        {
            tester = a -> a.isInGroup( outGroup );
        }
        else
        {
            tester = a -> true;
        }

        Set<MetricConstants> filtered = this.getMetrics()
                                            .stream()
                                            .filter( tester )
                                            .collect( Collectors.toCollection( HashSet::new ) );

        // Remove contingency table elements
        filtered.remove( MetricConstants.TRUE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_NEGATIVES );
        filtered.remove( MetricConstants.TRUE_NEGATIVES );

        return filtered.toArray( new MetricConstants[0] );
    }

    /**
     * Extracts the baseline metadata from the pool, if available.
     *
     * @return the baseline metadata or null
     * @throws NullPointerException if the pool is null
     */

    PoolMetadata getBaselineMetadata( Pool<?> pool )
    {
        Objects.requireNonNull( pool );

        PoolMetadata returnMe = null;

        if ( pool.hasBaseline() )
        {
            returnMe = pool.getBaselineData()
                           .getMetadata();
        }

        return returnMe;
    }

    /**
     * @return the minimum sample size
     */
    int getMinimumSampleSize()
    {
        return this.minimumSampleSize;
    }

    /**
     * Filters the prescribed thresholds using the inputs.
     * @param thresholds the thresholds
     * @param featureGroup the feature group containing features for which thresholds are required
     * @param thresholdTypes the threshold types
     * @return the filtered thresholds
     */

    Map<FeatureTuple, Set<ThresholdOuter>> getFilteredThresholds( Map<FeatureTuple, Set<ThresholdOuter>> thresholds,
                                                                  FeatureGroup featureGroup,
                                                                  ThresholdType... thresholdTypes )
    {
        Set<FeatureTuple> features = featureGroup.getFeatures();
        Set<ThresholdType> typeSet = Arrays.stream( thresholdTypes )
                                           .collect( Collectors.toSet() );
        UnaryOperator<Set<ThresholdOuter>> typesFilter =
                types -> types.stream()
                              .filter( next -> typeSet.contains( next.getType() ) )
                              .collect( Collectors.toSet() );

        return thresholds.entrySet()
                         .stream()
                         // Filter the features
                         .filter( next -> features.contains( next.getKey() ) )
                         .collect( Collectors.toMap( Map.Entry::getKey,
                                                     // Filter the types
                                                     next -> typesFilter.apply( next.getValue() ) ) );
    }

    /**
     * Performs work on the thread pool intended for slicing and dicing pooled datasets.
     *
     * @param <T> the type of result from the work
     * @param work the work to perform
     */

    <T> T doWorkWithSlicingExecutor( Supplier<T> work )
    {
        Future<T> workDone = CompletableFuture.supplyAsync( work,
                                                            this.getSlicingExecutor() );
        try
        {
            return workDone.get();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();
            throw new WresProcessingException( "Interrupted while attempting to complete work on a metric thread.", e );
        }
        catch ( ExecutionException e )
        {
            throw new WresProcessingException( "Failed to complete work on a metric thread.", e );
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes dichotomous pairs.
     *
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    private <T extends Statistic<?>> Future<List<T>>
    processDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs,
                             MetricCollection<Pool<Pair<Boolean, Boolean>>, T, T> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSizeInner = this.getMinimumSampleSize();
        int actualSampleSize = this.getSampleSizeForDichotomousPairs( pairs );

        // Log and return an empty result if the sample size is too small
        if ( actualSampleSize < minimumSampleSizeInner )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While processing dichotomous pairs for pool {}, discovered that the smaller of the "
                              + "number of left occurrences and non-occurrences was {}, which is less than the minimum "
                              + "sample size of {}. The following metrics will not be computed for this pool: {}.",
                              pairs.getMetadata(),
                              actualSampleSize,
                              minimumSampleSizeInner,
                              collection.getMetrics() );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        return this.processMetricsRequiredForThisPool( pairs, collection );
    }

    /**
     * @param pairs the pairs whose sample size is required
     * @return the sample size for a pool of dichotomous pairs, which is the smaller of left occurrences and 
     *            non-occurrences
     */

    private int getSampleSizeForDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs )
    {
        int occurrences = 0;
        int nonOccurrences = 0;

        for ( Pair<Boolean, Boolean> next : pairs.get() )
        {
            if ( Boolean.TRUE.equals( next.getLeft() ) )
            {
                occurrences++;
            }
            else
            {
                nonOccurrences++;
            }
        }

        return Math.min( occurrences, nonOccurrences );
    }

    /**
     * Constructor.
     *
     * @param metricsAndThresholds the metrics and thresholds
     * @param slicingExecutor an {@link ExecutorService} for executing slicing and dicing of pools, cannot be null
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    StatisticsProcessor( MetricsAndThresholds metricsAndThresholds,
                         ExecutorService slicingExecutor,
                         ExecutorService metricExecutor )
    {
        Objects.requireNonNull( slicingExecutor, "Specify a non-null threshold executor service." );
        Objects.requireNonNull( metricExecutor, "Specify a non-null metric executor service." );
        Objects.requireNonNull( metricsAndThresholds, "Specify metrics and thresholds to process." );
        Objects.requireNonNull( metricsAndThresholds.metrics(),
                                "Specify a non-null collection of metrics to process." );
        Objects.requireNonNull( metricsAndThresholds.thresholds(),
                                "Specify a non-null collection of thresholds to process." );

        if ( metricsAndThresholds.metrics()
                                 .isEmpty() )
        {
            throw new MetricCalculationException( "Cannot build a statistics processor without metrics." );
        }

        this.metrics = metricsAndThresholds.metrics();
        this.thresholds = metricsAndThresholds.thresholds();
        this.thresholdsWithoutAllData = this.copyWithoutAllDataThreshold( this.thresholds );
        this.minimumSampleSize = metricsAndThresholds.minimumSampleSize();

        LOGGER.debug( "Received the following metrics and threshold to compute: {}.", metricsAndThresholds );

        if ( this.minimumSampleSize < 0 )
        {
            throw new MetricParameterException( "The minimum sample size must be greater than or equal to zero: "
                                                + minimumSampleSize
                                                + "." );
        }

        LOGGER.debug( "Based on the project declaration, the following metrics will be computed: {}.", this.metrics );

        // Dichotomous scores
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            MetricConstants[] scores = this.getMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE );
            this.dichotomousScalar = MetricFactory.ofDichotomousScores( metricExecutor, scores );

            LOGGER.debug( "Created the dichotomous scores for processing. {}", this.dichotomousScalar );
        }
        else
        {
            this.dichotomousScalar = null;
        }

        // Set the executor for processing thresholds
        this.slicingExecutor = slicingExecutor;

        this.allDataThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                   ThresholdOperator.GREATER,
                                                   ThresholdOrientation.LEFT_AND_RIGHT );
    }

    /**
     * Copies the input without the "all data" threshold.
     * @param thresholds the thresholds
     * @return the copied thresholds without the "all data" threshold
     */

    private Map<FeatureTuple, Set<ThresholdOuter>> copyWithoutAllDataThreshold( Map<FeatureTuple, Set<ThresholdOuter>> thresholds )
    {
        return ThresholdSlicer.filter( thresholds, next -> next.stream()
                                                               .filter( n -> !n.isAllDataThreshold() )
                                                               .collect( Collectors.toSet() ) );
    }

    /**
     * Returns the set of metrics that are skill metrics where the baseline sample size is insufficient.
     *
     * @param <U> type of pooled data
     * @param metrics the metrics to consider
     * @param pool the pairs
     * @param minimumSampleSize the minimum sample size
     * @return the metrics to filter
     */

    private <U> Set<MetricConstants> getSkillMetricsWhereBaselineHasInsufficientSampleSize( Set<MetricConstants> metrics,
                                                                                            Pool<U> pool,
                                                                                            int minimumSampleSize )
    {
        Set<MetricConstants> filteredInner = Collections.emptySet();

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        if ( metrics.stream()
                    .anyMatch( MetricConstants::isSkillMetric )
             && pool.hasBaseline() )
        {
            int actualBaselineSampleSize = pool.getBaselineData().get().size();

            if ( actualBaselineSampleSize < minimumSampleSize )
            {
                filteredInner = metrics.stream()
                                       .filter( MetricConstants::isSkillMetric )
                                       .collect( Collectors.toUnmodifiableSet() );

                // Remove the filtered metrics
                LOGGER.debug( "While processing pairs for pool {}, discovered {} baseline pairs, which is fewer than "
                              + "the minimum sample size of {} pairs. The following metrics will not be computed for "
                              + "this pool: {}.",
                              pool.getBaselineData()
                                  .getMetadata(),
                              actualBaselineSampleSize,
                              minimumSampleSize,
                              filteredInner );
            }
        }
        return filteredInner;
    }

    /**
     * Returns the set of metrics that cannot be computed for a threshold and a threshold is present.
     *
     * @param <U> type of pooled data
     * @param metrics the metrics to consider
     * @param pool the pairs
     * @return the metrics to filter
     */

    private <U> Set<MetricConstants> getMetricsThatDoNotAcceptThresholdWhenThresholdPresent( Set<MetricConstants> metrics,
                                                                                             Pool<U> pool )
    {
        Set<MetricConstants> filteredInner = Collections.emptySet();

        // Are there any metrics that do not accept thresholds and is this a threshold other than "all data"?
        if ( metrics.stream()
                    .anyMatch( next -> !next.isAThresholdMetric() )
             && !ThresholdOuter.ALL_DATA.equals( pool.getMetadata()
                                                     .getThresholds()
                                                     .first() ) )
        {
            filteredInner = metrics.stream()
                                   .filter( next -> !next.isAThresholdMetric() )
                                   .collect( Collectors.toUnmodifiableSet() );

            if ( LOGGER.isDebugEnabled()
                 && !filteredInner.isEmpty() )
            {
                Pool<U> baseline = pool.getBaselineData();
                PoolMetadata baselineMetadata = null;
                if ( Objects.nonNull( baseline ) )
                {
                    baselineMetadata = baseline.getMetadata();
                }

                LOGGER.debug( "While processing pairs for pool {}, discovered {} metrics that cannot be computed for a "
                              + "threshold of {}. The following metrics will not be computed for this pool: {}.",
                              baselineMetadata,
                              filteredInner.size(),
                              pool.getMetadata()
                                  .getThresholds(),
                              filteredInner );
            }
        }

        return filteredInner;
    }

    /**
     * Returns the set of metrics that require an explicit baseline, and no baseline is available.
     *
     * @param <U> type of pooled data
     * @param metrics the metrics to consider
     * @param pool the pairs
     * @return the metrics to filter
     */

    private <U> Set<MetricConstants> getMetricsThatRequireExplicitBaselineAndNoneAvailable( Set<MetricConstants> metrics,
                                                                                            Pool<U> pool )
    {
        Set<MetricConstants> filteredInner = Collections.emptySet();

        // Is this a metric that requires an explicit baseline when none is available?
        // Cannot include skill metrics, in general, here because some skill metrics have a default baseline
        if ( !pool.hasBaseline() )
        {
            filteredInner = metrics.stream()
                                   .filter( MetricConstants::isExplicitBaselineRequired )
                                   .collect( Collectors.toUnmodifiableSet() );

            if ( LOGGER.isDebugEnabled()
                 && !filteredInner.isEmpty() )
            {
                LOGGER.debug( "While processing pairs for pool {}, discovered {} metrics that cannot be computed "
                              + "because they require a baseline dataset and the pool does not contain a baseline "
                              + "dataset. The following metrics will not be computed for this pool: {}.",
                              pool.getMetadata(),
                              filteredInner.size(),
                              filteredInner );
            }
        }

        return filteredInner;
    }
}
