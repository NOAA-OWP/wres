package wres.pipeline.statistics;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.DoublePredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.datamodel.pools.Pool;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsStore;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.metrics.Metric;
import wres.metrics.MetricCollection;
import wres.metrics.MetricFactory;
import wres.metrics.MetricParameterException;

/**
 * Creates statistics by computing {@link Metric} with {@link Pool} and stores them in a {@link StatisticsStore}.
 * 
 * @author James Brown
 */

public abstract class StatisticsProcessor<S extends Pool<?>> implements Function<S, StatisticsStore>
{
    /**
     * Filter for admissible numerical data.
     */

    static final DoublePredicate ADMISSABLE_DATA = Double::isFinite;

    /**
     * Message that indicates processing is complete.
     */

    static final String PROCESSING_COMPLETE_MESSAGE = "Completed processing of metrics for feature group '{}' "
                                                      + "at time window '{}'.";

    /**
     * Logger instance.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsProcessor.class );

    /**
     * A {@link MetricCollection} of {@link Metric} that consume dichotomous pairs and produce {@link ScoreStatistic}.
     */

    private final MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> dichotomousScalar;

    /**
     * An {@link ExecutorService} used to process the thresholds.
     */

    private final ExecutorService thresholdExecutor;

    /**
     * The all data threshold.
     */

    private final ThresholdOuter allDataThreshold;

    /**
     * The metrics by threshold and feature.
     */

    private final ThresholdsByMetricAndFeature metrics;

    /**
     * The raw metrics (the union for all thresholds and features).
     */

    private final Set<MetricConstants> rawMetrics;

    /**
     * Returns the metrics to process.
     * 
     * @return the metrics
     */

    public ThresholdsByMetricAndFeature getMetrics()
    {
        return this.metrics;
    }

    /**
     * Processes one threshold for metrics that consume dichotomous pairs. 
     * 
     * @param pairs the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    void processDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs,
                                  StatisticsFutures.MetricFuturesByTimeBuilder futures,
                                  StatisticType outGroup )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get().isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            Future<List<DoubleScoreStatisticOuter>> scores = this.processDichotomousPairs( pairs,
                                                                                           this.dichotomousScalar );
            futures.addDoubleScoreOutput( scores );
        }
    }

    /**
     * Inspects the minimum sample size and, if skill metrics are present and the minimum sample size is not met, 
     * removes the skill metrics from consideration, otherwise leaves them. Also, inspects the threshold and eliminates
     * any metrics from consideration for which {@link MetricConstants#isAThresholdMetric()} returns false when the 
     * threshold is not the "all data" threshold.
     * 
     * @param <U> the type of pooled data
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    <U, T extends Statistic<?>> Future<List<T>>
            processMetricsRequiredForThisPool( Pool<U> pairs,
                                               MetricCollection<Pool<U>, T, T> collection )
    {
        int minimumSampleSize = this.getMetrics()
                                    .getMinimumSampleSize();

        // The metrics to compute
        Set<MetricConstants> all = new HashSet<>( collection.getMetrics() );

        LOGGER.debug( "Considering whether to compute these metrics: {}.", all );
        
        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        if ( collection.getMetrics()
                       .stream()
                       .anyMatch( MetricConstants::isSkillMetric )
             && pairs.hasBaseline() )
        {
            int actualBaselineSampleSize = pairs.getBaselineData().get().size();

            if ( actualBaselineSampleSize < minimumSampleSize )
            {
                Set<MetricConstants> filteredInner = all.stream()
                                                        .filter( MetricConstants::isSkillMetric )
                                                        .collect( Collectors.toUnmodifiableSet() );

                // Remove the filtered metrics
                all.removeAll( filteredInner );

                if ( LOGGER.isDebugEnabled() )
                {
                    LOGGER.debug( "While processing pairs for pool {}, discovered {} baseline pairs, which is fewer than "
                                  + "the minimum sample size of {} pairs. The following metrics will not be computed for "
                                  + "this pool: {}.",
                                  pairs.getBaselineData().getMetadata(),
                                  actualBaselineSampleSize,
                                  minimumSampleSize,
                                  filteredInner );
                }
            }
        }

        // Are there any metrics that do not accept thresholds and is this a threshold other than "all data"?
        if ( collection.getMetrics().stream().anyMatch( next -> !next.isAThresholdMetric() )
             && !ThresholdOuter.ALL_DATA.equals( pairs.getMetadata().getThresholds().first() ) )
        {
            Set<MetricConstants> filteredInner = all.stream()
                                                    .filter( next -> !next.isAThresholdMetric() )
                                                    .collect( Collectors.toUnmodifiableSet() );

            // Remove the filtered metrics
            all.removeAll( filteredInner );

            if ( LOGGER.isDebugEnabled() && !filteredInner.isEmpty() )
            {
                LOGGER.debug( "While processing pairs for pool {}, discovered {} metrics that cannot be computed for a "
                              + "threshold of {}. The following metrics will not be computed for this pool: {}.",
                              pairs.getBaselineData().getMetadata(),
                              filteredInner.size(),
                              pairs.getMetadata().getThresholds(),
                              filteredInner );
            }
        }

        LOGGER.debug( "Computing these metrics: {}.", all );
        
        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, all ),
                                              this.thresholdExecutor );
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
        return this.rawMetrics.stream()
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
        return this.rawMetrics.stream()
                              .anyMatch( next -> next.isInGroup( inGroup ) );
    }

    /**
     * Returns true if metrics are available for the input {@link StatisticType}, false otherwise.
     * 
     * @param outGroup the {@link StatisticType}
     * @return true if metrics are available for the input {@link StatisticType} false otherwise
     */

    boolean hasMetrics( StatisticType outGroup )
    {
        return this.rawMetrics.stream()
                              .anyMatch( next -> next.isInGroup( outGroup ) );
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
     * Returns true if the input list of thresholds contains one or more probability thresholds, false otherwise.
     * 
     * @param check the thresholds to check
     * @return true if the input list contains one or more probability thresholds, false otherwise
     */

    boolean hasProbabilityThreshold( Set<ThresholdOuter> check )
    {
        return check.stream().anyMatch( ThresholdOuter::hasProbabilities );
    }

    /**
     * @return the threshold executor
     */

    ExecutorService getThresholdExecutor()
    {
        return this.thresholdExecutor;
    }

    /**
     * Returns a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}.
     * Individual elements of the contingency table are not considered.
     * 
     * @param inGroup the {@link SampleDataGroup}, may be null
     * @param outGroup the {@link StatisticType}, may be null
     * @return a set of {@link MetricConstants} for a specified {@link SampleDataGroup} and {@link StatisticType}
     *         or an empty array if both inputs are defined and no corresponding metrics are present
     */

    MetricConstants[] getMetrics( SampleDataGroup inGroup,
                                  StatisticType outGroup )
    {
        // Create a filter based on input
        Predicate<MetricConstants> tester = null;
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

        Set<MetricConstants> filtered = this.rawMetrics.stream()
                                                       .filter( tester )
                                                       .collect( Collectors.toCollection( HashSet::new ) );

        // Remove contingency table elements
        filtered.remove( MetricConstants.TRUE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_POSITIVES );
        filtered.remove( MetricConstants.FALSE_NEGATIVES );
        filtered.remove( MetricConstants.TRUE_NEGATIVES );

        return filtered.toArray( new MetricConstants[filtered.size()] );
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes dichotomous pairs.
     * 
     * @param <T> the type of {@link Statistic}
     * @param threshold the threshold
     * @param pairs the pairs
     * @param collection the metric collection
     * @return true if the future was added successfully
     */

    private <T extends Statistic<?>> Future<List<T>>
            processDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs,
                                     MetricCollection<Pool<Pair<Boolean, Boolean>>, T, T> collection )
    {
        // More samples than the minimum sample size?
        int minimumSampleSize = this.getMetrics()
                                    .getMinimumSampleSize();

        int actualSampleSize = this.getSampleSizeForDichotomousPairs( pairs );

        // Log and return an empty result if the sample size is too small
        if ( actualSampleSize < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "While processing dichotomous pairs for pool {}, discovered that the smaller of the "
                              + "number of left occurrences and non-occurrences was {}, which is less than the minimum "
                              + "sample size of {}. The following metrics will not be computed for this pool: {}.",
                              pairs.getMetadata(),
                              actualSampleSize,
                              minimumSampleSize,
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
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    StatisticsProcessor( ThresholdsByMetricAndFeature metrics,
                     ExecutorService thresholdExecutor,
                     ExecutorService metricExecutor )
    {
        Objects.requireNonNull( thresholdExecutor, "Specify a non-null threshold executor service." );

        Objects.requireNonNull( metricExecutor, "Specify a non-null metric executor service." );

        Objects.requireNonNull( metrics, "Specify a non-null collection of metrics to process." );

        this.metrics = metrics;
        this.rawMetrics = this.metrics.getMetrics();

        LOGGER.debug( "Based on the project declaration, the following metrics will be computed: {}.", this.metrics );

        //Dichotomous scores
        if ( this.hasMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ) )
        {
            MetricConstants[] scores = this.getMetrics( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE );
            this.dichotomousScalar = MetricFactory.ofDichotomousScoreCollection( metricExecutor, scores );

            LOGGER.debug( "Created the dichotomous scores for processing. {}", this.dichotomousScalar );
        }
        else
        {
            this.dichotomousScalar = null;
        }

        //Set the executor for processing thresholds
        this.thresholdExecutor = thresholdExecutor;

        this.allDataThreshold = ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                   Operator.GREATER,
                                                   ThresholdDataType.LEFT_AND_RIGHT );
    }

}