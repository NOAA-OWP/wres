package wres.pipeline.statistics;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdSlicer;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetricAndFeature;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricCollection;
import wres.metrics.MetricParameterException;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindowOuter}.
 * 
 * @author James Brown
 */

abstract class MetricProcessorByTime<S extends Pool<?>>
        extends MetricProcessor<S>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessorByTime.class );

    /**
     * Message that indicates processing is complete.
     */

    static final String PROCESSING_COMPLETE_MESSAGE = "Completed processing of metrics for feature '{}' "
                                                      + "at time window '{}'.";

    /**
     * Helper that returns a predicate for filtering single-valued pairs based on the 
     * {@link ThresholdOuter#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getDataType()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getDataType()} is not recognized
     */

    static Predicate<Pair<Double, Double>> getFilterForSingleValuedPairs( ThresholdOuter input )
    {
        switch ( input.getDataType() )
        {
            case LEFT:
                return Slicer.left( input );
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                return Slicer.leftAndRight( input );
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                return Slicer.right( input );
            default:
                throw new IllegalStateException( "Unrecognized threshold type '" + input.getDataType() + "'." );
        }
    }

    /**
     * Helper that returns a predicate for filtering {@link TimeSeriesOfSinglevaluedPairs} based on the 
     * {@link ThresholdOuter#getDataType()} of the input threshold.
     * 
     * @param threshold the threshold
     * @return the predicate for filtering pairs
     * @throws NullPointerException if the {@link ThresholdOuter#getDataType()} is null
     * @throws IllegalStateException if the {@link ThresholdOuter#getDataType()} is not recognized
     */

    static Predicate<TimeSeries<Pair<Double, Double>>> getFilterForTimeSeriesOfSingleValuedPairs( ThresholdOuter input )
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
     * Processes a set of metric futures for single-valued pairs.
     * 
     * TODO: collapse this with a generic call to futures.addOutput and take an input collection of metrics
     * 
     * @param singleValued the input pairs
     * @param futures the metric futures
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    void processSingleValuedPairs( Pool<Pair<Double, Double>> singleValued,
                                   MetricFuturesByTime.MetricFuturesByTimeBuilder futures )
    {
        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticType.DOUBLE_SCORE );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticType.DIAGRAM );
        }

        if ( this.hasMetrics( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ) )
        {
            this.processSingleValuedPairsByThreshold( singleValued, futures, StatisticType.BOXPLOT_PER_POOL );
        }

    }

    /**
     * Processes one threshold for metrics that consume dichotomous pairs. 
     * 
     * @param pairs the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     */

    void processDichotomousPairs( Pool<Pair<Boolean, Boolean>> pairs,
                                  MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
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
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        // The metrics to compute
        Set<MetricConstants> all = new HashSet<>( collection.getMetrics() );

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        if ( collection.getMetrics().stream().anyMatch( MetricConstants::isSkillMetric ) && pairs.hasBaseline() )
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

        return CompletableFuture.supplyAsync( () -> collection.apply( pairs, all ),
                                              this.thresholdExecutor );
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

    MetricProcessorByTime( ThresholdsByMetricAndFeature metrics,
                           ExecutorService thresholdExecutor,
                           ExecutorService metricExecutor )
    {
        super( metrics, thresholdExecutor, metricExecutor );
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
                                                      MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                      StatisticType outGroup )
    {
        // Filter the thresholds for this group and for the required types
        Map<FeatureTuple, ThresholdsByMetric> filtered = super.getMetrics().getThresholdsByMetricAndFeature();
        filtered = ThresholdSlicer.filterByGroup( filtered,
                                                  SampleDataGroup.SINGLE_VALUED,
                                                  outGroup,
                                                  ThresholdGroup.PROBABILITY,
                                                  ThresholdGroup.VALUE );

        // Unpack the thresholds and add the quantiles, if needed
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
            Map<FeatureTuple, Predicate<Pair<Double, Double>>> slicers =
                    ThresholdSlicer.getFiltersFromThresholds( thresholds,
                                                              MetricProcessorByTime::getFilterForSingleValuedPairs );

            // Add the threshold to the pool metadata            
            ThresholdOuter composed = ThresholdSlicer.compose( Set.copyOf( thresholds.values() ) );
            Pool<Pair<Double, Double>> innerPool = this.addThresholdToPoolMetadata( pool,
                                                                                    OneOrTwoThresholds.of( composed ) );

            Pool<Pair<Double, Double>> sliced = PoolSlicer.filter( innerPool, slicers, PoolSlicer.getFeatureMapper() );

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
                                           MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                           StatisticType outGroup )
    {
        // Don't waste cpu cycles computing statistics for empty pairs
        if ( pairs.get().isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        switch ( outGroup )
        {
            case DOUBLE_SCORE:
                futures.addDoubleScoreOutput( this.processSingleValuedPairs( pairs,
                                                                             this.singleValuedScore ) );
                break;

            case DIAGRAM:
                futures.addDiagramOutput( this.processSingleValuedPairs( pairs,
                                                                         this.singleValuedDiagrams ) );
                break;
            case BOXPLOT_PER_POOL:
                futures.addBoxPlotOutputPerPool( this.processSingleValuedPairs( pairs,
                                                                                this.singleValuedBoxPlot ) );
                break;
            default:
                throw new IllegalStateException( "The statistic group '" + outGroup
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
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        // Log and return an empty result if the sample size is too small
        if ( pairs.get().size() < minimumSampleSize )
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
                return CompletableFuture.supplyAsync( () -> collection.apply( pairs, ss ),
                                                      this.thresholdExecutor );
            }

            return CompletableFuture.completedFuture( List.of() );
        }

        return this.processMetricsRequiredForThisPool( pairs, collection );
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
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

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

}
