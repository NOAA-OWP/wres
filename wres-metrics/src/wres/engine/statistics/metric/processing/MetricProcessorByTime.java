package wres.engine.statistics.metric.processing;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.BasicPool.Builder;
import wres.datamodel.Slicer;
import wres.datamodel.metrics.Metrics;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdGroup;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricCollection;
import wres.engine.statistics.metric.MetricParameterException;

/**
 * A {@link MetricProcessor} that processes and stores metric results by {@link TimeWindowOuter}.
 * 
 * @author james.brown@hydrosolved.com
 */

public abstract class MetricProcessorByTime<S extends Pool<?>>
        extends MetricProcessor<S>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( MetricProcessorByTime.class );

    /**
     * Message that indicates processing is complete.
     */

    static final String PROCESSING_COMPLETE_MESSAGE = "Completed processing of metrics for feature '{}' "
                                                      + "at time window '{}'.";

    /**
     * The metric futures from previous calls, indexed by {@link TimeWindowOuter}.
     */

    List<MetricFuturesByTime> futures = new CopyOnWriteArrayList<>();

    @Override
    public boolean hasCachedMetricOutput()
    {
        return futures.stream()
                      .anyMatch( MetricFuturesByTime::hasFutureOutputs );
    }

    @Override
    StatisticsForProject getCachedMetricOutputInternal()
    {
        MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                new MetricFuturesByTime.MetricFuturesByTimeBuilder();
        if ( this.hasCachedMetricOutput() )
        {
            for ( MetricFuturesByTime future : futures )
            {
                builder.addFutures( future );
            }
        }
        return builder.build().getMetricOutput();
    }

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
                return Slicer.left( input::test );
            case LEFT_AND_RIGHT:
            case LEFT_AND_ANY_RIGHT:
            case LEFT_AND_RIGHT_MEAN:
                return Slicer.leftAndRight( input::test );
            case RIGHT:
            case ANY_RIGHT:
            case RIGHT_MEAN:
                return Slicer.right( input::test );
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
     * Adds the input {@link MetricFuturesByTime} to the internal store of existing {@link MetricFuturesByTime} 
     * defined for this processor.
     * 
     * @param mergeFutures the futures to add
     * @throws MetricOutputMergeException if the outputs cannot be merged across calls
     */

    void addToMergeList( MetricFuturesByTime mergeFutures )
    {
        Objects.requireNonNull( mergeFutures, "Specify non-null futures for merging." );

        //Merge futures if cached outputs identified
        Set<StatisticType> cacheMe = this.getMetricOutputTypesToCache();
        if ( !cacheMe.isEmpty() )
        {
            MetricFuturesByTime.MetricFuturesByTimeBuilder builder =
                    new MetricFuturesByTime.MetricFuturesByTimeBuilder();
            builder.addFutures( mergeFutures, cacheMe );
            this.futures.add( builder.build() );
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
        if ( pairs.getRawData().isEmpty() )
        {
            LOGGER.debug( "Skipping the calculation of statistics for an empty pool of pairs with metadata {}.",
                          pairs.getMetadata() );

            return;
        }

        if ( outGroup == StatisticType.DOUBLE_SCORE )
        {
            futures.addDoubleScoreOutput( this.processDichotomousPairs( pairs, this.dichotomousScalar ) );
        }
    }

    /**
     * Inspects the minimum sample size and, if skill metrics are present and the minimum sample size is not met, 
     * removes the skill metrics from consideration, otherwise leaves them.
     * 
     * @param <U> the left data type
     * @param <V> the right data type
     * @param <T> the type of {@link Statistic}
     * @param pairs the pairs
     * @param collection the metric collection
     * @return the future result
     */

    <U, V, T extends Statistic<?>> Future<List<T>>
            processWithOrWithoutSkillMetrics( Pool<Pair<U, V>> pairs,
                                              MetricCollection<Pool<Pair<U, V>>, T, T> collection )
    {
        int minimumSampleSize = super.getMetrics().getMinimumSampleSize();

        // Are there skill metrics and does the baseline also meet the minimum sample size constraint?
        if ( collection.getMetrics().stream().anyMatch( MetricConstants::isSkillMetric ) && pairs.hasBaseline() )
        {
            int actualBaselineSampleSize = pairs.getBaselineData().getRawData().size();

            if ( actualBaselineSampleSize < minimumSampleSize )
            {
                Set<MetricConstants> all = new HashSet<>( collection.getMetrics() );
                Set<MetricConstants> filtered = all.stream()
                                                   .filter( next -> !next.isSkillMetric() )
                                                   .collect( Collectors.toUnmodifiableSet() );

                // Remove the filtered metrics for logging
                all.removeAll( filtered );

                LOGGER.debug( "While processing pairs for pool {}, discovered {} baseline pairs, which is fewer than "
                              + "the minimum sample size of {} pairs. The following metrics will not be computed for "
                              + "this pool: {}.",
                              pairs.getBaselineData().getMetadata(),
                              actualBaselineSampleSize,
                              minimumSampleSize,
                              all );

                return CompletableFuture.supplyAsync( () -> collection.apply( pairs, filtered ),
                                                      this.thresholdExecutor );
            }
        }

        return CompletableFuture.supplyAsync( () -> collection.apply( pairs ), this.thresholdExecutor );
    }

    /**
     * Constructor.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an {@link ExecutorService} for executing thresholds, cannot be null 
     * @param metricExecutor an {@link ExecutorService} for executing metrics, cannot be null
     * @param mergeSet a list of {@link StatisticType} whose outputs should be retained and merged across calls to
     *            {@link #apply(Object)}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws NullPointerException if a required input is null
     */

    MetricProcessorByTime( final ProjectConfig config,
                           final Metrics metrics,
                           final ExecutorService thresholdExecutor,
                           final ExecutorService metricExecutor,
                           final Set<StatisticType> mergeSet )
            throws MetricParameterException
    {
        super( config, metrics, thresholdExecutor, metricExecutor, mergeSet );
    }

    /**
     * Processes all thresholds for metrics that consume single-valued pairs and produce a specified 
     * {@link StatisticType}. 
     * 
     * @param input the input pairs
     * @param futures the metric futures
     * @param outGroup the metric output type
     * @throws MetricCalculationException if the metrics cannot be computed
     */

    private void processSingleValuedPairsByThreshold( Pool<Pair<Double, Double>> input,
                                                      MetricFuturesByTime.MetricFuturesByTimeBuilder futures,
                                                      StatisticType outGroup )
    {
        // Find the thresholds for this group and for the required types
        ThresholdsByMetric filtered = super.getMetrics().getThresholdsByMetric()
                                                        .filterByGroup( SampleDataGroup.SINGLE_VALUED, outGroup )
                                                        .filterByType( ThresholdGroup.PROBABILITY,
                                                                       ThresholdGroup.VALUE );

        // Find the union across metrics
        Set<ThresholdOuter> union = filtered.union();

        double[] sorted = this.getSortedClimatology( input, union );

        // Iterate the thresholds
        for ( ThresholdOuter threshold : union )
        {
            // Add the quantiles to the threshold
            ThresholdOuter useMe = this.addQuantilesToThreshold( threshold, sorted );
            OneOrTwoThresholds oneOrTwo = OneOrTwoThresholds.of( useMe );

            // Add the threshold to the metadata, in order to fully qualify the pairs
            PoolMetadata baselineMeta = null;
            if ( input.hasBaseline() )
            {
                baselineMeta = PoolMetadata.of( input.getBaselineData().getMetadata(), oneOrTwo );
            }

            Builder<Pair<Double, Double>> builder = new Builder<>();

            Pool<Pair<Double, Double>> pairs = builder.addData( input )
                                                      .setMetadata( PoolMetadata.of( input.getMetadata(),
                                                                                     oneOrTwo ) )
                                                      .setMetadataForBaseline( baselineMeta )
                                                      .build();

            // Filter the data if required
            if ( useMe.isFinite() )
            {
                Predicate<Pair<Double, Double>> filter = MetricProcessorByTime.getFilterForSingleValuedPairs( useMe );

                pairs = Slicer.filter( pairs, filter, null );

            }

            this.processSingleValuedPairs( pairs,
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
        if ( pairs.getRawData().isEmpty() )
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
        if ( pairs.getRawData().size() < minimumSampleSize )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                Set<MetricConstants> collected = new HashSet<>( collection.getMetrics() );
                collected.remove( MetricConstants.SAMPLE_SIZE );

                LOGGER.debug( "While processing pairs for pool {}, discovered {} pairs, which is fewer than the "
                              + "minimum sample size of {} pairs. The following metrics will not be computed for this "
                              + "pool: {}.",
                              pairs.getMetadata(),
                              pairs.getRawData().size(),
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

        return this.processWithOrWithoutSkillMetrics( pairs, collection );
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

        return this.processWithOrWithoutSkillMetrics( pairs, collection );
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

        for ( Pair<Boolean, Boolean> next : pairs.getRawData() )
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
