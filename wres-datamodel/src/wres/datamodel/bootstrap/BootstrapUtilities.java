package wres.datamodel.bootstrap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;

/**
 * Utilities to assist with bootstrap resampling of data pools.
 *
 * @author James Brown
 */
public class BootstrapUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( BootstrapUtilities.class );

    /**
     * Estimates the optimal block size for each left-ish time-series in each mini-pool and returns the average of the
     * optimal block sizes across all time-series.
     * @param <R> the type of right-ish time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap in timestep units, together with the timestep
     * @throws InsufficientDataForResamplingException if there is insufficient data to calculate the optimal block size
     */
    public static <R> Pair<Long, Duration> getOptimalBlockSizeForStationaryBootstrap( Pool<TimeSeries<Pair<Double, R>>> pool )
    {
        List<Pool<TimeSeries<Pair<Double, R>>>> miniPools = pool.getMiniPools();

        List<Pair<Long, Duration>> blockSizes = new ArrayList<>();
        for ( Pool<TimeSeries<Pair<Double, R>>> next : miniPools )
        {
            // Main data with sufficient samples
            if ( BootstrapUtilities.hasSufficientDataForStationaryBootstrap( next.get() ) )
            {
                Pair<Long, Duration> nextMain =
                        getOptimalBlockSizeForStationaryBootstrap( next.get() );
                blockSizes.add( nextMain );
            }

            // Baseline data with sufficient samples
            if ( next.hasBaseline()
                 && BootstrapUtilities.hasSufficientDataForStationaryBootstrap( next.getBaselineData()
                                                                                    .get() ) )
            {
                List<TimeSeries<Pair<Double, R>>> baseline = next.getBaselineData()
                                                                 .get();
                Pair<Long, Duration> nextBaseline =
                        getOptimalBlockSizeForStationaryBootstrap( baseline );
                blockSizes.add( nextBaseline );
            }
        }

        if ( blockSizes.isEmpty() )
        {
            throw new InsufficientDataForResamplingException( "Insufficient data to "
                                                              + "calculate the optimal "
                                                              + "block size for the "
                                                              + "stationary bootstrap. "
                                                              + "The pool metadata was: "
                                                              + pool.getMetadata() );
        }

        double total = 0;
        Duration totalDuration = Duration.ZERO;
        for ( Pair<Long, Duration> next : blockSizes )
        {
            total += next.getLeft();
            totalDuration = totalDuration.plus( next.getValue() );
        }

        long optimalBlockSize = ( long ) Math.ceil( total / blockSizes.size() );
        Duration averageDuration = totalDuration.dividedBy( blockSizes.size() );

        LOGGER.debug( "Determined an optimal block size of {} timesteps of {} for applying the stationary bootstrap to "
                      + "the pool with metadata: {}. This is an average of the optimal block sizes across all observed "
                      + "time-series within the pool, which included the following block sizes: {}.",
                      optimalBlockSize,
                      averageDuration,
                      pool.getMetadata(),
                      blockSizes );

        return Pair.of( optimalBlockSize, averageDuration );
    }

    /**
     * Determines whether sufficient data is available for bootstrap resampling. There must be more than one event
     * across the consolidated time-series present.
     * @param <T> the type of time-series data
     * @param data the time-series data
     * @return whether there is sufficient data for the stationary bootstrap
     * @throws NullPointerException if the input is null
     */
    public static <T> boolean hasSufficientDataForStationaryBootstrap( List<TimeSeries<T>> data )
    {
        Objects.requireNonNull( data );

        // Consolidate the time-series by event datetime and count the number of events
        long eventCount = data.stream()
                              .flatMap( t -> t.getEvents()
                                              .stream()
                                              .map( Event::getTime ) )
                              .distinct()
                              .count();

        if ( LOGGER.isDebugEnabled() )
        {
            Set<TimeSeriesMetadata> metadatas = data.stream()
                                                    .map( TimeSeries::getMetadata )
                                                    .collect( Collectors.toSet() );

            LOGGER.debug( "Discovered {} events on which to perform the stationary bootstrap. The time-series examined "
                          + "had the following metadata: {}. ", eventCount, metadatas );
        }

        return eventCount > 1;
    }

    /**
     * Returns the subset of quantile statistics for which nominal statistics are available. Since bootstrap sampling
     * can generate "novel" statistics in some cases (i.e., resampled statistics for which nominal statistics) are not
     * available, such as when a minimum sample size is evaluated separately for the nominal statistics and resampled
     * statistics, this method allows the resampled quantiles to be reconciled with the nominal statistics.
     *
     * @param quantiles the quantiles
     * @param nominal the nominal statistics
     * @return the quantiles with corresponding nominal statistics
     * @throws NullPointerException if either input is null
     */

    public static List<Statistics> reconcileQuantilesWithNominalStatistics( List<Statistics> quantiles,
                                                                            List<Statistics> nominal )
    {
        Objects.requireNonNull( quantiles );
        Objects.requireNonNull( nominal );

        // Group the statistics by common pool boundaries
        List<Statistics> adjustedQuantiles = new ArrayList<>();
        for ( Statistics quantile : quantiles )
        {
            Statistics clean = quantile.toBuilder()
                                       .clearDiagrams()
                                       .clearDurationDiagrams()
                                       .clearScores()
                                       .clearDurationScores()
                                       .clearOneBoxPerPair()
                                       .clearOneBoxPerPool()
                                       .clearSummaryStatistic() // Clear the quantile information
                                       .build();

            Predicate<Statistics> filter = statistics ->
                    Objects.equals( statistics.toBuilder()
                                              .clearDiagrams()
                                              .clearDurationDiagrams()
                                              .clearScores()
                                              .clearDurationScores()
                                              .clearOneBoxPerPair()
                                              .clearOneBoxPerPool()
                                              .build(), clean );

            List<Statistics> matched = nominal.stream()
                                              .filter( filter )
                                              .toList();

            if ( !matched.isEmpty() )
            {
                Statistics adjustedQuantile = BootstrapUtilities.reconcileQuantilesWithNominalStatistics( quantile,
                                                                                                          matched );
                adjustedQuantiles.add( adjustedQuantile );
            }
            else if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Nominal statistics were discovered for the quantile with the following metadata: {}",
                              clean );
            }
        }

        return Collections.unmodifiableList( adjustedQuantiles );
    }

    /**
     * Returns the subset of quantile statistics for which nominal statistics are available.
     *
     * @param quantile the quantiles
     * @param nominal the nominal statistics
     * @return the quantiles with corresponding nominal statistics
     * @throws NullPointerException if either input is null
     */

    private static Statistics reconcileQuantilesWithNominalStatistics( Statistics quantile,
                                                                       List<Statistics> nominal )
    {
        Statistics.Builder builder = quantile.toBuilder()
                                             .clearDiagrams()
                                             .clearDurationDiagrams()
                                             .clearScores()
                                             .clearDurationScores()
                                             .clearOneBoxPerPair()
                                             .clearOneBoxPerPool();

        // Diagrams
        List<DiagramStatistic> diagrams = nominal.stream()
                                                 .flatMap( s -> s.getDiagramsList()
                                                                 .stream() )
                                                 .toList();
        BootstrapUtilities.addQuantileForDiagrams( quantile.getDiagramsList(),
                                                   diagrams,
                                                   builder );

        // Duration diagrams
        List<DurationDiagramStatistic> durationDiagrams = nominal.stream()
                                                                 .flatMap( s -> s.getDurationDiagramsList()
                                                                                 .stream() )
                                                                 .toList();
        BootstrapUtilities.addQuantileForDurationDiagrams( quantile.getDurationDiagramsList(),
                                                           durationDiagrams,
                                                           builder );

        // Scores
        List<DoubleScoreStatistic> scores = nominal.stream()
                                                   .flatMap( s -> s.getScoresList()
                                                                   .stream() )
                                                   .toList();
        BootstrapUtilities.addQuantileForScores( quantile.getScoresList(),
                                                 scores,
                                                 builder );

        // Duration scores
        List<DurationScoreStatistic> durationScores = nominal.stream()
                                                             .flatMap( s -> s.getDurationScoresList()
                                                                             .stream() )
                                                             .toList();
        BootstrapUtilities.addQuantileForDurationScores( quantile.getDurationScoresList(),
                                                         durationScores,
                                                         builder );

        // Box plots per pool
        List<BoxplotStatistic> boxPooled = nominal.stream()
                                                  .flatMap( s -> s.getOneBoxPerPoolList()
                                                                  .stream() )
                                                  .toList();
        BootstrapUtilities.addQuantileForBoxplot( quantile.getOneBoxPerPoolList(),
                                                  boxPooled,
                                                  builder::addOneBoxPerPool );

        // Box plots per pair
        List<BoxplotStatistic> boxPaired = nominal.stream()
                                                  .flatMap( s -> s.getOneBoxPerPairList()
                                                                  .stream() )
                                                  .toList();
        BootstrapUtilities.addQuantileForBoxplot( quantile.getOneBoxPerPairList(),
                                                  boxPaired,
                                                  builder::addOneBoxPerPair );

        return builder.build();
    }

    /**
     * Adds the quantile statistics for the supplied diagrams to the builder when a corresponding nominal statistic
     * exists.
     *
     * @param quantile the quantile statistics
     * @param nominal the nominal statistics
     * @param builder the builder to mutate
     */

    private static void addQuantileForDiagrams( List<DiagramStatistic> quantile,
                                                List<DiagramStatistic> nominal,
                                                Statistics.Builder builder )
    {
        for ( DiagramStatistic diagram : quantile )
        {
            MetricName name = diagram.getMetric()
                                     .getName();
            if ( nominal.stream()
                        .anyMatch( d -> d.getMetric()
                                         .getName() == name ) )
            {
                builder.addDiagrams( diagram );
            }
        }
    }

    /**
     * Adds the quantile statistics for the supplied duration diagrams to the builder when a corresponding nominal
     * statistic exists.
     *
     * @param quantile the quantile statistics
     * @param nominal the nominal statistics
     * @param builder the builder to mutate
     */

    private static void addQuantileForDurationDiagrams( List<DurationDiagramStatistic> quantile,
                                                        List<DurationDiagramStatistic> nominal,
                                                        Statistics.Builder builder )
    {
        for ( DurationDiagramStatistic diagram : quantile )
        {
            MetricName name = diagram.getMetric()
                                     .getName();
            if ( nominal.stream()
                        .anyMatch( d -> d.getMetric()
                                         .getName() == name ) )
            {
                builder.addDurationDiagrams( diagram );
            }
        }
    }

    /**
     * Adds the quantile statistics for the supplied scores to the builder when a corresponding nominal statistic
     * exists.
     *
     * @param quantile the quantile statistics
     * @param nominal the nominal statistics
     * @param builder the builder to mutate
     */

    private static void addQuantileForScores( List<DoubleScoreStatistic> quantile,
                                              List<DoubleScoreStatistic> nominal,
                                              Statistics.Builder builder )
    {
        for ( DoubleScoreStatistic score : quantile )
        {
            MetricName name = score.getMetric()
                                   .getName();
            if ( nominal.stream()
                        .anyMatch( d -> d.getMetric()
                                         .getName() == name ) )
            {
                builder.addScores( score );
            }
        }
    }

    /**
     * Adds the quantile statistics for the supplied scores to the builder when a corresponding nominal statistic
     * exists.
     *
     * @param quantile the quantile statistics
     * @param nominal the nominal statistics
     * @param builder the builder to mutate
     */

    private static void addQuantileForDurationScores( List<DurationScoreStatistic> quantile,
                                                      List<DurationScoreStatistic> nominal,
                                                      Statistics.Builder builder )
    {
        for ( DurationScoreStatistic score : quantile )
        {
            MetricName name = score.getMetric()
                                   .getName();
            if ( nominal.stream()
                        .anyMatch( d -> d.getMetric()
                                         .getName() == name ) )
            {
                builder.addDurationScores( score );
            }
        }
    }

    /**
     * Adds the quantile statistics for the supplied box plots to the builder when a corresponding nominal statistic
     * exists.
     *
     * @param quantile the quantile statistics
     * @param nominal the nominal statistics
     * @param consumer the consumer that mutates the builder
     */

    private static void addQuantileForBoxplot( List<BoxplotStatistic> quantile,
                                               List<BoxplotStatistic> nominal,
                                               Consumer<BoxplotStatistic> consumer )
    {
        for ( BoxplotStatistic box : quantile )
        {
            MetricName name = box.getMetric()
                                 .getName();
            if ( nominal.stream()
                        .anyMatch( d -> d.getMetric()
                                         .getName() == name ) )
            {
                consumer.accept( box );
            }
        }
    }

    /**
     * Estimates the optimal block size for the consolidated left-ish time-series in the input, together with the modal
     * timestep of the supplied time-series.
     * @param <T> the type of time-series data
     * @param pool the pool
     * @return the optimal block size for the stationary bootstrap
     * @throws InsufficientDataForResamplingException if there are fewer than two events across the consolidated series
     */
    private static <T> Pair<Long, Duration> getOptimalBlockSizeForStationaryBootstrap( List<TimeSeries<Pair<Double, T>>> pool )
    {
        SortedMap<Instant, Double> consolidated = pool.stream()
                                                      .flatMap( n -> n.getEvents()
                                                                      .stream() )
                                                      .collect( Collectors.toMap( Event::getTime, e -> e.getValue()
                                                                                                        .getLeft(),
                                                                                  ( a, b ) -> a, TreeMap::new ) );

        double[] data = new double[consolidated.size()];
        List<Duration> durations = new ArrayList<>();
        Instant last = null;
        int index = 0;
        for ( Map.Entry<Instant, Double> next : consolidated.entrySet() )
        {
            data[index] = next.getValue();
            Instant time = next.getKey();

            if ( index > 0 )
            {
                Duration between = Duration.between( last, time );
                durations.add( between );
            }

            last = time;
            index++;
        }

        long optimalBlockSize = BlockSizeEstimator.getOptimalBlockSize( data );

        // Find the corresponding timestep, which is the modal timestep
        Set<TimeSeriesMetadata> metadatas = pool.stream()
                                                .map( TimeSeries::getMetadata )
                                                .collect( Collectors.toSet() );
        Duration modalTimestep =
                durations.stream()
                         .collect( Collectors.groupingBy( Function.identity(),
                                                          Collectors.counting() ) )
                         .entrySet()
                         .stream()
                         .max( Map.Entry.comparingByValue() )
                         .map( Map.Entry::getKey )
                         .orElseThrow( () -> new InsufficientDataForResamplingException( "Insufficient data to "
                                                                                         + "calculate the optimal "
                                                                                         + "block size for the "
                                                                                         + "stationary bootstrap. "
                                                                                         + "The time-series examined "
                                                                                         + "had the following metadata: "
                                                                                         + metadatas ) );

        return Pair.of( optimalBlockSize, modalTimestep );
    }

    /**
     * Do not construct.
     */
    private BootstrapUtilities()
    {
    }
}
