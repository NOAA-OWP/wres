package wres.datamodel.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.MetricConstants.StatisticType;

/**
 * <p>An immutable store of {@link Statistic} associated with a verification project. This is the top-level
 * container of statistics within an evaluation, but does not require any particular shape of statistics
 * such as one pool or all pools.</p>
 * 
 * <p>Retrieve the statistics using the instance methods for particular {@link StatisticType}. If no statistics
 * exist, the instance methods return null. The store is built with {@link Future} of the {@link Statistic} and the 
 * instance methods call {@link Future#get()}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticsForProject
{

    /**
     * Thread safe map for {@link DoubleScoreStatistic}.
     */

    private final List<Future<List<DoubleScoreStatistic>>> doubleScores = new ArrayList<>();

    /**
     * Thread safe map for {@link DurationScoreStatistic}.
     */

    private final List<Future<List<DurationScoreStatistic>>> durationScores = new ArrayList<>();

    /**
     * Thread safe map for {@link DiagramStatistic}.
     */

    private final List<Future<List<DiagramStatistic>>> diagrams = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotStatistics} for each pair within a pool.
     */

    private final List<Future<List<BoxPlotStatistics>>> boxplotPerPair = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotStatistics} for each pool.
     */

    private final List<Future<List<BoxPlotStatistics>>> boxplotPerPool = new ArrayList<>();

    /**
     * Thread safe map for {@link PairedStatistic}.
     */

    private final List<Future<List<PairedStatistic<Instant, Duration>>>> paired = new ArrayList<>();


    /**
     * Returns a {@link List} of {@link DoubleScoreStatistic}.
     * 
     * @return the double score output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DoubleScoreStatistic> getDoubleScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DOUBLE_SCORE, this.doubleScores );
    }

    /**
     * Returns a {@link List} of {@link DurationScoreStatistic}.
     * 
     * @return the duration score output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DurationScoreStatistic> getDurationScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DURATION_SCORE, this.durationScores );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatistic}.
     * 
     * @return the diagram output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DiagramStatistic> getDiagramStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DIAGRAM, this.diagrams );
    }

    /**
     * Returns a {@link List} of {@link BoxPlotStatistics} per pair.
     * 
     * @return the box plot output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<BoxPlotStatistics> getBoxPlotStatisticsPerPair() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_PAIR, this.boxplotPerPair );
    }

    /**
     * Returns a {@link List} of {@link BoxPlotStatistics} for each pool.
     * 
     * @return the box plot output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<BoxPlotStatistics> getBoxPlotStatisticsPerPool() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_POOL, this.boxplotPerPool );
    }

    /**
     * Returns a {@link List} of {@link PairedStatistic}.
     * 
     * @return the paired output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<PairedStatistic<Instant, Duration>> getInstantDurationPairStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.PAIRED, this.paired );
    }

    /**
     * Returns true if results are available for the input type, false otherwise.
     * 
     * @param outGroup the {@link StatisticType} to test
     * @return true if results are available for the input, false otherwise
     */

    public boolean hasStatistic( StatisticType outGroup )
    {
        switch ( outGroup )
        {
            case DOUBLE_SCORE:
                return !this.doubleScores.isEmpty();
            case DURATION_SCORE:
                return !this.durationScores.isEmpty();
            case DIAGRAM:
                return !this.diagrams.isEmpty();
            case BOXPLOT_PER_PAIR:
                return !this.boxplotPerPair.isEmpty();
            case BOXPLOT_PER_POOL:
                return !this.boxplotPerPool.isEmpty();
            case PAIRED:
                return !this.paired.isEmpty();
            default:
                return false;
        }
    }

    /**
     * Returns all {@link StatisticType} for which outputs are available.
     * 
     * @return all {@link StatisticType} for which outputs are available
     */

    public Set<StatisticType> getStatisticTypes()
    {
        Set<StatisticType> returnMe = new HashSet<>();

        if ( this.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            returnMe.add( StatisticType.DOUBLE_SCORE );
        }

        if ( this.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            returnMe.add( StatisticType.DURATION_SCORE );
        }

        if ( this.hasStatistic( StatisticType.DIAGRAM ) )
        {
            returnMe.add( StatisticType.DIAGRAM );
        }

        if ( this.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            returnMe.add( StatisticType.BOXPLOT_PER_PAIR );
        }

        if ( this.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            returnMe.add( StatisticType.BOXPLOT_PER_POOL );
        }

        if ( this.hasStatistic( StatisticType.PAIRED ) )
        {
            returnMe.add( StatisticType.PAIRED );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Builder.
     */

    public static class Builder
    {

        /**
         * Thread safe map for {@link DoubleScoreStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<List<DoubleScoreStatistic>>> doubleScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DurationScoreStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationScoreStatistic>>> durationScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DiagramStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<List<DiagramStatistic>>> diagramsInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotStatistics} for each pair.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxPlotStatistics>>> boxplotPerPairInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotStatistics} for each pool.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxPlotStatistics>>> boxplotPerPoolInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link PairedStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<List<PairedStatistic<Instant, Duration>>>> pairedInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a new {@link DoubleScoreStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addDoubleScoreStatistics( Future<List<DoubleScoreStatistic>> result )
        {
            doubleScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DurationScoreStatistic} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addDurationScoreStatistics( Future<List<DurationScoreStatistic>> result )
        {
            durationScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DiagramStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addDiagramStatistics( Future<List<DiagramStatistic>> result )
        {
            diagramsInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxPlotStatistics} per pair for a collection of metrics to the internal store, 
         * merging with existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addBoxPlotStatisticsPerPair( Future<List<BoxPlotStatistics>> result )
        {
            boxplotPerPairInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxPlotStatistics} per pool for a collection of metrics to the internal store, 
         * merging with existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addBoxPlotStatisticsPerPool( Future<List<BoxPlotStatistics>> result )
        {
            boxplotPerPoolInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link PairedStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder
                addInstantDurationPairStatistics( Future<List<PairedStatistic<Instant, Duration>>> result )
        {
            pairedInternal.add( result );

            return this;
        }

        /**
         * Adds a an existing set of statistics to the builder.
         * 
         * @param project the statistics
         * @return the builder
         * @throws InterruptedException if the retrieval of the inputs was interrupted
         * @throws NullPointerException if the input is null
         */

        public Builder addStatistics( StatisticsForProject project ) throws InterruptedException
        {
            Objects.requireNonNull( project );

            if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
            {
                this.addDoubleScoreStatistics( CompletableFuture.completedFuture( project.getDoubleScoreStatistics() ) );
            }

            if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
            {
                this.addDurationScoreStatistics( CompletableFuture.completedFuture( project.getDurationScoreStatistics() ) );
            }

            if ( project.hasStatistic( StatisticType.DIAGRAM ) )
            {
                this.addDiagramStatistics( CompletableFuture.completedFuture( project.getDiagramStatistics() ) );
            }

            if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
            {
                this.addBoxPlotStatisticsPerPair( CompletableFuture.completedFuture( project.getBoxPlotStatisticsPerPair() ) );
            }

            if ( project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
            {
                this.addBoxPlotStatisticsPerPool( CompletableFuture.completedFuture( project.getBoxPlotStatisticsPerPool() ) );
            }

            if ( project.hasStatistic( StatisticType.PAIRED ) )
            {
                this.addInstantDurationPairStatistics( CompletableFuture.completedFuture( project.getInstantDurationPairStatistics() ) );
            }

            return this;
        }

        /**
         * Returns a {@link StatisticsForProject}.
         * 
         * @return a {@link StatisticsForProject}
         */

        public StatisticsForProject build()
        {
            return new StatisticsForProject( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private StatisticsForProject( Builder builder )
    {
        doubleScores.addAll( builder.doubleScoreInternal );
        durationScores.addAll( builder.durationScoreInternal );
        diagrams.addAll( builder.diagramsInternal );
        boxplotPerPair.addAll( builder.boxplotPerPairInternal );
        boxplotPerPool.addAll( builder.boxplotPerPoolInternal );
        paired.addAll( builder.pairedInternal );
    }

    /**
     * Unwraps a map of values that are wrapped in {@link Future} by calling {@link Future#get()} on each value and
     * returning a map of the unwrapped entries.
     * 
     * @param <T> the type of statistic
     * @param statsGroup the {@link StatisticType} for error logging
     * @param wrapped the list of values wrapped in {@link Future}
     * @return the unwrapped map
     * @throws InterruptedException if the retrieval is interrupted
     * @throws StatisticException if the result could not be produced
     */

    private <T extends Statistic<?>> List<T> unwrap( StatisticType statsGroup,
                                                     List<Future<List<T>>> wrapped )
            throws InterruptedException
    {
        if ( wrapped.isEmpty() )
        {
            return Collections.emptyList();
        }

        List<T> returnMe = new ArrayList<>();

        for ( Future<List<T>> next : wrapped )
        {
            try
            {
                List<T> result = next.get();

                returnMe.addAll( result );
            }
            catch ( ExecutionException e )
            {
                // Throw an unchecked exception here, as this is not recoverable
                throw new StatisticException( "While retrieving the results for group "
                                              + statsGroup
                                              + ".",
                                              e );
            }
        }

        // Sort the output list in metadata order
        returnMe.sort( ( first, second ) -> first.getMetadata().compareTo( second.getMetadata() ) );

        return Collections.unmodifiableList( returnMe );
    }


}
