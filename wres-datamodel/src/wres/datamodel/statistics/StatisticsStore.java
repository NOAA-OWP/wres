package wres.datamodel.statistics;

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

import wres.datamodel.metrics.MetricConstants.StatisticType;

/**
 * An immutable store of statistics.
 * 
 * @author James Brown
 */

public class StatisticsStore
{
    /**
     * Thread safe map for {@link DoubleScoreStatisticOuter}.
     */

    private final List<Future<List<DoubleScoreStatisticOuter>>> doubleScores = new ArrayList<>();

    /**
     * Thread safe map for {@link DurationScoreStatisticOuter}.
     */

    private final List<Future<List<DurationScoreStatisticOuter>>> durationScores = new ArrayList<>();

    /**
     * Thread safe map for {@link DiagramStatisticOuter}.
     */

    private final List<Future<List<DiagramStatisticOuter>>> diagrams = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxplotStatisticOuter} for each pair within a pool.
     */

    private final List<Future<List<BoxplotStatisticOuter>>> boxplotPerPair = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxplotStatisticOuter} for each pool.
     */

    private final List<Future<List<BoxplotStatisticOuter>>> boxplotPerPool = new ArrayList<>();

    /**
     * Thread safe map for {@link DurationDiagramStatisticOuter}.
     */

    private final List<Future<List<DurationDiagramStatisticOuter>>> paired = new ArrayList<>();

    /**
     * Minimum sample size used when forming the statistics.
     */

    private final int minimumSampleSize;

    /**
     * Combines the input store with the current store into a new store (without mutating the current store). This 
     * method is eager and brings forward the calculation of all statistics in both stores.
     * 
     * @param input the input store
     * @return the combined store
     * @throws NullPointerException if the input is null
     */

    public StatisticsStore combine( StatisticsStore input )
    {
        Objects.requireNonNull( input );

        try
        {
            return new Builder().setMinimumSampleSize( this.minimumSampleSize )
                                .addStatistics( input )
                                .addStatistics( this )
                                .build();
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread()
                  .interrupt();

            throw new StatisticException( "Interrupted while attempting to coimbine two statistics stores.", e );
        }
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter}.
     * 
     * @return the double score output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DoubleScoreStatisticOuter> getDoubleScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DOUBLE_SCORE, this.doubleScores );
    }

    /**
     * Returns a {@link List} of {@link DurationScoreStatisticOuter}.
     * 
     * @return the duration score output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DurationScoreStatisticOuter> getDurationScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DURATION_SCORE, this.durationScores );
    }

    /**
     * Returns a {@link List} of {@link DiagramStatisticOuter}.
     * 
     * @return the diagram output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DiagramStatisticOuter> getDiagramStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DIAGRAM, this.diagrams );
    }

    /**
     * Returns a {@link List} of {@link BoxplotStatisticOuter} per pair.
     * 
     * @return the box plot output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<BoxplotStatisticOuter> getBoxPlotStatisticsPerPair() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_PAIR, this.boxplotPerPair );
    }

    /**
     * Returns a {@link List} of {@link BoxplotStatisticOuter} for each pool.
     * 
     * @return the box plot output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<BoxplotStatisticOuter> getBoxPlotStatisticsPerPool() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_POOL, this.boxplotPerPool );
    }

    /**
     * Returns a {@link List} of {@link DurationDiagramStatisticOuter}.
     * 
     * @return the paired output
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public List<DurationDiagramStatisticOuter> getInstantDurationPairStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DURATION_DIAGRAM, this.paired );
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
            case DURATION_DIAGRAM:
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

        if ( this.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            returnMe.add( StatisticType.DURATION_DIAGRAM );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * @return the minimum sample size used when calculating statistics.
     */

    public int getMinimumSampleSize()
    {
        return this.minimumSampleSize;
    }

    /**
     * Builder.
     */

    public static class Builder
    {

        /**
         * Thread safe map for {@link DoubleScoreStatisticOuter}.
         */

        private final ConcurrentLinkedQueue<Future<List<DoubleScoreStatisticOuter>>> doubleScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DurationScoreStatisticOuter}.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationScoreStatisticOuter>>> durationScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DiagramStatisticOuter}.
         */

        private final ConcurrentLinkedQueue<Future<List<DiagramStatisticOuter>>> diagramsInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxplotStatisticOuter} for each pair.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxplotStatisticOuter>>> boxplotPerPairInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxplotStatisticOuter} for each pool.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxplotStatisticOuter>>> boxplotPerPoolInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DurationDiagramStatisticOuter}.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationDiagramStatisticOuter>>> pairedInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Minimum sample size used when forming the statistics.
         */

        private int minimumSampleSize;

        /**
         * Adds a new {@link DoubleScoreStatisticOuter} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addDoubleScoreStatistics( Future<List<DoubleScoreStatisticOuter>> result )
        {
            this.doubleScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DurationScoreStatisticOuter} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addDurationScoreStatistics( Future<List<DurationScoreStatisticOuter>> result )
        {
            this.durationScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DiagramStatisticOuter} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addDiagramStatistics( Future<List<DiagramStatisticOuter>> result )
        {
            this.diagramsInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxplotStatisticOuter} per pair for a collection of metrics to the internal store, 
         * merging with existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addBoxPlotStatisticsPerPair( Future<List<BoxplotStatisticOuter>> result )
        {
            this.boxplotPerPairInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxplotStatisticOuter} per pool for a collection of metrics to the internal store, 
         * merging with existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addBoxPlotStatisticsPerPool( Future<List<BoxplotStatisticOuter>> result )
        {
            this.boxplotPerPoolInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DurationDiagramStatisticOuter} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public Builder addInstantDurationPairStatistics( Future<List<DurationDiagramStatisticOuter>> result )
        {
            this.pairedInternal.add( result );

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

        public Builder addStatistics( StatisticsStore project ) throws InterruptedException
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

            if ( project.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
            {
                this.addInstantDurationPairStatistics( CompletableFuture.completedFuture( project.getInstantDurationPairStatistics() ) );
            }

            return this;
        }

        /**
         * Sets the minimum sample size for statistics calculation.
         * 
         * @param minimumSampleSize the minimum sample size
         * @return the builder
         */

        public Builder setMinimumSampleSize( int minimumSampleSize )
        {
            this.minimumSampleSize = minimumSampleSize;
            return this;
        }

        /**
         * Returns a {@link StatisticsStore}.
         * 
         * @return a {@link StatisticsStore}
         */

        public StatisticsStore build()
        {
            return new StatisticsStore( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private StatisticsStore( Builder builder )
    {
        this.doubleScores.addAll( builder.doubleScoreInternal );
        this.durationScores.addAll( builder.durationScoreInternal );
        this.diagrams.addAll( builder.diagramsInternal );
        this.boxplotPerPair.addAll( builder.boxplotPerPairInternal );
        this.boxplotPerPool.addAll( builder.boxplotPerPoolInternal );
        this.paired.addAll( builder.pairedInternal );
        this.minimumSampleSize = builder.minimumSampleSize;
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
