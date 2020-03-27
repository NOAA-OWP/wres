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
import wres.datamodel.statistics.ListOfStatistics.ListOfStatisticsBuilder;

/**
 * <p>An immutable store of {@link Statistic} associated with a verification project.</p>
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

    private final List<Future<ListOfStatistics<DoubleScoreStatistic>>> doubleScore = new ArrayList<>();

    /**
     * Thread safe map for {@link DurationScoreStatistic}.
     */

    private final List<Future<ListOfStatistics<DurationScoreStatistic>>> durationScore = new ArrayList<>();

    /**
     * Thread safe map for {@link DiagramStatistic}.
     */

    private final List<Future<ListOfStatistics<DiagramStatistic>>> multiVector = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotStatistics} for each pair within a pool.
     */

    private final List<Future<ListOfStatistics<BoxPlotStatistics>>> boxplotPerPair = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotStatistics} for each pool.
     */

    private final List<Future<ListOfStatistics<BoxPlotStatistics>>> boxplotPerPool = new ArrayList<>();

    /**
     * Thread safe map for {@link PairedStatistic}.
     */

    private final List<Future<ListOfStatistics<PairedStatistic<Instant, Duration>>>> paired = new ArrayList<>();


    /**
     * Returns a {@link ListOfStatistics} of {@link DoubleScoreStatistic} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<DoubleScoreStatistic> getDoubleScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DOUBLE_SCORE, this.doubleScore );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link DurationScoreStatistic} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<DurationScoreStatistic> getDurationScoreStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.DURATION_SCORE, this.durationScore );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link DiagramStatistic} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<DiagramStatistic> getMultiVectorStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticType.MULTIVECTOR, this.multiVector );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link BoxPlotStatistics} per pair or null if no 
     * output exists.
     * 
     * @return the box plot output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<BoxPlotStatistics> getBoxPlotStatisticsPerPair() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_PAIR, this.boxplotPerPair );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link BoxPlotStatistics} for each pool or null if no 
     * output exists.
     * 
     * @return the box plot output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<BoxPlotStatistics> getBoxPlotStatisticsPerPool() throws InterruptedException
    {
        return this.unwrap( StatisticType.BOXPLOT_PER_POOL, this.boxplotPerPool );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link PairedStatistic} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<PairedStatistic<Instant, Duration>> getPairedStatistics()
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
                return !this.doubleScore.isEmpty();
            case DURATION_SCORE:
                return !this.durationScore.isEmpty();
            case MULTIVECTOR:
                return !this.multiVector.isEmpty();
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

        if ( this.hasStatistic( StatisticType.MULTIVECTOR ) )
        {
            returnMe.add( StatisticType.MULTIVECTOR );
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

    public static class StatisticsForProjectBuilder
    {

        /**
         * Thread safe map for {@link DoubleScoreStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<DoubleScoreStatistic>>> doubleScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DurationScoreStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<DurationScoreStatistic>>> durationScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DiagramStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<DiagramStatistic>>> multiVectorInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotStatistics} for each pair.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<BoxPlotStatistics>>> boxplotPerPairInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotStatistics} for each pool.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<BoxPlotStatistics>>> boxplotPerPoolInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link PairedStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<PairedStatistic<Instant, Duration>>>> pairedInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a new {@link DoubleScoreStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public StatisticsForProjectBuilder
                addDoubleScoreStatistics( Future<ListOfStatistics<DoubleScoreStatistic>> result )
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

        public StatisticsForProjectBuilder
                addDurationScoreStatistics( Future<ListOfStatistics<DurationScoreStatistic>> result )
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

        public StatisticsForProjectBuilder
                addMultiVectorStatistics( Future<ListOfStatistics<DiagramStatistic>> result )
        {
            multiVectorInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxPlotStatistics} per pair for a collection of metrics to the internal store, 
         * merging with existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public StatisticsForProjectBuilder
                addBoxPlotStatisticsPerPair( Future<ListOfStatistics<BoxPlotStatistics>> result )
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

        public StatisticsForProjectBuilder
                addBoxPlotStatisticsPerPool( Future<ListOfStatistics<BoxPlotStatistics>> result )
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

        public StatisticsForProjectBuilder
                addPairedStatistics( Future<ListOfStatistics<PairedStatistic<Instant, Duration>>> result )
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

        public StatisticsForProjectBuilder addStatistics( StatisticsForProject project ) throws InterruptedException
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

            if ( project.hasStatistic( StatisticType.MULTIVECTOR ) )
            {
                this.addMultiVectorStatistics( CompletableFuture.completedFuture( project.getMultiVectorStatistics() ) );
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
                this.addPairedStatistics( CompletableFuture.completedFuture( project.getPairedStatistics() ) );
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

    private StatisticsForProject( StatisticsForProjectBuilder builder )
    {
        doubleScore.addAll( builder.doubleScoreInternal );
        durationScore.addAll( builder.durationScoreInternal );
        multiVector.addAll( builder.multiVectorInternal );
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
     * @return the unwrapped map or null if the input is empty
     * @throws InterruptedException if the retrieval is interrupted
     * @throws StatisticException if the result could not be produced
     */

    private <T extends Statistic<?>> ListOfStatistics<T> unwrap( StatisticType statsGroup,
                                                                 List<Future<ListOfStatistics<T>>> wrapped )
            throws InterruptedException
    {
        if ( wrapped.isEmpty() )
        {
            return null;
        }

        ListOfStatisticsBuilder<T> builder = new ListOfStatisticsBuilder<>();

        // Sort the output list in metadata order
        builder.setSorter( ( first, second ) -> first.getMetadata().compareTo( second.getMetadata() ) );

        for ( Future<ListOfStatistics<T>> next : wrapped )
        {
            try
            {
                ListOfStatistics<T> result = next.get();

                result.forEach( builder::addStatistic );
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

        return builder.build();
    }


}
