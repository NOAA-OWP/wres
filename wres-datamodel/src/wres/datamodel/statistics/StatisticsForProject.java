package wres.datamodel.statistics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.statistics.ListOfStatistics.ListOfStatisticsBuilder;

/**
 * <p>An immutable store of {@link Statistic} associated with a verification project.</p>
 * 
 * <p>Retrieve the statistics using the instance methods for particular {@link StatisticGroup}. If no statistics
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
     * Thread safe map for {@link MultiVectorStatistic}.
     */

    private final List<Future<ListOfStatistics<MultiVectorStatistic>>> multiVector = new ArrayList<>();

    /**
     * Thread safe map for {@link MatrixStatistic}.
     */

    private final List<Future<ListOfStatistics<MatrixStatistic>>> matrix = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotStatistic}.
     */

    private final List<Future<ListOfStatistics<BoxPlotStatistic>>> boxplot = new ArrayList<>();

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
        return this.unwrap( StatisticGroup.DOUBLE_SCORE, doubleScore );
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
        return this.unwrap( StatisticGroup.DURATION_SCORE, durationScore );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link MultiVectorStatistic} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<MultiVectorStatistic> getMultiVectorStatistics()
            throws InterruptedException
    {
        return this.unwrap( StatisticGroup.MULTIVECTOR, multiVector );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link MatrixStatistic} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<MatrixStatistic> getMatrixStatistics() throws InterruptedException
    {
        return this.unwrap( StatisticGroup.MATRIX, matrix );
    }

    /**
     * Returns a {@link ListOfStatistics} of {@link BoxPlotStatistic} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws StatisticException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfStatistics<BoxPlotStatistic> getBoxPlotStatistics() throws InterruptedException
    {
        return this.unwrap( StatisticGroup.BOXPLOT, boxplot );
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
        return this.unwrap( StatisticGroup.PAIRED, paired );
    }

    /**
     * Returns true if results are available for the input type, false otherwise.
     * 
     * @param outGroup the {@link StatisticGroup} to test
     * @return true if results are available for the input, false otherwise
     */

    public boolean hasStatistic( StatisticGroup outGroup )
    {
        switch ( outGroup )
        {
            case DOUBLE_SCORE:
                return !doubleScore.isEmpty();
            case DURATION_SCORE:
                return !durationScore.isEmpty();
            case MULTIVECTOR:
                return !multiVector.isEmpty();
            case MATRIX:
                return !matrix.isEmpty();
            case BOXPLOT:
                return !boxplot.isEmpty();
            case PAIRED:
                return !paired.isEmpty();
            default:
                return false;
        }
    }

    /**
     * Returns all {@link StatisticGroup} for which outputs are available.
     * 
     * @return all {@link StatisticGroup} for which outputs are available
     */

    public Set<StatisticGroup> getStatisticTypes()
    {
        Set<StatisticGroup> returnMe = new HashSet<>();

        if ( hasStatistic( StatisticGroup.DOUBLE_SCORE ) )
        {
            returnMe.add( StatisticGroup.DOUBLE_SCORE );
        }

        if ( hasStatistic( StatisticGroup.DURATION_SCORE ) )
        {
            returnMe.add( StatisticGroup.DURATION_SCORE );
        }

        if ( hasStatistic( StatisticGroup.MULTIVECTOR ) )
        {
            returnMe.add( StatisticGroup.MULTIVECTOR );
        }

        if ( hasStatistic( StatisticGroup.MATRIX ) )
        {
            returnMe.add( StatisticGroup.MATRIX );
        }

        if ( hasStatistic( StatisticGroup.BOXPLOT ) )
        {
            returnMe.add( StatisticGroup.BOXPLOT );
        }

        if ( hasStatistic( StatisticGroup.PAIRED ) )
        {
            returnMe.add( StatisticGroup.PAIRED );
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
         * Thread safe map for {@link MultiVectorStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<MultiVectorStatistic>>> multiVectorInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link MatrixStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<MatrixStatistic>>> matrixInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotStatistic}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<BoxPlotStatistic>>> boxplotInternal =
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
                addDoubleScoreOutput( Future<ListOfStatistics<DoubleScoreStatistic>> result )
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
         * Adds a new {@link MultiVectorStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public StatisticsForProjectBuilder
                addMultiVectorStatistics( Future<ListOfStatistics<MultiVectorStatistic>> result )
        {
            multiVectorInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link MatrixStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public StatisticsForProjectBuilder
                addMatrixStatistics( Future<ListOfStatistics<MatrixStatistic>> result )
        {
            matrixInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxPlotStatistic} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public StatisticsForProjectBuilder
                addBoxPlotStatistics( Future<ListOfStatistics<BoxPlotStatistic>> result )
        {
            boxplotInternal.add( result );

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
        matrix.addAll( builder.matrixInternal );
        boxplot.addAll( builder.boxplotInternal );
        paired.addAll( builder.pairedInternal );
    }

    /**
     * Unwraps a map of values that are wrapped in {@link Future} by calling {@link Future#get()} on each value and
     * returning a map of the unwrapped entries.
     * 
     * @param <T> the type of statistic
     * @param statsGroup the {@link StatisticGroup} for error logging
     * @param wrapped the list of values wrapped in {@link Future}
     * @return the unwrapped map or null if the input is empty
     * @throws InterruptedException if the retrieval is interrupted
     * @throws StatisticException if the result could not be produced
     */

    private <T extends Statistic<?>> ListOfStatistics<T> unwrap( StatisticGroup statsGroup,
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
            catch ( InterruptedException e )
            {
                // Decorate for context, use .initCause method to chain.
                throw (InterruptedException) new InterruptedException( "Interrupted while retrieving the results "
                                                                       + "for group "
                                                                       + statsGroup
                                                                       + "." ).initCause( e );
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
