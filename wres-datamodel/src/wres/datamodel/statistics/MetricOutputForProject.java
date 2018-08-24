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

import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.statistics.ListOfMetricOutput.ListOfMetricOutputBuilder;

/**
 * <p>An immutable store of {@link MetricOutput} associated with a verification project.</p>
 * 
 * <p>Retrieve the outputs using the instance methods for particular {@link MetricOutputGroup}. If no outputs exist, 
 * the instance methods return null. The store is built with {@link Future} of the {@link MetricOutput} and the 
 * instance methods call {@link Future#get()}.</p>
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricOutputForProject
{

    /**
     * Thread safe map for {@link DoubleScoreOutput}.
     */

    private final List<Future<ListOfMetricOutput<DoubleScoreOutput>>> doubleScore = new ArrayList<>();

    /**
     * Thread safe map for {@link DurationScoreOutput}.
     */

    private final List<Future<ListOfMetricOutput<DurationScoreOutput>>> durationScore = new ArrayList<>();

    /**
     * Thread safe map for {@link MultiVectorOutput}.
     */

    private final List<Future<ListOfMetricOutput<MultiVectorOutput>>> multiVector = new ArrayList<>();

    /**
     * Thread safe map for {@link MatrixOutput}.
     */

    private final List<Future<ListOfMetricOutput<MatrixOutput>>> matrix = new ArrayList<>();

    /**
     * Thread safe map for {@link BoxPlotOutput}.
     */

    private final List<Future<ListOfMetricOutput<BoxPlotOutput>>> boxplot = new ArrayList<>();

    /**
     * Thread safe map for {@link PairedOutput}.
     */

    private final List<Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>>> paired = new ArrayList<>();

    /**
     * Returns a {@link ListOfMetricOutput} of {@link DoubleScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<DoubleScoreOutput> getDoubleScoreOutput()
            throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.DOUBLE_SCORE, doubleScore );
    }

    /**
     * Returns a {@link ListOfMetricOutput} of {@link DurationScoreOutput} or null if no output exists.
     * 
     * @return the scalar output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<DurationScoreOutput> getDurationScoreOutput()
            throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.DURATION_SCORE, durationScore );
    }

    /**
     * Returns a {@link ListOfMetricOutput} of {@link MultiVectorOutput} or null if no output exists.
     * 
     * @return the multi-vector output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<MultiVectorOutput> getMultiVectorOutput()
            throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.MULTIVECTOR, multiVector );
    }

    /**
     * Returns a {@link ListOfMetricOutput} of {@link MatrixOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<MatrixOutput> getMatrixOutput() throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.MATRIX, matrix );
    }

    /**
     * Returns a {@link ListOfMetricOutput} of {@link BoxPlotOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<BoxPlotOutput> getBoxPlotOutput() throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.BOXPLOT, boxplot );
    }

    /**
     * Returns a {@link ListOfMetricOutput} of {@link PairedOutput} or null if no output exists.
     * 
     * @return the matrix output or null
     * @throws MetricOutputException if the output could not be retrieved
     * @throws InterruptedException if the retrieval was interrupted
     */

    public ListOfMetricOutput<PairedOutput<Instant, Duration>> getPairedOutput()
            throws InterruptedException
    {
        return this.unwrap( MetricOutputGroup.PAIRED, paired );
    }

    /**
     * Returns true if results are available for the input type, false otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup} to test
     * @return true if results are available for the input, false otherwise
     */

    public boolean hasOutput( MetricOutputGroup outGroup )
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
     * Returns all {@link MetricOutputGroup} for which outputs are available.
     * 
     * @return all {@link MetricOutputGroup} for which outputs are available
     */

    public Set<MetricOutputGroup> getOutputTypes()
    {
        Set<MetricOutputGroup> returnMe = new HashSet<>();

        if ( hasOutput( MetricOutputGroup.DOUBLE_SCORE ) )
        {
            returnMe.add( MetricOutputGroup.DOUBLE_SCORE );
        }

        if ( hasOutput( MetricOutputGroup.DURATION_SCORE ) )
        {
            returnMe.add( MetricOutputGroup.DURATION_SCORE );
        }

        if ( hasOutput( MetricOutputGroup.MULTIVECTOR ) )
        {
            returnMe.add( MetricOutputGroup.MULTIVECTOR );
        }

        if ( hasOutput( MetricOutputGroup.MATRIX ) )
        {
            returnMe.add( MetricOutputGroup.MATRIX );
        }

        if ( hasOutput( MetricOutputGroup.BOXPLOT ) )
        {
            returnMe.add( MetricOutputGroup.BOXPLOT );
        }

        if ( hasOutput( MetricOutputGroup.PAIRED ) )
        {
            returnMe.add( MetricOutputGroup.PAIRED );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Builder.
     */

    public static class MetricOutputForProjectBuilder
    {

        /**
         * Thread safe map for {@link DoubleScoreOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<DoubleScoreOutput>>> doubleScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link DurationScoreOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<DurationScoreOutput>>> durationScoreInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link MultiVectorOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<MultiVectorOutput>>> multiVectorInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link MatrixOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<MatrixOutput>>> matrixInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link BoxPlotOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<BoxPlotOutput>>> boxplotInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Thread safe map for {@link PairedOutput}.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>>> pairedInternal =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a new {@link DoubleScoreOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addDoubleScoreOutput( Future<ListOfMetricOutput<DoubleScoreOutput>> result )
        {
            doubleScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link DurationScoreOutput} for a collection of metrics to the internal store, merging with 
         * existing items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addDurationScoreOutput( Future<ListOfMetricOutput<DurationScoreOutput>> result )
        {
            durationScoreInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link MultiVectorOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addMultiVectorOutput( Future<ListOfMetricOutput<MultiVectorOutput>> result )
        {
            multiVectorInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link MatrixOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addMatrixOutput( Future<ListOfMetricOutput<MatrixOutput>> result )
        {
            matrixInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link BoxPlotOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addBoxPlotOutput( Future<ListOfMetricOutput<BoxPlotOutput>> result )
        {
            boxplotInternal.add( result );

            return this;
        }

        /**
         * Adds a new {@link PairedOutput} for a collection of metrics to the internal store, merging with existing 
         * items that share the same key, as required.
         * 
         * @param result the result
         * @return the builder
         */

        public MetricOutputForProjectBuilder
                addPairedOutput( Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>> result )
        {
            pairedInternal.add( result );

            return this;
        }

        /**
         * Returns a {@link MetricOutputForProject}.
         * 
         * @return a {@link MetricOutputForProject}
         */

        public MetricOutputForProject build()
        {
            return new MetricOutputForProject( this );
        }

    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MetricOutputForProject( MetricOutputForProjectBuilder builder )
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
     * @param <T> the type of output
     * @param outGroup the {@link MetricOutputGroup} for error logging
     * @param wrapped the list of values wrapped in {@link Future}
     * @return the unwrapped map or null if the input is empty
     * @throws InterruptedException if the retrieval is interrupted
     * @throws MetricOutputException if the result could not be produced
     */

    private <T extends MetricOutput<?>> ListOfMetricOutput<T> unwrap( MetricOutputGroup outGroup,
                                                                      List<Future<ListOfMetricOutput<T>>> wrapped )
            throws InterruptedException
    {
        if ( wrapped.isEmpty() )
        {
            return null;
        }

        ListOfMetricOutputBuilder<T> builder = new ListOfMetricOutputBuilder<>();
        
        // Sort the output list in metadata order
        builder.setSorter( ( first, second ) -> first.getMetadata().compareTo( second.getMetadata() ) );

        for ( Future<ListOfMetricOutput<T>> next : wrapped )
        {
            try
            {
                ListOfMetricOutput<T> result = next.get();

                result.forEach( builder::addOutput );
            }
            catch ( InterruptedException e )
            {
                // Decorate for context, use .initCause method to chain.
                throw (InterruptedException) new InterruptedException( "Interrupted while retrieving the results "
                                                                       + "for group "
                                                                       + outGroup
                                                                       + "." ).initCause( e );
            }
            catch ( ExecutionException e )
            {
                // Throw an unchecked exception here, as this is not recoverable
                throw new MetricOutputException( "While retrieving the results for group "
                                                 + outGroup
                                                 + ".",
                                                 e );
            }
        }

        return builder.build();
    }


}
