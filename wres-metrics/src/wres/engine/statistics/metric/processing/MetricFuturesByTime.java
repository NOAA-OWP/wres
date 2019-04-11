package wres.engine.statistics.metric.processing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * Store of metric futures for each output type. Use {@link #getMetricOutput()} to obtain the processed
 * {@link StatisticsForProject}.
 * 
 * @author james.brown@hydrosolved.com
 */

class MetricFuturesByTime
{

    /**
     * {@link DoubleScoreStatistic} results.
     */

    private final List<Future<ListOfStatistics<DoubleScoreStatistic>>> doubleScore = new ArrayList<>();

    /**
     * {@link DurationScoreStatistic} results.
     */

    private final List<Future<ListOfStatistics<DurationScoreStatistic>>> durationScore = new ArrayList<>();

    /**
     * {@link MultiVectorStatistic} results.
     */

    private final List<Future<ListOfStatistics<MultiVectorStatistic>>> multiVector = new ArrayList<>();

    /**
     * {@link BoxPlotStatistics} results.
     */

    private final List<Future<ListOfStatistics<BoxPlotStatistics>>> boxplot = new ArrayList<>();

    /**
     * {@link PairedStatistic} results.
     */

    private final List<Future<ListOfStatistics<PairedStatistic<Instant, Duration>>>> paired = new ArrayList<>();

    /**
     * {@link MatrixStatistic} results.
     */

    private final List<Future<ListOfStatistics<MatrixStatistic>>> matrix = new ArrayList<>();

    /**
     * Returns the results associated with the futures.
     * 
     * @return the metric results
     */

    StatisticsForProject getMetricOutput()
    {
        StatisticsForProjectBuilder builder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        //Add outputs for current futures
        doubleScore.forEach( builder::addDoubleScoreOutput );
        durationScore.forEach( builder::addDurationScoreStatistics );
        multiVector.forEach( builder::addMultiVectorStatistics );
        boxplot.forEach( builder::addBoxPlotStatistics );
        paired.forEach( builder::addPairedStatistics );
        matrix.forEach( builder::addMatrixStatistics );
        return builder.build();
    }

    /**
     * Returns the {@link StatisticGroup} for which futures exist.
     * 
     * @return the set of output types for which futures exist
     */

    Set<StatisticGroup> getOutputTypes()
    {
        Set<StatisticGroup> returnMe = new HashSet<>();

        if ( !this.doubleScore.isEmpty() )
        {
            returnMe.add( StatisticGroup.DOUBLE_SCORE );
        }

        if ( !this.durationScore.isEmpty() )
        {
            returnMe.add( StatisticGroup.DURATION_SCORE );
        }

        if ( !this.multiVector.isEmpty() )
        {
            returnMe.add( StatisticGroup.MULTIVECTOR );
        }

        if ( !this.boxplot.isEmpty() )
        {
            returnMe.add( StatisticGroup.BOXPLOT );
        }

        if ( !this.paired.isEmpty() )
        {
            returnMe.add( StatisticGroup.PAIRED );
        }

        if ( !this.matrix.isEmpty() )
        {
            returnMe.add( StatisticGroup.MATRIX );
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Returns true if one or more future outputs is available, false otherwise.
     * 
     * @return true if one or more future outputs is available, false otherwise
     */

    boolean hasFutureOutputs()
    {
        return !this.getOutputTypes().isEmpty();
    }

    /**
     * A builder for the metric futures.
     */

    static class MetricFuturesByTimeBuilder
    {

        /**
         * {@link DoubleScoreStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<DoubleScoreStatistic>>> doubleScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DurationScoreStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<DurationScoreStatistic>>> durationScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link MultiVectorStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<MultiVectorStatistic>>> multiVector =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link BoxPlotStatistics} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<BoxPlotStatistics>>> boxplot =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link PairedStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<PairedStatistic<Instant, Duration>>>> paired =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link MatrixStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfStatistics<MatrixStatistic>>> matrix =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a set of future {@link DoubleScoreStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Future<ListOfStatistics<DoubleScoreStatistic>> value )
        {
            this.doubleScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DurationScoreStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationScoreOutput( Future<ListOfStatistics<DurationScoreStatistic>> value )
        {
            this.durationScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MultiVectorStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMultiVectorOutput( Future<ListOfStatistics<MultiVectorStatistic>> value )
        {
            this.multiVector.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxPlotStatistics} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutput( Future<ListOfStatistics<BoxPlotStatistics>> value )
        {
            this.boxplot.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addPairedOutput( Future<ListOfStatistics<PairedStatistic<Instant, Duration>>> value )
        {
            this.paired.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMatrixOutput( Future<ListOfStatistics<MatrixStatistic>> value )
        {
            this.matrix.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DoubleScoreStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<ListOfStatistics<DoubleScoreStatistic>> value )
        {
            Objects.requireNonNull( key.getLeft() );
            
            this.doubleScore.add( value );

            return this;
        }

        /**
         * Build the metric futures.
         * 
         * @return the metric futures
         */

        MetricFuturesByTime build()
        {
            return new MetricFuturesByTime( this );
        }

        /**
         * Adds the outputs from an existing {@link MetricFuturesByTime} for the outputs that are included in the
         * merge list.
         * 
         * @param futures the input futures
         * @param mergeSet the merge list
         * @return the builder
         * @throws MetricOutputMergeException if the outputs cannot be merged across calls
         */

        MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures )
        {
            this.addFutures( futures, StatisticGroup.set() );
            return this;
        }

        /**
         * Adds the outputs from an existing {@link MetricFuturesByTime} for the outputs that are included in the
         * merge list.
         * 
         * @param futures the input futures
         * @param mergeSet the merge list
         * @return the builder
         * @throws MetricOutputMergeException if the outputs cannot be merged across calls
         */

        MetricFuturesByTimeBuilder addFutures( MetricFuturesByTime futures,
                                               Set<StatisticGroup> mergeSet )
        {
            if ( Objects.nonNull( mergeSet ) )
            {
                for ( StatisticGroup nextGroup : mergeSet )
                {
                    if ( nextGroup == StatisticGroup.DOUBLE_SCORE )
                    {
                        this.doubleScore.addAll( futures.doubleScore );
                    }
                    else if ( nextGroup == StatisticGroup.DURATION_SCORE )
                    {
                        this.durationScore.addAll( futures.durationScore );
                    }
                    else if ( nextGroup == StatisticGroup.MULTIVECTOR )
                    {
                        this.multiVector.addAll( futures.multiVector );
                    }
                    else if ( nextGroup == StatisticGroup.BOXPLOT )
                    {
                        this.boxplot.addAll( futures.boxplot );
                    }
                    else if ( nextGroup == StatisticGroup.PAIRED )
                    {
                        this.paired.addAll( futures.paired );
                    }
                    else if ( nextGroup == StatisticGroup.MATRIX )
                    {
                        this.matrix.addAll( futures.matrix );
                    }
                }
            }
            return this;
        }
    }

    /**
     * Hidden constructor.
     * 
     * @param builder the builder
     */

    private MetricFuturesByTime( MetricFuturesByTimeBuilder builder )
    {
        doubleScore.addAll( builder.doubleScore );
        durationScore.addAll( builder.durationScore );
        multiVector.addAll( builder.multiVector );
        boxplot.addAll( builder.boxplot );
        paired.addAll( builder.paired );
        matrix.addAll( builder.matrix );
    }

}
