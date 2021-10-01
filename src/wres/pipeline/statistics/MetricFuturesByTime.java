package wres.pipeline.statistics;

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
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.Builder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Store of metric futures for each output type. Use {@link #getMetricOutput()} to obtain the processed
 * {@link StatisticsForProject}.
 * 
 * @author James Brown
 */

class MetricFuturesByTime
{

    /**
     * {@link DoubleScoreStatisticOuter} results.
     */

    private final List<Future<List<DoubleScoreStatisticOuter>>> doubleScore = new ArrayList<>();

    /**
     * {@link DurationScoreStatisticOuter} results.
     */

    private final List<Future<List<DurationScoreStatisticOuter>>> durationScore = new ArrayList<>();

    /**
     * {@link DiagramStatisticOuter} results.
     */

    private final List<Future<List<DiagramStatisticOuter>>> diagrams = new ArrayList<>();

    /**
     * {@link BoxplotStatisticOuter} results per pair.
     */

    private final List<Future<List<BoxplotStatisticOuter>>> boxplotPerPair = new ArrayList<>();

    /**
     * {@link BoxplotStatisticOuter} results per pool.
     */

    private final List<Future<List<BoxplotStatisticOuter>>> boxplotPerPool = new ArrayList<>();

    /**
     * {@link DurationDiagramStatisticOuter} results.
     */

    private final List<Future<List<DurationDiagramStatisticOuter>>> paired = new ArrayList<>();

    /**
     * Returns the results associated with the futures.
     * 
     * @return the metric results
     */

    StatisticsForProject getMetricOutput()
    {
        Builder builder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        //Add outputs for current futures
        this.doubleScore.forEach( builder::addDoubleScoreStatistics );
        this.durationScore.forEach( builder::addDurationScoreStatistics );
        this.diagrams.forEach( builder::addDiagramStatistics );
        this.boxplotPerPair.forEach( builder::addBoxPlotStatisticsPerPair );
        this.boxplotPerPool.forEach( builder::addBoxPlotStatisticsPerPool );
        this.paired.forEach( builder::addInstantDurationPairStatistics );
        return builder.build();
    }

    /**
     * Returns the {@link StatisticType} for which futures exist.
     * 
     * @return the set of output types for which futures exist
     */

    Set<StatisticType> getOutputTypes()
    {
        Set<StatisticType> returnMe = new HashSet<>();

        if ( !this.doubleScore.isEmpty() )
        {
            returnMe.add( StatisticType.DOUBLE_SCORE );
        }

        if ( !this.durationScore.isEmpty() )
        {
            returnMe.add( StatisticType.DURATION_SCORE );
        }

        if ( !this.diagrams.isEmpty() )
        {
            returnMe.add( StatisticType.DIAGRAM );
        }

        if ( !this.boxplotPerPair.isEmpty() )
        {
            returnMe.add( StatisticType.BOXPLOT_PER_PAIR );
        }

        if ( !this.boxplotPerPool.isEmpty() )
        {
            returnMe.add( StatisticType.BOXPLOT_PER_POOL );
        }

        if ( !this.paired.isEmpty() )
        {
            returnMe.add( StatisticType.DURATION_DIAGRAM );
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
         * {@link DoubleScoreStatisticOuter} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DoubleScoreStatisticOuter>>> doubleScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DurationScoreStatisticOuter} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationScoreStatisticOuter>>> durationScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DiagramStatisticOuter} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DiagramStatisticOuter>>> diagram =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link BoxplotStatisticOuter} results per pair.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxplotStatisticOuter>>> boxplotPerPair =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link BoxplotStatisticOuter} results per pool.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxplotStatisticOuter>>> boxplotPerPool =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DurationDiagramStatisticOuter} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationDiagramStatisticOuter>>> durationDiagram =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a set of future {@link DoubleScoreStatisticOuter} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Future<List<DoubleScoreStatisticOuter>> value )
        {
            this.doubleScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DurationScoreStatisticOuter} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationScoreOutput( Future<List<DurationScoreStatisticOuter>> value )
        {
            this.durationScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DiagramStatisticOuter} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDiagramOutput( Future<List<DiagramStatisticOuter>> value )
        {
            this.diagram.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxplotStatisticOuter} per pair to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutputPerPair( Future<List<BoxplotStatisticOuter>> value )
        {
            this.boxplotPerPair.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxplotStatisticOuter} per pool to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutputPerPool( Future<List<BoxplotStatisticOuter>> value )
        {
            this.boxplotPerPool.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DurationDiagramStatisticOuter} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationDiagramOutput( Future<List<DurationDiagramStatisticOuter>> value )
        {
            this.durationDiagram.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DoubleScoreStatisticOuter} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Pair<TimeWindowOuter, OneOrTwoThresholds> key,
                                                         Future<List<DoubleScoreStatisticOuter>> value )
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
            this.addFutures( futures, StatisticType.set() );
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
                                               Set<StatisticType> mergeSet )
        {
            if ( Objects.nonNull( mergeSet ) )
            {
                for ( StatisticType nextGroup : mergeSet )
                {
                    if ( nextGroup == StatisticType.DOUBLE_SCORE )
                    {
                        this.doubleScore.addAll( futures.doubleScore );
                    }
                    else if ( nextGroup == StatisticType.DURATION_SCORE )
                    {
                        this.durationScore.addAll( futures.durationScore );
                    }
                    else if ( nextGroup == StatisticType.DIAGRAM )
                    {
                        this.diagram.addAll( futures.diagrams );
                    }
                    else if ( nextGroup == StatisticType.BOXPLOT_PER_PAIR )
                    {
                        this.boxplotPerPair.addAll( futures.boxplotPerPair );
                    }
                    else if ( nextGroup == StatisticType.BOXPLOT_PER_POOL )
                    {
                        this.boxplotPerPool.addAll( futures.boxplotPerPool );
                    }
                    else if ( nextGroup == StatisticType.DURATION_DIAGRAM )
                    {
                        this.durationDiagram.addAll( futures.paired );
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
        this.doubleScore.addAll( builder.doubleScore );
        this.durationScore.addAll( builder.durationScore );
        this.diagrams.addAll( builder.diagram );
        this.boxplotPerPair.addAll( builder.boxplotPerPair );
        this.boxplotPerPool.addAll( builder.boxplotPerPool );
        this.paired.addAll( builder.durationDiagram );
    }

}
