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
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.TimeWindow;

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

    private final List<Future<List<DoubleScoreStatistic>>> doubleScore = new ArrayList<>();

    /**
     * {@link DurationScoreStatistic} results.
     */

    private final List<Future<List<DurationScoreStatistic>>> durationScore = new ArrayList<>();

    /**
     * {@link DiagramStatistic} results.
     */

    private final List<Future<List<DiagramStatistic>>> multiVector = new ArrayList<>();

    /**
     * {@link BoxPlotStatistics} results per pair.
     */

    private final List<Future<List<BoxPlotStatistics>>> boxplotPerPair = new ArrayList<>();

    /**
     * {@link BoxPlotStatistics} results per pool.
     */

    private final List<Future<List<BoxPlotStatistics>>> boxplotPerPool = new ArrayList<>();    
    
    /**
     * {@link PairedStatistic} results.
     */

    private final List<Future<List<PairedStatistic<Instant, Duration>>>> paired = new ArrayList<>();

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
        doubleScore.forEach( builder::addDoubleScoreStatistics );
        durationScore.forEach( builder::addDurationScoreStatistics );
        multiVector.forEach( builder::addMultiVectorStatistics );
        boxplotPerPair.forEach( builder::addBoxPlotStatisticsPerPair );
        boxplotPerPool.forEach( builder::addBoxPlotStatisticsPerPool );
        paired.forEach( builder::addPairedStatistics );
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

        if ( !this.multiVector.isEmpty() )
        {
            returnMe.add( StatisticType.MULTIVECTOR );
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
            returnMe.add( StatisticType.PAIRED );
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

        private final ConcurrentLinkedQueue<Future<List<DoubleScoreStatistic>>> doubleScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DurationScoreStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DurationScoreStatistic>>> durationScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DiagramStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<List<DiagramStatistic>>> multiVector =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link BoxPlotStatistics} results per pair.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxPlotStatistics>>> boxplotPerPair =
                new ConcurrentLinkedQueue<>();
        
        /**
         * {@link BoxPlotStatistics} results per pool.
         */

        private final ConcurrentLinkedQueue<Future<List<BoxPlotStatistics>>> boxplotPerPool =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link PairedStatistic} results.
         */

        private final ConcurrentLinkedQueue<Future<List<PairedStatistic<Instant, Duration>>>> paired =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a set of future {@link DoubleScoreStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Future<List<DoubleScoreStatistic>> value )
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

        MetricFuturesByTimeBuilder addDurationScoreOutput( Future<List<DurationScoreStatistic>> value )
        {
            this.durationScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DiagramStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMultiVectorOutput( Future<List<DiagramStatistic>> value )
        {
            this.multiVector.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxPlotStatistics} per pair to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutputPerPair( Future<List<BoxPlotStatistics>> value )
        {
            this.boxplotPerPair.add( value );

            return this;
        }
        
        /**
         * Adds a set of future {@link BoxPlotStatistics} per pool to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutputPerPool( Future<List<BoxPlotStatistics>> value )
        {
            this.boxplotPerPool.add( value );

            return this;
        }        

        /**
         * Adds a set of future {@link PairedStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addPairedOutput( Future<List<PairedStatistic<Instant, Duration>>> value )
        {
            this.paired.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DoubleScoreStatistic} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<List<DoubleScoreStatistic>> value )
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
                    else if ( nextGroup == StatisticType.MULTIVECTOR )
                    {
                        this.multiVector.addAll( futures.multiVector );
                    }
                    else if ( nextGroup == StatisticType.BOXPLOT_PER_PAIR )
                    {
                        this.boxplotPerPair.addAll( futures.boxplotPerPair );
                    }
                    else if ( nextGroup == StatisticType.BOXPLOT_PER_POOL )
                    {
                        this.boxplotPerPool.addAll( futures.boxplotPerPool );
                    }
                    else if ( nextGroup == StatisticType.PAIRED )
                    {
                        this.paired.addAll( futures.paired );
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
        boxplotPerPair.addAll( builder.boxplotPerPair );
        boxplotPerPool.addAll( builder.boxplotPerPool );
        paired.addAll( builder.paired );
    }

}
