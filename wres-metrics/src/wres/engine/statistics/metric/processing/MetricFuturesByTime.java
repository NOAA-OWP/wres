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
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotOutput;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.datamodel.statistics.DurationScoreOutput;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MatrixOutput;
import wres.datamodel.statistics.MetricOutputForProject;
import wres.datamodel.statistics.MultiVectorOutput;
import wres.datamodel.statistics.PairedOutput;
import wres.datamodel.statistics.MetricOutputForProject.MetricOutputForProjectBuilder;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * Store of metric futures for each output type. Use {@link #getMetricOutput()} to obtain the processed
 * {@link MetricOutputForProject}.
 * 
 * @author james.brown@hydrosolved.com
 */

class MetricFuturesByTime
{

    /**
     * {@link DoubleScoreOutput} results.
     */

    private final List<Future<ListOfMetricOutput<DoubleScoreOutput>>> doubleScore = new ArrayList<>();

    /**
     * {@link DurationScoreOutput} results.
     */

    private final List<Future<ListOfMetricOutput<DurationScoreOutput>>> durationScore = new ArrayList<>();

    /**
     * {@link MultiVectorOutput} results.
     */

    private final List<Future<ListOfMetricOutput<MultiVectorOutput>>> multiVector = new ArrayList<>();

    /**
     * {@link BoxPlotOutput} results.
     */

    private final List<Future<ListOfMetricOutput<BoxPlotOutput>>> boxplot = new ArrayList<>();

    /**
     * {@link PairedOutput} results.
     */

    private final List<Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>>> paired = new ArrayList<>();

    /**
     * {@link MatrixOutput} results.
     */

    private final List<Future<ListOfMetricOutput<MatrixOutput>>> matrix = new ArrayList<>();

    /**
     * Returns the results associated with the futures.
     * 
     * @return the metric results
     */

    MetricOutputForProject getMetricOutput()
    {
        MetricOutputForProjectBuilder builder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        //Add outputs for current futures
        doubleScore.forEach( builder::addDoubleScoreOutput );
        durationScore.forEach( builder::addDurationScoreOutput );
        multiVector.forEach( builder::addMultiVectorOutput );
        boxplot.forEach( builder::addBoxPlotOutput );
        paired.forEach( builder::addPairedOutput );
        matrix.forEach( builder::addMatrixOutput );
        return builder.build();
    }

    /**
     * Returns the {@link MetricOutputGroup} for which futures exist.
     * 
     * @return the set of output types for which futures exist
     */

    Set<MetricOutputGroup> getOutputTypes()
    {
        Set<MetricOutputGroup> returnMe = new HashSet<>();

        if ( !this.doubleScore.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.DOUBLE_SCORE );
        }

        if ( !this.durationScore.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.DURATION_SCORE );
        }

        if ( !this.multiVector.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.MULTIVECTOR );
        }

        if ( !this.boxplot.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.BOXPLOT );
        }

        if ( !this.paired.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.PAIRED );
        }

        if ( !this.matrix.isEmpty() )
        {
            returnMe.add( MetricOutputGroup.MATRIX );
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
         * {@link DoubleScoreOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<DoubleScoreOutput>>> doubleScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link DurationScoreOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<DurationScoreOutput>>> durationScore =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link MultiVectorOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<MultiVectorOutput>>> multiVector =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link BoxPlotOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<BoxPlotOutput>>> boxplot =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link PairedOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>>> paired =
                new ConcurrentLinkedQueue<>();

        /**
         * {@link MatrixOutput} results.
         */

        private final ConcurrentLinkedQueue<Future<ListOfMetricOutput<MatrixOutput>>> matrix =
                new ConcurrentLinkedQueue<>();

        /**
         * Adds a set of future {@link DoubleScoreOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Future<ListOfMetricOutput<DoubleScoreOutput>> value )
        {
            this.doubleScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DurationScoreOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationScoreOutput( Future<ListOfMetricOutput<DurationScoreOutput>> value )
        {
            this.durationScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MultiVectorOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMultiVectorOutput( Future<ListOfMetricOutput<MultiVectorOutput>> value )
        {
            this.multiVector.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxPlotOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutput( Future<ListOfMetricOutput<BoxPlotOutput>> value )
        {
            this.boxplot.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addPairedOutput( Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>> value )
        {
            this.paired.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMatrixOutput( Future<ListOfMetricOutput<MatrixOutput>> value )
        {
            this.matrix.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DoubleScoreOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDoubleScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<ListOfMetricOutput<DoubleScoreOutput>> value )
        {
            Objects.requireNonNull( key.getLeft() );
            
            this.doubleScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link DurationScoreOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addDurationScoreOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                           Future<ListOfMetricOutput<DurationScoreOutput>> value )
        {
            this.durationScore.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MultiVectorOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMultiVectorOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                         Future<ListOfMetricOutput<MultiVectorOutput>> value )
        {
            this.multiVector.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link BoxPlotOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addBoxPlotOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                     Future<ListOfMetricOutput<BoxPlotOutput>> value )
        {
            this.boxplot.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addPairedOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                    Future<ListOfMetricOutput<PairedOutput<Instant, Duration>>> value )
        {
            this.paired.add( value );

            return this;
        }

        /**
         * Adds a set of future {@link MatrixOutput} to the appropriate internal store.
         * 
         * @param value the future result
         * @return the builder
         */

        MetricFuturesByTimeBuilder addMatrixOutput( Pair<TimeWindow, OneOrTwoThresholds> key,
                                                    Future<ListOfMetricOutput<MatrixOutput>> value )
        {
            this.matrix.add( value );

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
            this.addFutures( futures, MetricOutputGroup.set() );
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
                                               Set<MetricOutputGroup> mergeSet )
        {
            if ( Objects.nonNull( mergeSet ) )
            {
                for ( MetricOutputGroup nextGroup : mergeSet )
                {
                    if ( nextGroup == MetricOutputGroup.DOUBLE_SCORE )
                    {
                        this.doubleScore.addAll( futures.doubleScore );
                    }
                    else if ( nextGroup == MetricOutputGroup.DURATION_SCORE )
                    {
                        this.durationScore.addAll( futures.durationScore );
                    }
                    else if ( nextGroup == MetricOutputGroup.MULTIVECTOR )
                    {
                        this.multiVector.addAll( futures.multiVector );
                    }
                    else if ( nextGroup == MetricOutputGroup.BOXPLOT )
                    {
                        this.boxplot.addAll( futures.boxplot );
                    }
                    else if ( nextGroup == MetricOutputGroup.PAIRED )
                    {
                        this.paired.addAll( futures.paired );
                    }
                    else if ( nextGroup == MetricOutputGroup.MATRIX )
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
