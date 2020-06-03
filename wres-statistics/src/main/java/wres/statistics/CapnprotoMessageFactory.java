package wres.statistics;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.capnproto.EnumList;
import org.capnproto.MessageBuilder;
import org.capnproto.Serialize;
import org.capnproto.StructList;

import wres.statistics.generated.capnp.PoolOuter.Pool;
import wres.statistics.generated.capnp.ReferenceTimeOuter.ReferenceTime;
import wres.statistics.generated.capnp.ReferenceTimeTypeOuter.ReferenceTimeType;
import wres.statistics.generated.capnp.ScoreMetricOuter.ScoreMetric;
import wres.statistics.generated.capnp.ScoreMetricOuter.ScoreMetric.ScoreMetricComponent;
import wres.statistics.generated.capnp.ScoreMetricOuter.ScoreMetric.ScoreMetricComponent.ScoreComponentName;
import wres.statistics.generated.capnp.ScoreStatisticOuter.ScoreStatistic;
import wres.statistics.generated.capnp.ScoreStatisticOuter.ScoreStatistic.ScoreStatisticComponent;
import wres.statistics.generated.capnp.SeasonOuter.Season;
import wres.statistics.generated.capnp.StatisticsOuter.Statistics;
import wres.statistics.generated.capnp.ThresholdOuter.Threshold;
import wres.statistics.generated.capnp.TimeScaleOuter.TimeScale;
import wres.statistics.generated.capnp.TimeWindowOuter.TimeWindow;
import wres.statistics.generated.capnp.ValueFilterOuter.ValueFilter;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.EvaluationEvent;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.statistics.generated.capnp.BoxplotMetricOuter.BoxplotMetric;
import wres.statistics.generated.capnp.BoxplotMetricOuter.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.capnp.BoxplotStatisticOuter.BoxplotStatistic;
import wres.statistics.generated.capnp.DiagramMetricOuter.DiagramMetric;
import wres.statistics.generated.capnp.DiagramMetricOuter.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.capnp.DiagramMetricOuter.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.capnp.DiagramStatisticOuter.DiagramStatistic;
import wres.statistics.generated.capnp.DiagramStatisticOuter.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.capnp.DurationDiagramMetricOuter.DurationDiagramMetric;
import wres.statistics.generated.capnp.DurationDiagramStatisticOuter.DurationDiagramStatistic;
import wres.statistics.generated.capnp.DurationDiagramStatisticOuter.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.capnp.DurationOuter.Duration;
import wres.statistics.generated.capnp.DurationScoreMetricOuter.DurationScoreMetric;
import wres.statistics.generated.capnp.DurationScoreMetricOuter.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.capnp.DurationScoreMetricOuter.DurationScoreMetric.DurationScoreMetricComponent.DurationScoreComponentName;
import wres.statistics.generated.capnp.DurationScoreStatisticOuter.DurationScoreStatistic;
import wres.statistics.generated.capnp.DurationScoreStatisticOuter.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.capnp.EvaluationOuter.Evaluation;
import wres.statistics.generated.capnp.EvaluationStatusOuter.EvaluationStatus;
import wres.statistics.generated.capnp.EvaluationStatusOuter.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.capnp.EvaluationStatusOuter.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.capnp.EvaluationStatusOuter.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.capnp.GeometryOuter;
import wres.statistics.generated.capnp.GeometryOuter.Geometry;
import wres.statistics.generated.capnp.MetricNameOuter.MetricName;
import wres.statistics.generated.capnp.PairsOuter.Pairs;
import wres.statistics.generated.capnp.PairsOuter.Pairs.Pair;
import wres.statistics.generated.capnp.PairsOuter.Pairs.TimeSeriesOfPairs;

/**
 * Creates statistics messages in Cap'n Proto format from internal representations.
 * 
 * @author james.brown@hydrosolved.com
 */

public class CapnprotoMessageFactory
{

    /**
     * Adds a {@link StatisticsForProject} to a {@link MessageBuilder}.
     * 
     * @param builder the builder
     * @param project the project statistics to add
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     * @return the statistics builder
     */

    public static Statistics.Builder parse( MessageBuilder builder, StatisticsForProject project )
            throws InterruptedException
    {
        Objects.requireNonNull( project );

        Statistics.Builder statistics = builder.initRoot( Statistics.factory );

        SampleMetadata metadata = SampleMetadata.of();

        // Add the double scores
        if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores = project.getDoubleScoreStatistics();
            Iterator<wres.datamodel.statistics.DoubleScoreStatistic> iterator = doubleScores.iterator();
            StructList.Builder<ScoreStatistic.Builder> scores = statistics.initScores( doubleScores.size() );
            scores.forEach( next -> CapnprotoMessageFactory.parse( next, iterator.next() ) );
            metadata = doubleScores.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the diagrams
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatistic> diagramStatistics = project.getDiagramStatistics();
            Iterator<wres.datamodel.statistics.DiagramStatistic> iterator = diagramStatistics.iterator();
            StructList.Builder<DiagramStatistic.Builder> diagrams = statistics.initDiagrams( diagramStatistics.size() );
            diagrams.forEach( next -> CapnprotoMessageFactory.parse( next, iterator.next() ) );
            metadata = diagramStatistics.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the boxplots
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR )
             || project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxPlotStatistics> boxplots =
                    new ArrayList<>( project.getBoxPlotStatisticsPerPair() );
            boxplots.addAll( project.getBoxPlotStatisticsPerPool() );
            StructList.Builder<BoxplotStatistic.Builder> boxes = statistics.initBoxPlots( boxplots.size() );
            Iterator<wres.datamodel.statistics.BoxPlotStatistics> iterator = boxplots.iterator();
            boxes.forEach( next -> CapnprotoMessageFactory.parse( next, iterator.next() ) );
            metadata = boxplots.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the duration scores
        if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatistic> durationScores =
                    project.getDurationScoreStatistics();
            Iterator<wres.datamodel.statistics.DurationScoreStatistic> iterator = durationScores.iterator();
            StructList.Builder<DurationScoreStatistic.Builder> scores =
                    statistics.initDurationScores( durationScores.size() );
            scores.forEach( next -> CapnprotoMessageFactory.parse( next, iterator.next() ) );
            metadata = durationScores.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the duration diagrams with instant/duration pairs
        if ( project.hasStatistic( StatisticType.PAIRED ) )
        {
            List<wres.datamodel.statistics.PairedStatistic<Instant, java.time.Duration>> durationDiagrams =
                    project.getInstantDurationPairStatistics();
            Iterator<wres.datamodel.statistics.PairedStatistic<Instant, java.time.Duration>> iterator =
                    durationDiagrams.iterator();
            StructList.Builder<DurationDiagramStatistic.Builder> diagrams =
                    statistics.initDurationDiagrams( durationDiagrams.size() );
            diagrams.forEach( next -> CapnprotoMessageFactory.parse( next, iterator.next() ) );
            metadata = durationDiagrams.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the pool
        Pool.Builder sample = statistics.getPool();
        CapnprotoMessageFactory.parse( sample, metadata );

        return statistics;
    }

    /**
     * Adds a {@link PoolOfPairs} to a {@link Pairs.Builder}.
     * 
     * @param builder the pairs builder
     * @param pairs the pairs to add
     * @throws NullPointerException if any input is null
     */

    public static void parseEnsemblePairs( Pairs.Builder builder, PoolOfPairs<Double, Ensemble> pairs )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( pairs );

        // Initialize the time-series container
        StructList.Builder<TimeSeriesOfPairs.Builder> seriesBuilder = builder.initTimeSeries( pairs.get().size() );
        Iterator<TimeSeriesOfPairs.Builder> iterator = seriesBuilder.iterator();

        for ( TimeSeries<org.apache.commons.lang3.tuple.Pair<Double, Ensemble>> nextSeries : pairs.get() )
        {
            TimeSeriesOfPairs.Builder nextBuilder = iterator.next();

            // Add the reference times
            Map<wres.datamodel.time.ReferenceTimeType, Instant> times = nextSeries.getReferenceTimes();
            StructList.Builder<ReferenceTime.Builder> refTimesBuilder = nextBuilder.initReferenceTimes( times.size() );
            Iterator<ReferenceTime.Builder> refIterator = refTimesBuilder.iterator();
            for ( Map.Entry<wres.datamodel.time.ReferenceTimeType, Instant> nextEntry : times.entrySet() )
            {
                wres.datamodel.time.ReferenceTimeType nextType = nextEntry.getKey();
                Instant nextTime = nextEntry.getValue();

                ReferenceTime.Builder nextRef = refIterator.next();

                nextRef.getReferenceTime().setSeconds( nextTime.getEpochSecond() );
                nextRef.getReferenceTime().setNanos( nextTime.getNano() );
                nextRef.setReferenceTimeType( ReferenceTimeType.valueOf( nextType.toString() ) );
            }

            // Add the events
            StructList.Builder<Pair.Builder> events = nextBuilder.initPairs( nextSeries.getEvents().size() );
            Iterator<Pair.Builder> pairIterator = events.iterator();

            for ( Event<org.apache.commons.lang3.tuple.Pair<Double, Ensemble>> nextEvent : nextSeries.getEvents() )
            {
                Pair.Builder nextPairBuilder = pairIterator.next();

                nextPairBuilder.getValidTime().setSeconds( nextEvent.getTime().getEpochSecond() );
                nextPairBuilder.getValidTime().setNanos( nextEvent.getTime().getNano() );

                org.capnproto.PrimitiveList.Double.Builder leftBuilder = nextPairBuilder.initLeft( 1 );
                leftBuilder.set( 0, nextEvent.getValue().getLeft() );

                double[] ensemble = nextEvent.getValue().getRight().getMembers();

                org.capnproto.PrimitiveList.Double.Builder rightBuilder = nextPairBuilder.initRight( ensemble.length );

                for ( int i = 0; i < ensemble.length; i++ )
                {
                    rightBuilder.set( i, ensemble[i] );
                }
            }
        }
    }
    
    /**
     * Copies the message bytes to a {@link ByteBuffer}.
     * 
     * @param message the message
     * @return the buffered bytes
     * @throws NullPointerException if the message is null
     */
    
    public static ByteBuffer getByteBufferFromOutputSegments( MessageBuilder message ) {
        
        ByteBuffer[] segments = message.getSegmentsForOutput();
        int tableSize = ( segments.length + 2 ) & ( ~1 );

        // Add the message framing
        ByteBuffer table = ByteBuffer.allocate( 4 * tableSize );
        table.order( ByteOrder.LITTLE_ENDIAN );

        table.putInt( 0, segments.length - 1 );

        for ( int i = 0; i < segments.length; ++i )
        {
            table.putInt( 4 * ( i + 1 ), segments[i].limit() / 8 );
        }

        int words = (int) Serialize.computeSerializedSizeInWords( message );
        int byteWords = words * 8; // 8 bytes per word

        ByteBuffer returnMe = ByteBuffer.allocate( table.limit() + byteWords );

        while ( table.hasRemaining() )
        {
            returnMe.put( table );
        }

        // Add the message content
        for ( ByteBuffer buffer : segments )
        {
            while ( buffer.hasRemaining() )
            {
                returnMe.put( buffer );
            }
        }

        return returnMe;
    }      

    /**
     * Adds information from a {@link SampleMetadata} to an {@link Evaluation.Builder}. 
     * See #61388.
     * 
     * TODO: need a better abstraction of an evaluation within the software. See #61388. For now, use the message
     * representation and fill in any missing blanks that can be filled from the statistics metadata.
     * 
     * @param builder the evaluation builder, pre-populated with information not present in the metadata
     * @param metadata the metadata
     * @throws NullPointerException if the builder or metadata is null
     */

    public static void parse( Evaluation.Builder builder, SampleMetadata metadata )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( metadata );

        // Create an evaluation with as much data as possible
        // TODO: need a better abstraction of an evaluation within the software. See #61388
        // For now, hints like a job identifier and start/end time will need to come from the 
        // message instance provided.

        builder.setMeasurementUnit( metadata.getMeasurementUnit().getUnit() );

        if ( metadata.hasTimeScale() )
        {
            CapnprotoMessageFactory.parse( builder.getTimeScale(), metadata.getTimeScale() );
        }

        if ( metadata.hasIdentifier() )
        {
            DatasetIdentifier identifier = metadata.getIdentifier();
            if ( identifier.hasVariableID() )
            {
                builder.setVariableName( identifier.getVariableID() );
            }
            if ( identifier.hasScenarioID() )
            {
                builder.setRightSourceName( identifier.getScenarioID() );
            }
            if ( identifier.hasScenarioIDForBaseline() )
            {
                builder.setBaselineSourceName( identifier.getScenarioIDForBaseline() );
            }
        }

        // Set the season and value filters from the project declaration
        if ( metadata.hasProjectConfig() )
        {
            // Season
            ProjectConfig project = metadata.getProjectConfig();
            if ( Objects.nonNull( project.getPair().getSeason() ) )
            {
                CapnprotoMessageFactory.parse( builder.getSeason(), project.getPair().getSeason() );
            }

            if ( Objects.nonNull( project.getPair().getValues() ) )
            {
                CapnprotoMessageFactory.parse( builder.getValueFilter(), project.getPair().getValues() );
            }
        }
    }

    /**
     * Adds information on the status of an evaluation to a .
     * 
     * @param builder the status builder
     * @param startTime the evaluation start time
     * @param endTime the evaluation end time
     * @param status the completion status
     * @param events a list of evaluation events
     * @throws NullPointerException if the builder, start time, completion status or list of events is null
     */

    public static void parse( EvaluationStatus.Builder builder,
                              Instant startTime,
                              Instant endTime,
                              CompletionStatus status,
                              List<EvaluationEvent> events )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( startTime );
        Objects.requireNonNull( status );
        Objects.requireNonNull( events );

        builder.getEvaluationStartTime().setSeconds( startTime.getEpochSecond() );
        builder.getEvaluationStartTime().setNanos( startTime.getNano() );

        if ( Objects.nonNull( endTime ) )
        {
            builder.getEvaluationEndTime().setSeconds( endTime.getEpochSecond() );
            builder.getEvaluationEndTime().setNanos( endTime.getNano() );
        }

        builder.setCompletionStatus( status );

        StructList.Builder<EvaluationStatusEvent.Builder> eventBuilder = builder.initStatusEvents( events.size() );
        Iterator<EvaluationStatusEvent.Builder> eventIterator = eventBuilder.iterator();

        for ( EvaluationEvent event : events )
        {
            EvaluationStatusEvent.Builder nextEventBuilder = eventIterator.next();

            nextEventBuilder.setEventMessage( event.getMessage() );
            nextEventBuilder.setEventType( StatusMessageType.valueOf( event.getEventType().name() ) );
        }
    }

    /**
     * Adds a {@link wres.config.generated.PairConfig.Season} to a {@link Season.Builder}.
     * 
     * @param builder the season builder
     * @param season the season to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( Season.Builder builder, wres.config.generated.PairConfig.Season season )
    {
        Objects.requireNonNull( season );
        Objects.requireNonNull( builder );

        builder.setStartDay( (byte) season.getEarliestDay() );
        builder.setStartMonth( (byte) season.getEarliestMonth() );
        builder.setEndDay( (byte) season.getLatestDay() );
        builder.setEndMonth( (byte) season.getLatestMonth() );
    }

    /**
     * Adds a {@link DoubleBoundsType} to a {@link ValueFilter.Builder}.
     * 
     * @param builder the value filter builder
     * @param filter the filter to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( ValueFilter.Builder builder, DoubleBoundsType filter )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( filter );

        if ( Objects.nonNull( filter.getMinimum() ) )
        {
            builder.setMinimumInclusiveValue( filter.getMinimum() );
        }

        if ( Objects.nonNull( filter.getMaximum() ) )
        {
            builder.setMaximumInclusiveValue( filter.getMaximum() );
        }
    }

    /**
     * Adds a {@link wres.datamodel.scale.TimeScale} to a {@link TimeScale.Builder}.
     * 
     * @param builder the time scale builder
     * @param metadata the time scale to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( TimeScale.Builder builder, wres.datamodel.scale.TimeScale timeScale )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( timeScale );

        builder.getPeriod().setSeconds( timeScale.getPeriod().getSeconds() );
        builder.getPeriod().setNanos( timeScale.getPeriod().getNano() );
        builder.setFunction( TimeScale.TimeScaleFunction.valueOf( timeScale.getFunction().name() ) );
    }

    /**
     * Adds a {@link wres.datamodel.sampledata.SampleMetadata} to the {@link Pool.Builder}.
     * 
     * @param builder the pool builder
     * @param metadata the pool metadata to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( Pool.Builder builder, SampleMetadata metadata )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( metadata );

        if ( metadata.hasTimeWindow() )
        {
            CapnprotoMessageFactory.parse( builder.getTimeWindow(), metadata.getTimeWindow() );
        }

        if ( metadata.hasIdentifier() && metadata.getIdentifier().hasGeospatialID() )
        {
            // One geometry only for now
            StructList.Builder<GeometryOuter.Geometry.Builder> inner = builder.initGeometries( 1 );
            Geometry.Builder geoBuilder = inner.get( 0 );
            CapnprotoMessageFactory.parse( geoBuilder, metadata.getIdentifier().getGeospatialID() );
        }
    }

    /**
     * Adds a {@link wres.datamodel.time.TimeWindow} to the {@link TimeWindow.Builder}.
     * 
     * @param builder the time window builder
     * @param timeWindow the time window to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( TimeWindow.Builder builder, wres.datamodel.time.TimeWindow timeWindow )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( timeWindow );

        builder.getEarliestReferenceTime().setSeconds( timeWindow.getEarliestReferenceTime().getEpochSecond() );
        builder.getEarliestReferenceTime().setNanos( timeWindow.getEarliestReferenceTime().getNano() );
        builder.getLatestReferenceTime().setSeconds( timeWindow.getLatestReferenceTime().getEpochSecond() );
        builder.getLatestReferenceTime().setNanos( timeWindow.getLatestReferenceTime().getNano() );

        builder.getEarliestValidTime().setSeconds( timeWindow.getEarliestValidTime().getEpochSecond() );
        builder.getEarliestValidTime().setNanos( timeWindow.getEarliestValidTime().getNano() );
        builder.getLatestValidTime().setSeconds( timeWindow.getLatestValidTime().getEpochSecond() );
        builder.getLatestValidTime().setNanos( timeWindow.getLatestValidTime().getNano() );

        builder.getEarliestLeadDuration().setSeconds( timeWindow.getEarliestLeadDuration().getSeconds() );
        builder.getEarliestLeadDuration().setNanos( timeWindow.getEarliestLeadDuration().getNano() );
        builder.getLatestLeadDuration().setSeconds( timeWindow.getLatestLeadDuration().getSeconds() );
        builder.getLatestLeadDuration().setNanos( timeWindow.getLatestLeadDuration().getNano() );

        // Initialize the correct number of reference times and then add them
        EnumList.Builder<ReferenceTimeType> inner = builder.initReferenceTimeType( 1 );
        inner.set( 0, ReferenceTimeType.UNKNOWN );
    }

    /**
     * Adds a {@link wres.datamodel.thresholds.Threshold} to the {@link Threshold.Builder}.
     * 
     * @param builder the threshold builder
     * @param threshold the threshold to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( Threshold.Builder builder, wres.datamodel.thresholds.Threshold threshold )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( threshold );

        builder.setLeftThresholdValue( threshold.getValues().first() );

        if ( threshold.hasProbabilities() )
        {
            builder.setLeftThresholdProbability( threshold.getProbabilities().first() );

            if ( threshold.hasBetweenCondition() )
            {
                builder.setRightThresholdProbability( threshold.getValues().second() );
            }
        }

        if ( threshold.hasBetweenCondition() )
        {
            builder.setRightThresholdValue( threshold.getProbabilities().second() );
        }

        if ( threshold.hasUnits() )
        {
            builder.setThresholdValueUnits( threshold.getUnits().toString() );
        }

        if ( threshold.hasLabel() )
        {
            builder.setName( threshold.getLabel() );
        }
    }

    /**
     * Adds a {@link wres.datamodel.statistics.DoubleScoreStatistic} to a {@link ScoreStatistic.Builder}.
     * 
     * @param builder the statistic builder
     * @param statistic the statistic to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( ScoreStatistic.Builder builder,
                               wres.datamodel.statistics.DoubleScoreStatistic statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );
        
        ScoreMetric.Builder metricBuilder = builder.getMetric();

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            CapnprotoMessageFactory.parse( builder.getEventThreshold(), thresholds.first() );

            if ( thresholds.hasTwo() )
            {
                CapnprotoMessageFactory.parse( builder.getDecisionThreshold(), thresholds.second() );
            }
        }

        MetricConstants metricName = statistic.getMetadata().getMetricID();
        metricBuilder.setName( MetricName.valueOf( metricName.name() ) );

        // Initialize the components
        StructList.Builder<ScoreMetricComponent.Builder> scoreMetricBuilders =
                metricBuilder.initComponents( statistic.getComponents().size() );
        Iterator<ScoreMetricComponent.Builder> componentBuilders = scoreMetricBuilders.iterator();

        StructList.Builder<ScoreStatisticComponent.Builder> scoreStatisticBuilders =
                builder.initStatistics( statistic.getComponents().size() );
        Iterator<ScoreStatisticComponent.Builder> statisticComponentBuilders = scoreStatisticBuilders.iterator();

        // Set the metric components and score values
        // and then propagate to the payload here
        for ( MetricConstants next : statistic.getComponents() )
        {
            // Use the full name for the MAIN component
            String name = next.name();
            if ( next == MetricConstants.MAIN )
            {
                name = "MAIN_SCORE";
            }

            ScoreComponentName scoreName = ScoreComponentName.valueOf( name );

            Double minimum = (Double) metricName.getMinimum();
            Double maximum = (Double) metricName.getMaximum();
            Double optimum = (Double) metricName.getOptimum();

            // Set the limits for the component where available
            if ( next.hasLimits() )
            {
                minimum = (Double) next.getMinimum();
                maximum = (Double) next.getMaximum();
                optimum = (Double) next.getOptimum();
            }

            ScoreMetricComponent.Builder nextBuilder = componentBuilders.next();

            nextBuilder.setName( scoreName );
            nextBuilder.setMinimum( optimum );
            nextBuilder.setMinimum( minimum );
            nextBuilder.setMinimum( maximum );

            ScoreStatisticComponent.Builder nextStatisticBuilder = statisticComponentBuilders.next();

            nextStatisticBuilder.setName( scoreName );
            nextStatisticBuilder.setValue( statistic.getComponent( next ).getData() );         
        }
    }

    /**
     * Adds a {@link wres.datamodel.statistics.DurationScoreStatistic} to a {@link DurationScore.Builder}.
     * 
     * @param builder the builder
     * @param statistic the statistic to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( DurationScoreStatistic.Builder builder,
                               wres.datamodel.statistics.DurationScoreStatistic statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );

        DurationScoreMetric.Builder metricBuilder = builder.getMetric();

        MetricConstants metricName = statistic.getMetadata().getMetricID();
        metricBuilder.setName( MetricName.valueOf( metricName.name() ) );

        // Set the metric components and score values
        // Initialize the components
        StructList.Builder<DurationScoreMetricComponent.Builder> scoreMetricBuilders =
                metricBuilder.initComponents( statistic.getComponents().size() );
        Iterator<DurationScoreMetricComponent.Builder> componentBuilders = scoreMetricBuilders.iterator();

        StructList.Builder<DurationScoreStatisticComponent.Builder> scoreStatisticBuilders =
                builder.initStatistics( statistic.getComponents().size() );
        Iterator<DurationScoreStatisticComponent.Builder> statisticComponentBuilders =
                scoreStatisticBuilders.iterator();

        for ( MetricConstants next : statistic.getComponents() )
        {
            String name = next.name();
            DurationScoreComponentName scoreName = DurationScoreComponentName.valueOf( name );

            DurationScoreMetricComponent.Builder metricComponent = componentBuilders.next();
            metricComponent.setName( scoreName );

            CapnprotoMessageFactory.addLimitsToTimingStatistic( metricComponent, next );

            java.time.Duration score = statistic.getComponent( next ).getData();

            DurationScoreStatisticComponent.Builder scoreComponentBuilder = statisticComponentBuilders.next();
            scoreComponentBuilder.setName( scoreName );
            scoreComponentBuilder.getValue().setSeconds( score.getSeconds() );
            scoreComponentBuilder.getValue().setNanos( score.getNano() );
        }
    }

    /**
     * Adds a {@link wres.datamodel.statistics.DiagramStatistic} to a {@link DiagramStatistic.Builder}.
     * 
     * @param builder the statistic builder
     * @param statistic the statistic to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( DiagramStatistic.Builder builder, wres.datamodel.statistics.DiagramStatistic statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );

        DiagramMetric.Builder metricBuilder = builder.getMetric();

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            CapnprotoMessageFactory.parse( builder.getEventThreshold(), thresholds.first() );

            if ( thresholds.hasTwo() )
            {
                CapnprotoMessageFactory.parse( builder.getDecisionThreshold(), thresholds.second() );
            }
        }

        // Set the diagram components and values
        StructList.Builder<DiagramMetricComponent.Builder> diagramMetricBuilders =
                metricBuilder.initComponents( statistic.getData().size() );
        Iterator<DiagramMetricComponent.Builder> componentBuilders = diagramMetricBuilders.iterator();

        StructList.Builder<DiagramStatisticComponent.Builder> diagramStatisticBuilders =
                builder.initStatistics( statistic.getData().size() );
        Iterator<DiagramStatisticComponent.Builder> statisticComponentBuilders = diagramStatisticBuilders.iterator();

        for ( Map.Entry<MetricDimension, VectorOfDoubles> nextDimension : statistic.getData().entrySet() )
        {
            DiagramComponentName componentName = DiagramComponentName.valueOf( nextDimension.getKey()
                                                                                            .name() );

            DiagramMetricComponent.Builder nextComponent = componentBuilders.next();
            nextComponent.setName( componentName );

            DiagramStatisticComponent.Builder statisticComponent = statisticComponentBuilders.next();
            double[] values = nextDimension.getValue().getDoubles();
            org.capnproto.PrimitiveList.Double.Builder componentValues = statisticComponent.initValues( values.length );
            for ( int i = 0; i < values.length; i++ )
            {
                componentValues.set( i, values[i] );
            }
        }

        metricBuilder.setName( MetricName.valueOf( statistic.getMetadata().getMetricID().name() ) );
    }

    /**
     * Adds a {@link wres.datamodel.statistics.PairedStatistic} to a {@link DurationDiagramStatistic.Builder}.
     * 
     * @param builder the statistic builder
     * @param statistic the statistic to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( DurationDiagramStatistic.Builder builder,
                               wres.datamodel.statistics.PairedStatistic<Instant, java.time.Duration> statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );

        DurationDiagramMetric.Builder metricBuilder = builder.getMetric();
        MetricConstants name = statistic.getMetadata().getMetricID();
        metricBuilder.setName( MetricName.valueOf( name.name() ) );

        // Set the limits for the diagram where available
        if ( name.hasLimits() )
        {
            java.time.Duration minimum = (java.time.Duration) name.getMinimum();
            java.time.Duration maximum = (java.time.Duration) name.getMaximum();
            java.time.Duration optimum = (java.time.Duration) name.getOptimum();

            if ( Objects.nonNull( minimum ) )
            {
                metricBuilder.getMinimum().setSeconds( minimum.getSeconds() );
                metricBuilder.getMinimum().setNanos( minimum.getNano() );
            }

            if ( Objects.nonNull( maximum ) )
            {
                metricBuilder.getMaximum().setSeconds( maximum.getSeconds() );
                metricBuilder.getMaximum().setNanos( maximum.getNano() );
            }

            if ( Objects.nonNull( optimum ) )
            {
                metricBuilder.getOptimum().setSeconds( optimum.getSeconds() );
                metricBuilder.getOptimum().setNanos( optimum.getNano() );
            }
        }

        // Set the diagram components and values
        StructList.Builder<PairOfInstantAndDuration.Builder> statsBuilders =
                builder.initStatistics( statistic.getData().size() );
        Iterator<PairOfInstantAndDuration.Builder> iterator = statsBuilders.iterator();
        for ( org.apache.commons.lang3.tuple.Pair<Instant, java.time.Duration> nextPair : statistic.getData() )
        {
            Instant nextInstant = nextPair.getLeft();
            java.time.Duration nextDuration = nextPair.getRight();

            PairOfInstantAndDuration.Builder nextBuilder = iterator.next();
            nextBuilder.getTime().setSeconds( nextInstant.getEpochSecond() );
            nextBuilder.getTime().setNanos( nextInstant.getNano() );
            nextBuilder.getDuration().setSeconds( nextDuration.getSeconds() );
            nextBuilder.getDuration().setNanos( nextDuration.getNano() );
        }
    }

    /**
     * Adds a {@link wres.datamodel.statistics.BoxPlotStatistics} to a {@link BoxplotStatistic.Builder}.
     * 
     * @param builder the statistic builder
     * @param statistic the statistic to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( BoxplotStatistic.Builder builder, wres.datamodel.statistics.BoxPlotStatistics statistic )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( statistic );

        BoxplotMetric.Builder metricBuilder = builder.getMetric();

        if ( !statistic.getData().isEmpty() )
        {
            StatisticMetadata meta = statistic.getMetadata();

            // Add the quantiles, which are common to all boxes
            BoxPlotStatistic first = statistic.getData().get( 0 );
            double[] quantiles = first.getProbabilities().getDoubles();

            org.capnproto.PrimitiveList.Double.Builder quantileBuilder =
                    metricBuilder.initQuantiles( quantiles.length );
            for ( int i = 0; i < quantiles.length; i++ )
            {
                quantileBuilder.set( i, quantiles[i] );
            }

            MetricConstants metricName = meta.getMetricID();
            metricBuilder.setName( MetricName.valueOf( metricName.name() ) );

            if ( first.hasLinkedValue() )
            {
                MetricDimension dimension = first.getLinkedValueType();
                LinkedValueType valueType = LinkedValueType.valueOf( dimension.name() );
                metricBuilder.setLinkedValueType( valueType );
            }

            metricBuilder.setUnits( meta.getSampleMetadata().getMeasurementUnit().toString() );
            metricBuilder.setMinimum( (Double) metricName.getMinimum() );
            metricBuilder.setMaximum( (Double) metricName.getMaximum() );
            metricBuilder.setOptimum( (Double) metricName.getOptimum() );

            // Set the individual boxes
            StructList.Builder<BoxplotStatistic.Box.Builder> boxBuilders =
                    builder.initStatistics( statistic.getData().size() );
            Iterator<BoxplotStatistic.Box.Builder> boxIterator = boxBuilders.iterator();
            for ( BoxPlotStatistic next : statistic.getData() )
            {
                double[] doubles = next.getData().getDoubles();
                BoxplotStatistic.Box.Builder box = boxIterator.next();

                org.capnproto.PrimitiveList.Double.Builder boxQ = box.initQuantiles( doubles.length );
                for ( int i = 0; i < doubles.length; i++ )
                {
                    boxQ.set( i, doubles[i] );
                }

                if ( next.hasLinkedValue() )
                {
                    box.setLinkedValue( next.getLinkedValue() );
                }
            }
        }
    }

    /**
     * Add a {@link wres.datamodel.sampledata.Location} to a {@link Geometry.Builder}.
     * 
     * @param builder the geometry builder
     * @param location the location to add
     * @throws NullPointerException if any input is null
     */

    private static void parse( Geometry.Builder builder, wres.datamodel.sampledata.Location location )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( location );

        if ( location.hasLocationName() )
        {
            builder.setName( location.getLocationName() );
        }

        if ( location.hasCoordinates() )
        {
            builder.setLatitude( location.getLatitude() );
            builder.setLongitude( location.getLongitude() );
        }
    }


    /**
     * Adds the boundaries to timing error statistics. 
     * 
     * TODO: add these boundaries directly to the {@link MetricConstants}, which will require a separate enumeration for the 
     * {@link MetricGroup#UNIVARIATE_STATISTIC} that apply to timing errors.
     * 
     * @param builder the builder
     * @param name the metric name whose boundaries are required
     * @throws NullPointerException if either input is null
     */

    private static void addLimitsToTimingStatistic( DurationScoreMetricComponent.Builder builder, MetricConstants name )
    {
        Objects.requireNonNull( builder );
        Objects.requireNonNull( name );


        Duration.Builder minimum = builder.getMinimum();
        Duration.Builder maximum = builder.getMinimum();
        Duration.Builder optimum = builder.getOptimum();

        long minSeconds = wres.datamodel.time.TimeWindow.DURATION_MIN.getSeconds();
        int minNanos = wres.datamodel.time.TimeWindow.DURATION_MIN.getNano();
        long maxSeconds = wres.datamodel.time.TimeWindow.DURATION_MAX.getSeconds();
        int maxNanos = wres.datamodel.time.TimeWindow.DURATION_MAX.getNano();

        switch ( name )
        {
            case MEAN:
            case MEDIAN:
            case MINIMUM:
            case MAXIMUM:
                minimum.setSeconds( minSeconds );
                minimum.setNanos( minNanos );
                maximum.setSeconds( maxSeconds );
                maximum.setNanos( maxNanos );
                optimum.setSeconds( 0 );
                optimum.setNanos( 0 );
                break;
            case STANDARD_DEVIATION:
            case MEAN_ABSOLUTE:
                minimum.setSeconds( 0 );
                minimum.setNanos( 0 );
                maximum.setSeconds( maxSeconds );
                maximum.setNanos( maxNanos );
                optimum.setSeconds( 0 );
                optimum.setNanos( 0 );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized univariate statistic for serializing timing errors "
                                                    + "to statistics message: "
                                                    + name );
        }

    }

    /**
     * Do not construct.
     */

    private CapnprotoMessageFactory()
    {
    }

}
