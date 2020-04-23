package wres.statistics;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.config.generated.DoubleBoundsType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.Ensemble;
import wres.datamodel.EvaluationEvent;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ScoreMetric;
import wres.statistics.generated.ScoreMetric.ScoreMetricComponent;
import wres.statistics.generated.ScoreMetric.ScoreMetricComponent.ScoreComponentName;
import wres.statistics.generated.ScoreStatistic.ScoreStatisticComponent;
import wres.statistics.generated.ScoreStatistic;
import wres.statistics.generated.Season;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.Threshold.ThresholdType;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;

/**
 * Creates messages from internal representations.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessageFactory
{

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a list of 
     * {@link wres.datamodel.statistics.DoubleScoreStatistic} and {@link wres.datamodel.statistics.DiagramStatistic}
     * for a common pool/sample. The common metadata is populated from any statistic discovered.
     * 
     * @param doubleScores the scores
     * @param diagrams the diagrams
     * @param pairs the optional pairs
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input except the pairs is null
     * @return the statistics message
     */

    public static Statistics parse( List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores,
                                    List<wres.datamodel.statistics.DiagramStatistic> diagrams,
                                    PoolOfPairs<Double, Ensemble> pairs )
    {
        return MessageFactory.parse( doubleScores, diagrams, List.of(), pairs );
    }

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a list of 
     * {@link wres.datamodel.statistics.DoubleScoreStatistic} and {@link wres.datamodel.statistics.DiagramStatistic}
     * and {@link wres.datamodel.statistics.BoxPlotStatistics} for a common pool/sample. The common metadata is 
     * populated from any statistic discovered.
     * 
     * @param doubleScores the scores
     * @param diagrams the diagrams
     * @param boxplots the box plots
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input is null
     * @return the statistics message
     */

    public static Statistics parse( List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores,
                                    List<wres.datamodel.statistics.DiagramStatistic> diagrams,
                                    List<wres.datamodel.statistics.BoxPlotStatistics> boxplots )
    {
        Objects.requireNonNull( doubleScores );
        Objects.requireNonNull( diagrams );
        Objects.requireNonNull( boxplots );

        if ( doubleScores.isEmpty() && diagrams.isEmpty() && boxplots.isEmpty() )
        {
            throw new IllegalArgumentException( "Expected at least one statistic to serialize but found none." );
        }

        Statistics.Builder statistics = Statistics.newBuilder();

        SampleMetadata metadata = SampleMetadata.of();

        // Add the scores
        for ( DoubleScoreStatistic nextScore : doubleScores )
        {
            statistics.addScores( MessageFactory.parse( nextScore ) );
            metadata = nextScore.getMetadata().getSampleMetadata();
        }

        // Add the diagrams
        for ( wres.datamodel.statistics.DiagramStatistic nextDiagram : diagrams )
        {
            statistics.addDiagrams( MessageFactory.parse( nextDiagram ) );
            metadata = nextDiagram.getMetadata().getSampleMetadata();
        }

        // Add the boxplots
        for ( wres.datamodel.statistics.BoxPlotStatistics nextDiagram : boxplots )
        {
            statistics.addBoxplots( MessageFactory.parse( nextDiagram ) );
            metadata = nextDiagram.getMetadata().getSampleMetadata();
        }

        Pool.Builder sample = Pool.newBuilder();

        if ( metadata.hasTimeWindow() )
        {
            TimeWindow timeWindow = MessageFactory.parse( metadata.getTimeWindow() );
            sample.setTimeWindow( timeWindow );
        }

        if ( metadata.hasIdentifier() && metadata.getIdentifier().hasGeospatialID() )
        {
            Location location = metadata.getIdentifier().getGeospatialID();
            Geometry geometry = MessageFactory.parse( location );
            sample.addGeometries( geometry );
        }

        statistics.setPool( sample );

        return statistics.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a list of 
     * {@link wres.datamodel.statistics.DoubleScoreStatistic} and {@link wres.datamodel.statistics.DiagramStatistic}
     * for a common pool/sample. The common metadata is populated from any statistic discovered.
     * 
     * @param doubleScores the scores
     * @param diagrams the diagrams
     * @param boxplots the box plots
     * @param pairs the optional pairs
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input except the pairs is null
     * @return the statistics message
     */

    public static Statistics parse( List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores,
                                    List<wres.datamodel.statistics.DiagramStatistic> diagrams,
                                    List<wres.datamodel.statistics.BoxPlotStatistics> boxplots,
                                    PoolOfPairs<Double, Ensemble> pairs )
    {
        Statistics prototype = MessageFactory.parse( doubleScores, diagrams, boxplots );

        Statistics.Builder statistics = Statistics.newBuilder( prototype );

        if ( Objects.nonNull( pairs ) )
        {
            Pool prototypeSample = statistics.getPool();
            Pool.Builder sampleBuilder = Pool.newBuilder( prototypeSample );
            sampleBuilder.setPairs( MessageFactory.parseEnsemblePairs( pairs ) );
            statistics.setPool( sampleBuilder );
        }

        return statistics.build();
    }

    /**
     * Returns a {@link Evaluation} from the closest approximation at present, namely a {@link SampleMetadata}. 
     * See #61388.
     * 
     * TODO: need a better abstraction of an evaluation within the software. See #61388. For now, use the message
     * representation and fill in any missing blanks that can be filled from the statistics metadata.
     * 
     * @param evaluation the broad outlines of an evaluation
     * @param metadata the metadata
     * @return an evaluation message
     */

    public static Evaluation parse( Evaluation evaluation, SampleMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        // Create an evaluation with as much data as possible
        // TODO: need a better abstraction of an evaluation within the software. See #61388
        // For now, hints like a job identifier and start/end time will need to come from the 
        // message instance provided.
        Evaluation.Builder evaluationPlus = Evaluation.newBuilder( evaluation );

        evaluationPlus.setMeasurementUnit( metadata.getMeasurementUnit().getUnit() );

        if ( metadata.hasTimeScale() )
        {
            evaluationPlus.setTimeScale( MessageFactory.parse( metadata.getTimeScale() ) );
        }

        if ( metadata.hasIdentifier() )
        {
            DatasetIdentifier identifier = metadata.getIdentifier();
            if ( identifier.hasVariableID() )
            {
                evaluationPlus.setVariableName( identifier.getVariableID() );
            }
            if ( identifier.hasScenarioID() )
            {
                evaluationPlus.setRightSourceName( identifier.getScenarioID() );
            }
            if ( identifier.hasScenarioIDForBaseline() )
            {
                evaluationPlus.setBaselineSourceName( identifier.getScenarioIDForBaseline() );
            }
        }

        // Set the season and value filters from the project declaration
        if ( metadata.hasProjectConfig() )
        {
            // Season
            ProjectConfig project = metadata.getProjectConfig();
            if ( Objects.nonNull( project.getPair().getSeason() ) )
            {
                evaluationPlus.setSeason( MessageFactory.parse( project.getPair().getSeason() ) );
            }

            if ( Objects.nonNull( project.getPair().getValues() ) )
            {
                evaluationPlus.setValueFilter( MessageFactory.parse( project.getPair().getValues() ) );
            }

        }

        return evaluationPlus.build();
    }

    /**
     * Creates a {@link EvaluationStatus} message from a list of {@link EvaluationEvent} and other metadata.
     * 
     * @param startTime the evaluation start time
     * @param endTime the evaluation end time
     * @param status the completion status
     * @param events a list of evaluation events
     * @return a status message
     * @throws NullPointerException if the completion status or list of events is null
     */

    public static EvaluationStatus parse( Instant startTime,
                                          Instant endTime,
                                          CompletionStatus status,
                                          List<EvaluationEvent> events )
    {
        Objects.requireNonNull( status );
        Objects.requireNonNull( events );

        EvaluationStatus.Builder builder = EvaluationStatus.newBuilder();

        if ( Objects.nonNull( startTime ) )
        {
            Timestamp start = Timestamp.newBuilder()
                                       .setSeconds( startTime.getEpochSecond() )
                                       .build();
            builder.setEvaluationStartTime( start );
        }

        if ( Objects.nonNull( startTime ) )
        {
            Timestamp end = Timestamp.newBuilder()
                                     .setSeconds( endTime.getEpochSecond() )
                                     .build();
            builder.setEvaluationEndTime( end );
        }

        builder.setCompletionStatus( status );

        for ( EvaluationEvent event : events )
        {
            EvaluationStatusEvent.Builder statusEvent = EvaluationStatusEvent.newBuilder();
            statusEvent.setEventType( StatusMessageType.valueOf( event.getEventType().name() ) )
                       .setEventMessage( event.getMessage() );
            builder.addStatusEvents( statusEvent );
        }

        return builder.build();
    }


    /**
     * Creates a {@link Season} message from a {@link wres.config.generated.PairConfig.Season}.
     * 
     * @param season the declared season
     * @return the season message
     */

    public static Season parse( wres.config.generated.PairConfig.Season season )
    {
        Objects.requireNonNull( season );

        return Season.newBuilder()
                     .setStartDay( season.getEarliestDay() )
                     .setStartMonth( season.getEarliestMonth() )
                     .setEndDay( season.getLatestDay() )
                     .setEndMonth( season.getLatestMonth() )
                     .build();
    }

    /**
     * Creates a {@link ValueFilter} message from a {@link DoubleBoundsType}.
     * 
     * @param filter the declared value filter
     * @return the value filter message
     */

    public static ValueFilter parse( DoubleBoundsType filter )
    {
        Objects.requireNonNull( filter );

        ValueFilter.Builder filterBuilder = ValueFilter.newBuilder();

        if ( Objects.nonNull( filter.getMinimum() ) )
        {
            filterBuilder.setMinimumInclusiveValue( filter.getMinimum() );
        }

        if ( Objects.nonNull( filter.getMaximum() ) )
        {
            filterBuilder.setMaximumInclusiveValue( filter.getMaximum() );
        }

        return filterBuilder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeScale} from a {@link wres.datamodel.scale.TimeScale}.
     * 
     * @param timeScale the time scale from which to create a message
     * @return the message
     */

    public static TimeScale parse( wres.datamodel.scale.TimeScale timeScale )
    {
        Objects.requireNonNull( timeScale );

        return TimeScale.newBuilder()
                        .setPeriod( Duration.newBuilder()
                                            .setSeconds( timeScale.getPeriod()
                                                                  .toSeconds() )
                                            .build() )
                        .setFunction( TimeScaleFunction.valueOf( timeScale.getFunction()
                                                                          .name() ) )
                        .build();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from a {@link wres.datamodel.time.TimeWindow}.
     * 
     * @param timeWindow the time window from which to create a message
     * @return the message
     */

    public static TimeWindow parse( wres.datamodel.time.TimeWindow timeWindow )
    {
        Objects.requireNonNull( timeWindow );

        Timestamp earliestReferenceTime = Timestamp.newBuilder()
                                                   .setSeconds( timeWindow.getEarliestReferenceTime().getEpochSecond() )
                                                   .build();

        Timestamp latestReferenceTime = Timestamp.newBuilder()
                                                 .setSeconds( timeWindow.getLatestReferenceTime().getEpochSecond() )
                                                 .build();

        Timestamp earliestValidTime = Timestamp.newBuilder()
                                               .setSeconds( timeWindow.getEarliestValidTime().getEpochSecond() )
                                               .build();

        Timestamp latestValidTime = Timestamp.newBuilder()
                                             .setSeconds( timeWindow.getLatestValidTime().getEpochSecond() )
                                             .build();

        Duration earliestLeadDuration = Duration.newBuilder()
                                                .setSeconds( timeWindow.getEarliestLeadDuration().getSeconds() )
                                                .setNanos( timeWindow.getEarliestLeadDuration().getNano() )
                                                .build();

        Duration latestLeadDuration = Duration.newBuilder()
                                              .setSeconds( timeWindow.getLatestLeadDuration().getSeconds() )
                                              .setNanos( timeWindow.getLatestLeadDuration().getNano() )
                                              .build();

        // Currently no hint from the internal type about the reference time types considered. Default to UNKNOWN.
        Set<ReferenceTimeType> referenceTimeTypes = Set.of( ReferenceTimeType.UNKNOWN );

        return TimeWindow.newBuilder()
                         .setEarliestReferenceTime( earliestReferenceTime )
                         .setLatestReferenceTime( latestReferenceTime )
                         .setEarliestValidTime( earliestValidTime )
                         .setLatestValidTime( latestValidTime )
                         .setEarliestLeadDuration( earliestLeadDuration )
                         .setLatestLeadDuration( latestLeadDuration )
                         .addAllReferenceTimeType( referenceTimeTypes )
                         .build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Threshold} from a 
     * {@link wres.datamodel.thresholds.Threshold}.
     * 
     * @param threshold the threshold from which to create a message
     * @return the message
     */

    public static Threshold parse( wres.datamodel.thresholds.Threshold threshold )
    {
        Objects.requireNonNull( threshold );

        Threshold.Builder builder = Threshold.newBuilder()
                                             .setType( ThresholdType.valueOf( threshold.getCondition().name() ) )
                                             .setLeftThresholdValue( threshold.getValues().first() );

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

        return builder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.ScoreStatistic} from a 
     * {@link wres.datamodel.statistics.DoubleScoreStatistic}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static ScoreStatistic parse( wres.datamodel.statistics.DoubleScoreStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        ScoreMetric.Builder metricBuilder = ScoreMetric.newBuilder();
        ScoreStatistic.Builder scoreBuilder = ScoreStatistic.newBuilder();

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            Threshold evenThreshold = MessageFactory.parse( thresholds.first() );
            scoreBuilder.setEventThreshold( evenThreshold );
            if ( thresholds.hasTwo() )
            {
                Threshold decisionThreshold = MessageFactory.parse( thresholds.second() );
                scoreBuilder.setDecisionThreshold( decisionThreshold );
            }
        }

        MetricConstants metricName = statistic.getMetadata().getMetricID();

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

            double minimum = metricName.getMinimum();
            double maximum = metricName.getMaximum();
            double optimum = metricName.getOptimum();

            // Set the limits for the component where available
            if ( next.hasLimits() )
            {
                minimum = next.getMinimum();
                maximum = next.getMaximum();
                optimum = next.getOptimum();
            }

            ScoreMetricComponent metricComponent = ScoreMetricComponent.newBuilder()
                                                                       .setName( scoreName )
                                                                       .setMinimum( minimum )
                                                                       .setMaximum( maximum )
                                                                       .setOptimum( optimum )
                                                                       .build();

            metricBuilder.addComponents( metricComponent );

            ScoreStatisticComponent.Builder scoreComponentBuilder =
                    ScoreStatisticComponent.newBuilder()
                                           .setName( scoreName )
                                           .setValue( statistic.getComponent( next ).getData() );

            scoreBuilder.addStatistics( scoreComponentBuilder );
        }

        // Set the metric
        metricBuilder.setName( MetricName.valueOf( metricName.name() ) );
        scoreBuilder.setMetric( metricBuilder );

        return scoreBuilder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.DiagramStatistic}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DiagramStatistic parse( wres.datamodel.statistics.DiagramStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        DiagramMetric.Builder metricBuilder = DiagramMetric.newBuilder();
        DiagramStatistic.Builder diagramBuilder = DiagramStatistic.newBuilder();

        SampleMetadata metadata = statistic.getMetadata().getSampleMetadata();
        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            Threshold evenThreshold = MessageFactory.parse( thresholds.first() );
            diagramBuilder.setEventThreshold( evenThreshold );
            if ( thresholds.hasTwo() )
            {
                Threshold decisionThreshold = MessageFactory.parse( thresholds.second() );
                diagramBuilder.setDecisionThreshold( decisionThreshold );
            }
        }

        // Set the diagram components and values
        for ( Map.Entry<MetricDimension, VectorOfDoubles> nextDimension : statistic.getData().entrySet() )
        {
            DiagramComponentName componentName = DiagramComponentName.valueOf( nextDimension.getKey()
                                                                                            .name() );

            metricBuilder.addComponents( DiagramMetricComponent.newBuilder()
                                                               .setName( componentName ) );
            DiagramStatisticComponent.Builder statisticComponent = DiagramStatisticComponent.newBuilder();
            statisticComponent.addAllValues( Arrays.stream( nextDimension.getValue()
                                                                         .getDoubles() )
                                                   .boxed()
                                                   .collect( Collectors.toList() ) )
                              .setName( componentName );
            diagramBuilder.addStatistics( statisticComponent );
        }

        // Set the metric
        metricBuilder.setName( MetricName.valueOf( statistic.getMetadata().getMetricID().name() ) );
        diagramBuilder.setMetric( metricBuilder );

        return diagramBuilder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.BoxPlotStatistics}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static BoxplotStatistic parse( wres.datamodel.statistics.BoxPlotStatistics statistic )
    {
        Objects.requireNonNull( statistic );

        BoxplotMetric.Builder metricBuilder = BoxplotMetric.newBuilder();
        BoxplotStatistic.Builder statisticBuilder = BoxplotStatistic.newBuilder();

        if ( !statistic.getData().isEmpty() )
        {
            StatisticMetadata meta = statistic.getMetadata();

            // Add the quantiles, which are common to all boxes
            BoxPlotStatistic first = statistic.getData().get( 0 );
            double[] quantiles = first.getProbabilities().getDoubles();
            Arrays.stream( quantiles ).forEach( metricBuilder::addQuantiles );

            MetricConstants metricName = meta.getMetricID();

            metricBuilder.setLinkedValueType( LinkedValueType.valueOf( first.getLinkedValueType().name() ) )
                         .setName( MetricName.valueOf( metricName.name() ) )
                         .setUnits( meta.getSampleMetadata().getMeasurementUnit().toString() )
                         .setMinimum( metricName.getMinimum() )
                         .setMaximum( metricName.getMaximum() )
                         .setOptimum( metricName.getOptimum() );

            // Set the individual boxes
            for ( BoxPlotStatistic next : statistic.getData() )
            {
                double[] doubles = next.getData().getDoubles();
                BoxplotStatistic.Box.Builder box = BoxplotStatistic.Box.newBuilder();
                Arrays.stream( doubles ).forEach( box::addQuantiles );
                box.setLinkedValue( next.getLinkedValue() );
                statisticBuilder.addStatistics( box );
            }
        }

        return statisticBuilder.setMetric( metricBuilder ).build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Geometry} from a 
     * {@link wres.datamodel.sampledata.Location}.
     * 
     * @param location the location from which to create a message
     * @return the message
     */

    public static Geometry parse( wres.datamodel.sampledata.Location location )
    {
        Objects.requireNonNull( location );

        Geometry.Builder builder = Geometry.newBuilder();

        if ( location.hasLocationName() )
        {
            builder.setName( location.getLocationName() );
        }

        if ( location.hasCoordinates() )
        {
            builder.setLatitude( location.getLatitude() );
            builder.setLongitude( location.getLongitude() );
        }

        return builder.build();
    }

    /**
     * Returns a {@link Evaluation} from the closest approximation at present, namely a {@link SampleMetadata}. 
     * See #61388.
     * 
     * @param metadata the metadata
     * @return an evaluation message
     */

    private static Pairs parseEnsemblePairs( PoolOfPairs<Double, Ensemble> pairs )
    {
        Objects.requireNonNull( pairs );

        Pairs.Builder builder = Pairs.newBuilder();

        for ( TimeSeries<Pair<Double, Ensemble>> nextSeries : pairs.get() )
        {
            TimeSeriesOfPairs.Builder series = TimeSeriesOfPairs.newBuilder();

            // Add the reference times
            Map<wres.datamodel.time.ReferenceTimeType, Instant> times = nextSeries.getReferenceTimes();
            for ( Map.Entry<wres.datamodel.time.ReferenceTimeType, Instant> nextEntry : times.entrySet() )
            {
                wres.datamodel.time.ReferenceTimeType nextType = nextEntry.getKey();
                Instant nextTime = nextEntry.getValue();
                ReferenceTime nextRef =
                        ReferenceTime.newBuilder()
                                     .setReferenceTimeType( ReferenceTimeType.valueOf( nextType.toString() ) )
                                     .setReferenceTime( Timestamp.newBuilder()
                                                                 .setSeconds( nextTime.getEpochSecond() )
                                                                 .setNanos( nextTime.getNano() ) )
                                     .build();
                series.addReferenceTimes( nextRef );
            }

            // Add the events
            for ( Event<Pair<Double, Ensemble>> nextEvent : nextSeries.getEvents() )
            {
                wres.statistics.generated.Pairs.Pair.Builder nextPair =
                        wres.statistics.generated.Pairs.Pair.newBuilder();

                nextPair.setValidTime( Timestamp.newBuilder()
                                                .setSeconds( nextEvent.getTime().getEpochSecond() )
                                                .setNanos( nextEvent.getTime().getNano() ) )
                        .addLeft( nextEvent.getValue().getLeft() );

                for ( double nextRight : nextEvent.getValue().getRight().getMembers() )
                {
                    nextPair.addRight( nextRight );
                }

                series.addPairs( nextPair );
            }

            builder.addTimeSeries( series );
        }

        return builder.build();
    }

    /**
     * Do not construct.
     */

    private MessageFactory()
    {
    }

}
