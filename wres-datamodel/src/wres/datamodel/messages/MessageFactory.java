package wres.datamodel.messages;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
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
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
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
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.DurationScoreComponentName;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
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
import wres.statistics.generated.Threshold.ThresholdDataType;
import wres.statistics.generated.Threshold.ThresholdOperator;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;

/**
 * Creates statistics messages in protobuf format from internal representations.
 * 
 * TODO: most of the helpers within this class will disappear when the containers in the {@link wres.datamodel} are 
 * replaced with canonical abstractions from {@link wres.statistics.generated}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MessageFactory
{

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsForProject}.
     * 
     * @param project the project statistics
     * @return the statistics message
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static Collection<Statistics> parseByPool( StatisticsForProject project ) throws InterruptedException
    {
        Objects.requireNonNull( project );

        Collection<StatisticsForProject> decomposedStatistics = MessageFactory.getStatisticsPerPool( project );

        Collection<Statistics> returnMe = new ArrayList<>();

        for ( StatisticsForProject next : decomposedStatistics )
        {
            Statistics statistics = MessageFactory.parse( next );
            returnMe.add( statistics );
        }

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Decomposes the input into pools. A pool contains a single set of space, time and threshold dimensions.
     * 
     * @param project the project statistics
     * @return the decomposed statistics
     * @throws InterruptedException if the statistics could not be retrieved from the project
     * @throws NullPointerException if the input is null
     */

    private static Collection<StatisticsForProject> getStatisticsPerPool( StatisticsForProject project )
            throws InterruptedException
    {
        Objects.requireNonNull( project );

        Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics = new HashMap<>();

        // Double scores
        if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<DoubleScoreStatistic> statistics = project.getDoubleScoreStatistics();
            MessageFactory.addDoubleScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Duration scores
        if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatistic> statistics = project.getDurationScoreStatistics();
            MessageFactory.addDurationScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Diagrams
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatistic> statistics = project.getDiagramStatistics();
            MessageFactory.addDiagramStatisticsToPool( statistics, mappedStatistics );
        }

        // Box plots per pair
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxPlotStatistics> statistics = project.getBoxPlotStatisticsPerPair();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, false );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxPlotStatistics> statistics = project.getBoxPlotStatisticsPerPool();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, true );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.PAIRED ) )
        {
            List<PairedStatistic<Instant, java.time.Duration>> statistics = project.getInstantDurationPairStatistics();
            MessageFactory.addPairedStatisticsToPool( statistics, mappedStatistics );
        }

        Collection<StatisticsForProject> returnMe = new ArrayList<>();

        mappedStatistics.values().forEach( next -> returnMe.add( next.build() ) );

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Creates pool boundaries from metadata.
     * 
     * @param metadata the metadata
     * @return the pool boundaries
     */

    private static PoolBoundaries getPoolBoundaries( SampleMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        wres.datamodel.time.TimeWindow window = metadata.getTimeWindow();
        wres.datamodel.thresholds.OneOrTwoThresholds thresholds = metadata.getThresholds();

        // TODO: To be replaced by some other abstraction, probably a FeatureTuple
        Location location = metadata.getIdentifier()
                                    .getGeospatialID();

        return new PoolBoundaries( location, window, thresholds );
    }

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a
     * {@link wres.datamodel.statistics.StatisticsForProject}.
     * 
     * @return the statistics message
     * @param project the project statistics
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static Statistics parse( StatisticsForProject project ) throws InterruptedException
    {
        Objects.requireNonNull( project );

        Statistics.Builder statistics = Statistics.newBuilder();

        SampleMetadata metadata = SampleMetadata.of();

        // Add the double scores
        if ( project.hasStatistic( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatistic> doubleScores = project.getDoubleScoreStatistics();
            doubleScores.forEach( next -> statistics.addScores( MessageFactory.parse( next ) ) );
            metadata = doubleScores.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the diagrams
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatistic> diagrams = project.getDiagramStatistics();
            diagrams.forEach( next -> statistics.addDiagrams( MessageFactory.parse( next ) ) );
            metadata = diagrams.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the boxplots
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR )
             || project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxPlotStatistics> boxplots =
                    new ArrayList<>( project.getBoxPlotStatisticsPerPair() );
            boxplots.addAll( project.getBoxPlotStatisticsPerPool() );
            boxplots.forEach( next -> statistics.addBoxplots( MessageFactory.parse( next ) ) );
            metadata = boxplots.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the duration scores
        if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatistic> durationScores =
                    project.getDurationScoreStatistics();
            durationScores.forEach( next -> statistics.addDurationScores( MessageFactory.parse( next ) ) );
            metadata = durationScores.get( 0 ).getMetadata().getSampleMetadata();
        }

        // Add the duration diagrams with instant/duration pairs
        if ( project.hasStatistic( StatisticType.PAIRED ) )
        {
            List<wres.datamodel.statistics.PairedStatistic<Instant, java.time.Duration>> durationDiagrams =
                    project.getInstantDurationPairStatistics();
            durationDiagrams.forEach( next -> statistics.addDurationDiagrams( MessageFactory.parse( next ) ) );
            metadata = durationDiagrams.get( 0 ).getMetadata().getSampleMetadata();
        }

        Pool.Builder sample = Pool.newBuilder();

        if ( metadata.hasTimeWindow() )
        {
            TimeWindow timeWindow = MessageFactory.parse( metadata.getTimeWindow() );
            sample.setTimeWindow( timeWindow );
        }

        if ( metadata.hasThresholds() )
        {
            OneOrTwoThresholds thresholds = metadata.getThresholds();
            Threshold evenThreshold = MessageFactory.parse( thresholds.first() );
            sample.setEventThreshold( evenThreshold );
            if ( thresholds.hasTwo() )
            {
                Threshold decisionThreshold = MessageFactory.parse( thresholds.second() );
                sample.setDecisionThreshold( decisionThreshold );
            }
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
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pol from a
     * {@link wres.datamodel.statistics.StatisticsForProject}.
     * 
     * @param project the project statistics
     * @param pairs the optional pairs
     * @return the statistics messages
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static Statistics parse( StatisticsForProject project, PoolOfPairs<Double, Ensemble> pairs )
            throws InterruptedException
    {
        Statistics prototype = MessageFactory.parse( project );

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
     * @throws NullPointerException if the start time completion status or list of events is null
     */

    public static EvaluationStatus parse( Instant startTime,
                                          Instant endTime,
                                          CompletionStatus status,
                                          List<EvaluationEvent> events )
    {
        Objects.requireNonNull( startTime );
        Objects.requireNonNull( status );
        Objects.requireNonNull( events );

        EvaluationStatus.Builder builder = EvaluationStatus.newBuilder();

        Timestamp start = Timestamp.newBuilder()
                                   .setSeconds( startTime.getEpochSecond() )
                                   .build();
        builder.setEvaluationStartTime( start );


        if ( Objects.nonNull( endTime ) )
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
                                             .setOperator( ThresholdOperator.valueOf( threshold.getOperator().name() ) )
                                             .setDataType( ThresholdDataType.valueOf( threshold.getDataType().name() ) )
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
     * Creates a {@link wres.statistics.generated.DurationScoreStatistic} from a 
     * {@link wres.datamodel.statistics.DurationScoreStatistic}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationScoreStatistic parse( wres.datamodel.statistics.DurationScoreStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        DurationScoreMetric.Builder metricBuilder = DurationScoreMetric.newBuilder();
        DurationScoreStatistic.Builder scoreBuilder = DurationScoreStatistic.newBuilder();

        MetricConstants metricName = statistic.getMetadata().getMetricID();

        // Set the metric components and score values
        // and then propagate to the payload here
        for ( MetricConstants next : statistic.getComponents() )
        {
            String name = next.name();
            DurationScoreComponentName scoreName = DurationScoreComponentName.valueOf( name );

            DurationScoreMetricComponent.Builder metricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                               .setName( scoreName );

            MessageFactory.addLimitsToTimingStatistic( metricComponent, next );

            metricBuilder.addComponents( metricComponent );

            java.time.Duration score = statistic.getComponent( next ).getData();
            Duration protoScore = Duration.newBuilder()
                                          .setSeconds( score.getSeconds() )
                                          .setNanos( score.getNano() )
                                          .build();

            DurationScoreStatisticComponent.Builder scoreComponentBuilder =
                    DurationScoreStatisticComponent.newBuilder()
                                                   .setName( scoreName )
                                                   .setValue( protoScore );

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
     * Creates a {@link wres.statistics.generated.DurationDiagramStatistic} from a 
     * {@link wres.datamodel.statistics.PairedStatistic} composed of timing 
     * errors.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationDiagramStatistic
            parse( wres.datamodel.statistics.PairedStatistic<Instant, java.time.Duration> statistic )
    {
        Objects.requireNonNull( statistic );

        DurationDiagramMetric.Builder metricBuilder = DurationDiagramMetric.newBuilder();
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
                Duration minimimProto = Duration.newBuilder()
                                                .setSeconds( minimum.getSeconds() )
                                                .setNanos( minimum.getNano() )
                                                .build();
                metricBuilder.setMinimum( minimimProto );
            }

            if ( Objects.nonNull( maximum ) )
            {
                Duration maximumProto = Duration.newBuilder()
                                                .setSeconds( maximum.getSeconds() )
                                                .setNanos( maximum.getNano() )
                                                .build();
                metricBuilder.setMaximum( maximumProto );
            }

            if ( Objects.nonNull( optimum ) )
            {
                Duration optimumProto = Duration.newBuilder()
                                                .setSeconds( optimum.getSeconds() )
                                                .setNanos( optimum.getNano() )
                                                .build();
                metricBuilder.setOptimum( optimumProto );
            }
        }

        DurationDiagramStatistic.Builder diagramBuilder = DurationDiagramStatistic.newBuilder();

        // Set the diagram components and values
        for ( Pair<Instant, java.time.Duration> nextPair : statistic.getData() )
        {
            Instant nextInstant = nextPair.getLeft();
            java.time.Duration nextDuration = nextPair.getRight();

            Timestamp stamp = Timestamp.newBuilder()
                                       .setSeconds( nextInstant.getEpochSecond() )
                                       .setNanos( nextInstant.getNano() )
                                       .build();

            Duration duration = Duration.newBuilder()
                                        .setSeconds( nextDuration.getSeconds() )
                                        .setNanos( nextDuration.getNano() )
                                        .build();

            PairOfInstantAndDuration.Builder builder = PairOfInstantAndDuration.newBuilder();
            builder.setTime( stamp ).setDuration( duration );
            diagramBuilder.addStatistics( builder );
        }

        // Set the metric
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
            if ( first.hasLinkedValue() )
            {
                MetricDimension dimension = first.getLinkedValueType();
                LinkedValueType valueType = LinkedValueType.valueOf( dimension.name() );
                metricBuilder.setLinkedValueType( valueType );
            }

            metricBuilder.setName( MetricName.valueOf( metricName.name() ) )
                         .setUnits( meta.getSampleMetadata().getMeasurementUnit().toString() )
                         .setMinimum( (Double) metricName.getMinimum() )
                         .setMaximum( (Double) metricName.getMaximum() )
                         .setOptimum( (Double) metricName.getOptimum() );

            // Set the individual boxes
            for ( BoxPlotStatistic next : statistic.getData() )
            {
                double[] doubles = next.getData().getDoubles();
                BoxplotStatistic.Box.Builder box = BoxplotStatistic.Box.newBuilder();
                Arrays.stream( doubles ).forEach( box::addQuantiles );
                if ( next.hasLinkedValue() )
                {
                    box.setLinkedValue( next.getLinkedValue() );
                }
                statisticBuilder.addStatistics( box );
            }
        }

        return statisticBuilder.setMetric( metricBuilder ).build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Geometry} from a 
     * {@link wres.datamodel.sampledata.Location}.
     * 
     * TODO: map across the new FeatureTuple when it arrives and then delete this comment.
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

        // Add the wkt, srid, description and right/baseline names, as available

        return builder.build();
    }

    /**
     * Returns a {@link Pairs} from a {@link PoolOfPairs}.
     * 
     * @param metadata the metadata
     * @return a pairs message
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

        Duration minimum = Duration.newBuilder()
                                   .setSeconds( wres.datamodel.time.TimeWindow.DURATION_MIN.getSeconds() )
                                   .setNanos( wres.datamodel.time.TimeWindow.DURATION_MIN.getNano() )
                                   .build();

        Duration maximum = Duration.newBuilder()
                                   .setSeconds( wres.datamodel.time.TimeWindow.DURATION_MAX.getSeconds() )
                                   .setNanos( wres.datamodel.time.TimeWindow.DURATION_MAX.getNano() )
                                   .build();

        Duration zero = Duration.newBuilder()
                                .setSeconds( java.time.Duration.ZERO.getSeconds() )
                                .setNanos( java.time.Duration.ZERO.getNano() )
                                .build();

        switch ( name )
        {
            case MEAN:
                builder.setMinimum( minimum ).setMaximum( maximum ).setOptimum( zero );
                break;
            case MEDIAN:
                builder.setMinimum( minimum ).setMaximum( maximum ).setOptimum( zero );
                break;
            case MINIMUM:
                builder.setMinimum( minimum ).setMaximum( maximum ).setOptimum( zero );
                break;
            case MAXIMUM:
                builder.setMinimum( minimum ).setMaximum( maximum ).setOptimum( zero );
                break;
            case STANDARD_DEVIATION:
                builder.setMinimum( zero ).setMaximum( maximum ).setOptimum( zero );
                break;
            case MEAN_ABSOLUTE:
                builder.setMinimum( zero ).setMaximum( maximum ).setOptimum( zero );
                break;
            default:
                throw new IllegalArgumentException( "Unrecognized univariate statistic for serializing timing errors to "
                                                    + "protobuf." );
        }

    }

    /**
     * Class the helps to organize statistics by pool boundaries within a map.
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class PoolBoundaries
    {
        private final OneOrTwoThresholds thresholds;
        private final wres.datamodel.time.TimeWindow window;
        private final wres.datamodel.sampledata.Location location;

        private PoolBoundaries( wres.datamodel.sampledata.Location location,
                                wres.datamodel.time.TimeWindow window,
                                OneOrTwoThresholds thresholds )
        {
            this.location = location;
            this.window = window;
            this.thresholds = thresholds;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( o == this )
            {
                return true;
            }

            if ( ! ( o instanceof PoolBoundaries ) )
            {
                return false;
            }

            PoolBoundaries input = (PoolBoundaries) o;

            return Objects.equals( this.location, input.location ) && Objects.equals( this.window, input.window )
                   && Objects.equals( this.thresholds, input.thresholds );
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( location, window, thresholds );
        }
    }

    /**
     * Adds the new statistics to the map.
     * 
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDoubleScoreStatisticsToPool( List<DoubleScoreStatistic> statistics,
                                                        Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DoubleScoreStatistic next : statistics )
        {
            SampleMetadata metadata = next.getMetadata().getSampleMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DoubleScoreStatistic>> future = CompletableFuture.completedFuture( List.of( next ) );
            another.addDoubleScoreStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     * 
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void
            addDurationScoreStatisticsToPool( List<wres.datamodel.statistics.DurationScoreStatistic> statistics,
                                              Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DurationScoreStatistic next : statistics )
        {
            SampleMetadata metadata = next.getMetadata().getSampleMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DurationScoreStatistic>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addDurationScoreStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     * 
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @param perPool is true if the statistics are per pool, false for per pair (per pool)
     * @throws NullPointerException if the input is null
     */

    private static void addBoxPlotStatisticsToPool( List<wres.datamodel.statistics.BoxPlotStatistics> statistics,
                                                    Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics,
                                                    boolean perPool )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.BoxPlotStatistics next : statistics )
        {
            SampleMetadata metadata = next.getMetadata().getSampleMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.BoxPlotStatistics>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            if( perPool)
            {
                another.addBoxPlotStatisticsPerPool( future );
            }
            else
            {
                another.addBoxPlotStatisticsPerPair( future );
            }
        }
    }

    /**
     * Adds the new statistics to the map.
     * 
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDiagramStatisticsToPool( List<wres.datamodel.statistics.DiagramStatistic> statistics,
                                                    Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DiagramStatistic next : statistics )
        {
            SampleMetadata metadata = next.getMetadata().getSampleMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DiagramStatistic>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addDiagramStatistics( future );
        }
    }

    /**
     * Adds the new statistics to the map.
     * 
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addPairedStatisticsToPool( List<PairedStatistic<Instant, java.time.Duration>> statistics,
                                                    Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( PairedStatistic<Instant, java.time.Duration> next : statistics )
        {
            SampleMetadata metadata = next.getMetadata().getSampleMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<PairedStatistic<Instant, java.time.Duration>>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            another.addInstantDurationPairStatistics( future );
        }
    }
    
    /**
     * Do not construct.
     */

    private MessageFactory()
    {
    }

}
