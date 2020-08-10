package wres.datamodel.messages;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import wres.config.ProjectConfigs;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.Ensemble;
import wres.datamodel.EvaluationEvent;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.CompletionStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusMessageType;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Season;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.Pairs.TimeSeriesOfPairs;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.ValueFilter;
import wres.statistics.generated.Evaluation.DefaultData;

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

    private static final Logger LOGGER = LoggerFactory.getLogger( MessageFactory.class );


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

    public static Collection<Statistics> parse( StatisticsForProject project ) throws InterruptedException
    {
        return MessageFactory.parse( project, Collections.emptySet() );
    }

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsForProject}. Optionally ignores some types of statistics.
     *
     * @param project the project statistics
     * @param ignore the types of statistics to ignore
     * @return the statistics message
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if any input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    public static Collection<Statistics> parse( StatisticsForProject project, Set<StatisticType> ignore )
            throws InterruptedException
    {
        Objects.requireNonNull( project );

        Collection<StatisticsForProject> decomposedStatistics = MessageFactory.getStatisticsPerPool( project );

        Collection<Statistics> returnMe = new ArrayList<>();

        for ( StatisticsForProject next : decomposedStatistics )
        {
            Statistics statistics = MessageFactory.parseOnePool( next, ignore );
            returnMe.add( statistics );
        }

        return Collections.unmodifiableCollection( returnMe );
    }

    /**
     * Creates an evaluation from a project declaration.
     * 
     * @param project the project declaration
     * @return an evaluation
     * @throws NullPointerException if the project is null
     */

    public static Evaluation parse( ProjectConfig project )
    {
        Objects.requireNonNull( project );

        Evaluation.Builder builder = Evaluation.newBuilder();

        // Populate the evaluation from the supplied information as reasonably as possible
        if ( Objects.nonNull( project.getPair() ) && Objects.nonNull( project.getPair().getUnit() ) )
        {
            builder.setMeasurementUnit( project.getPair().getUnit() );

            LOGGER.debug( "Populated the evaluation with a measurement unit of {}.",
                          project.getPair().getUnit() );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getLeft() )
             && Objects.nonNull( project.getInputs().getLeft().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( project.getInputs()
                                                                                           .getLeft() );
            builder.setLeftVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a left variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getRight() )
             && Objects.nonNull( project.getInputs().getRight().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( project.getInputs()
                                                                                           .getRight() );
            builder.setRightVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a right variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getBaseline() )
             && Objects.nonNull( project.getInputs().getBaseline().getVariable() ) )
        {
            String variableName = ProjectConfigs.getVariableIdFromDataSourceConfig( project.getInputs()
                                                                                           .getBaseline() );
            builder.setBaselineVariableName( variableName );

            LOGGER.debug( "Populated the evaluation with a baseline variable name of {}.",
                          variableName );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getLeft() )
             && Objects.nonNull( project.getInputs().getLeft().getLabel() ) )
        {
            String name = project.getInputs().getLeft().getLabel();
            builder.setLeftDataName( name );

            LOGGER.debug( "Populated the evaluation with a left source name of {}.",
                          name );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getRight() )
             && Objects.nonNull( project.getInputs().getRight().getLabel() ) )
        {
            String name = project.getInputs().getRight().getLabel();
            builder.setRightDataName( name );

            LOGGER.debug( "Populated the evaluation with a right source name of {}.",
                          name );
        }

        if ( Objects.nonNull( project.getInputs() ) && Objects.nonNull( project.getInputs().getBaseline() )
             && Objects.nonNull( project.getInputs().getBaseline().getLabel() ) )
        {
            String name = project.getInputs().getBaseline().getLabel();
            builder.setBaselineDataName( name );

            LOGGER.debug( "Populated the evaluation with a baseline source name of {}.",
                          name );
        }

        if ( Objects.nonNull( project.getPair() ) && Objects.nonNull( project.getPair().getSeason() ) )
        {
            Season season = MessageFactory.parse( project.getPair().getSeason() );
            builder.setSeason( season );

            LOGGER.debug( "Populated the evaluation with a season of {}.",
                          season );
        }

        if ( Objects.nonNull( project.getPair() ) && Objects.nonNull( project.getPair().getValues() ) )
        {
            ValueFilter filter = MessageFactory.parse( project.getPair().getValues() );
            builder.setValueFilter( filter );

            LOGGER.debug( "Populated the evaluation with a value filter of {}.",
                          project.getPair().getValues() );
        }

        if ( Objects.nonNull( project.getMetrics() ) )
        {

            int metricCount = project.getMetrics()
                                     .stream()
                                     .mapToInt( a -> DataFactory.getMetricsFromMetricsConfig( a, project ).size() )
                                     .sum();

            builder.setMetricCount( metricCount );

            LOGGER.debug( "Populated the evaluation with a metric count of {}.",
                          metricCount );
        }

        builder.setDefaultBaseline( DefaultData.OBSERVED_CLIMATOLOGY );
        LOGGER.debug( "Populated the evaluation with a default baseline of {}.",
                      DefaultData.OBSERVED_CLIMATOLOGY );

        return builder.build();
    }

    /**
     * Creates a {@link EvaluationStatus} message from a list of {@link EvaluationEvent} and other metadata.
     * 
     * @param time an optional time associated with the status event
     * @param status the completion status
     * @param events a list of evaluation events
     * @return a status message
     * @throws NullPointerException if the status or list of events is null
     */

    public static EvaluationStatus parse( Instant time,
                                          CompletionStatus status,
                                          List<EvaluationEvent> events )
    {
        Objects.requireNonNull( status );
        Objects.requireNonNull( events );

        EvaluationStatus.Builder builder = EvaluationStatus.newBuilder();

        if ( Objects.nonNull( time ) )
        {
            Timestamp aTime = Timestamp.newBuilder()
                                       .setSeconds( time.getEpochSecond() )
                                       .setNanos( time.getNano() )
                                       .build();
            builder.setTime( aTime );
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
     * Builds a pool from the input, some of which may be missing.
     * 
     * @param featureTuple the feature tuple
     * @param timeWindow the time window
     * @param timeScale the time scale
     * @param thresholds the thresholds
     * @param isBaselinePool is true if the pool refers to pairs of left and baseline data, otherwise left and right
     * @return the pool
     */

    public static Pool parse( FeatureTuple featureTuple,
                              TimeWindowOuter timeWindow,
                              TimeScaleOuter timeScale,
                              OneOrTwoThresholds thresholds,
                              boolean isBaselinePool )
    {

        Pool.Builder poolBuilder = Pool.newBuilder()
                                       .setIsBaselinePool( isBaselinePool );

        // Feature tuple
        if ( Objects.nonNull( featureTuple ) )
        {
            GeometryTuple geoTuple = MessageFactory.parse( featureTuple );
            poolBuilder.addGeometryTuples( geoTuple );

            LOGGER.debug( "While creating sample metadata, populated the pool with a geometry tuple of {}.",
                          featureTuple );
        }

        // Time window
        if ( Objects.nonNull( timeWindow ) )
        {
            wres.statistics.generated.TimeWindow window = MessageFactory.parse( timeWindow );
            poolBuilder.setTimeWindow( window );

            LOGGER.debug( "While creating sample metadata, populated the pool with a time window of {}.",
                          timeWindow );
        }

        // Time scale
        if ( Objects.nonNull( timeScale ) )
        {
            wres.statistics.generated.TimeScale scale = MessageFactory.parse( timeScale );
            poolBuilder.setTimeScale( scale );

            LOGGER.debug( "While creating sample metadata, populated the pool with a time scale of "
                          + "{}.",
                          timeScale );
        }

        // Thresholds
        if ( Objects.nonNull( thresholds ) )
        {
            wres.statistics.generated.Threshold event = MessageFactory.parse( thresholds.first() );
            poolBuilder.setEventThreshold( event );

            if ( thresholds.hasTwo() )
            {
                wres.statistics.generated.Threshold decision = MessageFactory.parse( thresholds.second() );
                poolBuilder.setDecisionThreshold( decision );
            }

            LOGGER.debug( "While creating pool metadata, populated the pool with a threshold of {}.",
                          thresholds );
        }

        return poolBuilder.build();
    }

    /**
     * Returns a {@link Pairs} from a {@link PoolOfPairs}.
     * 
     * @param pairs The pairs
     * @return a pairs message
     */

    public static Pairs parseTimeSeriesOfEnsemblePairs( PoolOfPairs<Double, Ensemble> pairs )
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
     * Creates a {@link wres.statistics.generated.TimeScale} from a {@link wres.datamodel.scale.TimeScaleOuter}.
     * 
     * @param timeScale the time scale from which to create a message
     * @return the message
     */

    public static TimeScale parse( wres.datamodel.scale.TimeScaleOuter timeScale )
    {
        Objects.requireNonNull( timeScale );

        return timeScale.getTimeScale();
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from a {@link wres.datamodel.time.TimeWindowOuter}.
     * 
     * @param timeWindow the time window from which to create a message
     * @return the message
     */

    public static TimeWindow parse( wres.datamodel.time.TimeWindowOuter timeWindow )
    {
        Objects.requireNonNull( timeWindow );

        return timeWindow.getTimeWindow();
    }

    /**
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static java.time.Duration parse( Duration duration )
    {
        Objects.requireNonNull( duration );

        return java.time.Duration.ofSeconds( duration.getSeconds(), duration.getNanos() );
    }

    /**
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static Duration parse( java.time.Duration duration )
    {
        Objects.requireNonNull( duration );

        return Duration.newBuilder()
                       .setSeconds( duration.getSeconds() )
                       .setNanos( duration.getNano() )
                       .build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Threshold} from a 
     * {@link wres.datamodel.thresholds.ThresholdOuter}.
     * 
     * @param threshold the threshold from which to create a message
     * @return the message
     */

    public static Threshold parse( wres.datamodel.thresholds.ThresholdOuter threshold )
    {
        Objects.requireNonNull( threshold );

        return threshold.getThreshold();
    }

    /**
     * Creates a {@link wres.statistics.generated.DoubleScoreStatistic} from a
     * {@link wres.datamodel.statistics.DoubleScoreStatisticOuter}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DoubleScoreStatistic parse( wres.datamodel.statistics.DoubleScoreStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getData();
    }

    /**
     * Creates a {@link wres.statistics.generated.DurationScoreStatistic} from a 
     * {@link wres.datamodel.statistics.DurationScoreStatisticOuter}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationScoreStatistic parse( wres.datamodel.statistics.DurationScoreStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getData();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.DiagramStatisticOuter}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DiagramStatistic parse( wres.datamodel.statistics.DiagramStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getData();
    }

    /**
     * Creates a {@link wres.statistics.generated.DurationDiagramStatistic} from a
     * {@link wres.datamodel.statistics.DurationDiagramStatisticOuter} composed of timing
     * errors.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static DurationDiagramStatistic parse( wres.datamodel.statistics.DurationDiagramStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getData();
    }

    /**
     * Creates a {@link wres.statistics.generated.DiagramStatistic} from a 
     * {@link wres.datamodel.statistics.BoxplotStatisticOuter}.
     * 
     * @param statistic the statistic from which to create a message
     * @return the message
     */

    public static BoxplotStatistic parse( wres.datamodel.statistics.BoxplotStatisticOuter statistic )
    {
        Objects.requireNonNull( statistic );

        return statistic.getData();
    }

    /**
     * Creates a {@link wres.statistics.generated.GeometryTuple} from a {@link wres.datamodel.FeatureTuple}.
     * 
     * @param location the location from which to create a message
     * @return the message
     */

    public static GeometryTuple parse( FeatureTuple location )
    {
        Objects.requireNonNull( location );

        Geometry left = MessageFactory.parse( location.getLeft() );
        Geometry right = MessageFactory.parse( location.getRight() );
        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left )
                                                     .setRight( right );

        if ( Objects.nonNull( location.getBaseline() ) )
        {
            Geometry baseline = MessageFactory.parse( location.getBaseline() );
            builder.setBaseline( baseline );
        }

        return builder.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Geometry} from a {@link wres.datamodel.FeatureKey}.
     * 
     * @param featureKey the feature key from which to create a message
     * @return the message
     */

    public static Geometry parse( FeatureKey featureKey )
    {
        Objects.requireNonNull( featureKey );

        Geometry.Builder builder = Geometry.newBuilder();

        if ( Objects.nonNull( featureKey.getName() ) )
        {
            builder.setName( featureKey.getName() );
        }

        if ( Objects.nonNull( featureKey.getDescription() ) )
        {
            builder.setDescription( featureKey.getDescription() );
        }

        if ( Objects.nonNull( featureKey.getWkt() ) )
        {
            builder.setWkt( featureKey.getWkt() );
        }

        if ( Objects.nonNull( featureKey.getSrid() ) )
        {
            builder.setSrid( featureKey.getSrid() );
        }

        return builder.build();
    }

    /**
     * Creates a {@link wres.datamodel.FeatureTuple} from a {@link wres.statistics.generated.GeometryTuple}.
     * 
     * @param location the location from which to create a message
     * @return the message
     */

    public static FeatureTuple parse( GeometryTuple location )
    {
        Objects.requireNonNull( location );

        Geometry leftGeom = location.getLeft();
        FeatureKey left = new FeatureKey( leftGeom.getName(),
                                          leftGeom.getDescription(),
                                          leftGeom.getSrid(),
                                          leftGeom.getWkt() );

        Geometry rightGeom = location.getRight();
        FeatureKey right = new FeatureKey( rightGeom.getName(),
                                           rightGeom.getDescription(),
                                           rightGeom.getSrid(),
                                           rightGeom.getWkt() );

        FeatureKey baseline = null;

        if ( location.hasBaseline() )
        {
            Geometry baselineGeom = location.getBaseline();
            baseline = new FeatureKey( baselineGeom.getName(),
                                       baselineGeom.getDescription(),
                                       baselineGeom.getSrid(),
                                       baselineGeom.getWkt() );
        }

        return new FeatureTuple( left, right, baseline );
    }

    /**
     * Creates a collection of {@link wres.statistics.generated.Statistics} by pool from a
     * {@link wres.datamodel.statistics.StatisticsForProject}. Optionally ignores some statistic types.
     * 
     * @param project the project statistics
     * @param pairs the optional pairs
     * @param ignore the types of statistics to ignore
     * @return the statistics messages
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    static Statistics parseOnePool( StatisticsForProject project,
                                    PoolOfPairs<Double, Ensemble> pairs,
                                    Set<StatisticType> ignore )
            throws InterruptedException
    {
        Statistics prototype = MessageFactory.parseOnePool( project, ignore );

        Statistics.Builder statistics = Statistics.newBuilder( prototype );

        if ( Objects.nonNull( pairs ) )
        {
            Pool prototypeSample = statistics.getPool();
            Pool.Builder sampleBuilder = Pool.newBuilder( prototypeSample );
            sampleBuilder.setPairs( MessageFactory.parseTimeSeriesOfEnsemblePairs( pairs ) );
            statistics.setPool( sampleBuilder );
        }

        return statistics.build();
    }

    /**
     * Creates a {@link wres.statistics.generated.Statistics} from a 
     * {@link wres.datamodel.statistics.StatisticsForProject}. Optionally ignores some types of statistics.
     * 
     * @param onePool the pool-shaped statistics
     * @param ignore the statistic types to ignore
     * @return the statistics message
     * @throws IllegalArgumentException if there are zero statistics in total
     * @throws NullPointerException if the input is null
     * @throws InterruptedException if the statistics could not be retrieved from the project
     */

    static Statistics parseOnePool( StatisticsForProject onePool,
                                    Set<StatisticType> ignore )
            throws InterruptedException
    {
        Objects.requireNonNull( onePool );
        Objects.requireNonNull( ignore );

        Statistics.Builder statistics = Statistics.newBuilder();

        SampleMetadata metadata = SampleMetadata.of();

        // Add the double scores
        if ( onePool.hasStatistic( StatisticType.DOUBLE_SCORE ) && !ignore.contains( StatisticType.DOUBLE_SCORE ) )
        {
            List<wres.datamodel.statistics.DoubleScoreStatisticOuter> doubleScores = onePool.getDoubleScoreStatistics();
            doubleScores.forEach( next -> statistics.addScores( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = doubleScores.get( 0 ).getMetadata();
        }

        // Add the diagrams
        if ( onePool.hasStatistic( StatisticType.DIAGRAM ) && !ignore.contains( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> diagrams = onePool.getDiagramStatistics();
            diagrams.forEach( next -> statistics.addDiagrams( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = diagrams.get( 0 ).getMetadata();
        }

        // Add the boxplots per pool
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_POOL )
             && !ignore.contains( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPool();
            boxplots.forEach( next -> statistics.addOneBoxPerPool( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = boxplots.get( 0 ).getMetadata();
        }

        // Add the boxplots per pair
        if ( onePool.hasStatistic( StatisticType.BOXPLOT_PER_PAIR )
             && !ignore.contains( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> boxplots = onePool.getBoxPlotStatisticsPerPair();
            boxplots.forEach( next -> statistics.addOneBoxPerPair( MessageFactory.parse( next ) ) );
            metadata = boxplots.get( 0 ).getMetadata();
        }

        // Add the duration scores
        if ( onePool.hasStatistic( StatisticType.DURATION_SCORE ) && !ignore.contains( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> durationScores =
                    onePool.getDurationScoreStatistics();
            durationScores.forEach( next -> statistics.addDurationScores( MessageFactory.parse( next ) ) );

            // Because the input is a single pool
            metadata = durationScores.get( 0 ).getMetadata();
        }

        // Add the duration diagrams with instant/duration pairs
        if ( onePool.hasStatistic( StatisticType.DURATION_DIAGRAM )
             && !ignore.contains( StatisticType.DURATION_DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DurationDiagramStatisticOuter> durationDiagrams =
                    onePool.getInstantDurationPairStatistics();
            durationDiagrams.forEach( next -> statistics.addDurationDiagrams( MessageFactory.parse( next ) ) );
            metadata = durationDiagrams.get( 0 ).getMetadata();
        }

        // Set the pool information
        if ( metadata.getPool().getIsBaselinePool() )
        {
            statistics.setBaselinePool( metadata.getPool() );
        }
        else
        {
            statistics.setPool( metadata.getPool() );
        }

        return statistics.build();
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
            List<DoubleScoreStatisticOuter> statistics = project.getDoubleScoreStatistics();
            MessageFactory.addDoubleScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Duration scores
        if ( project.hasStatistic( StatisticType.DURATION_SCORE ) )
        {
            List<wres.datamodel.statistics.DurationScoreStatisticOuter> statistics =
                    project.getDurationScoreStatistics();
            MessageFactory.addDurationScoreStatisticsToPool( statistics, mappedStatistics );
        }

        // Diagrams
        if ( project.hasStatistic( StatisticType.DIAGRAM ) )
        {
            List<wres.datamodel.statistics.DiagramStatisticOuter> statistics = project.getDiagramStatistics();
            MessageFactory.addDiagramStatisticsToPool( statistics, mappedStatistics );
        }

        // Box plots per pair
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_PAIR ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics = project.getBoxPlotStatisticsPerPair();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, false );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.BOXPLOT_PER_POOL ) )
        {
            List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics = project.getBoxPlotStatisticsPerPool();
            MessageFactory.addBoxPlotStatisticsToPool( statistics, mappedStatistics, true );
        }

        // Box plots statistics per pool
        if ( project.hasStatistic( StatisticType.DURATION_DIAGRAM ) )
        {
            List<DurationDiagramStatisticOuter> statistics =
                    project.getInstantDurationPairStatistics();
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

        wres.datamodel.time.TimeWindowOuter window = metadata.getTimeWindow();
        wres.datamodel.thresholds.OneOrTwoThresholds thresholds = metadata.getThresholds();

        GeometryTuple geometryTuple = metadata.getPool()
                                              .getGeometryTuples( 0 );

        FeatureTuple featureTuple = MessageFactory.parse( geometryTuple );

        return new PoolBoundaries( featureTuple, window, thresholds, metadata.getPool().getIsBaselinePool() );
    }

    /**
     * Class the helps to organize statistics by pool boundaries within a map.
     *
     * @author james.brown@hydrosolved.com
     */

    private static class PoolBoundaries
    {
        private final OneOrTwoThresholds thresholds;
        private final wres.datamodel.time.TimeWindowOuter window;
        private final wres.datamodel.FeatureTuple location;
        private final boolean isBaselinePool;

        private PoolBoundaries( wres.datamodel.FeatureTuple location,
                                wres.datamodel.time.TimeWindowOuter window,
                                OneOrTwoThresholds thresholds,
                                boolean isBaselinePool )
        {
            this.location = location;
            this.window = window;
            this.thresholds = thresholds;
            this.isBaselinePool = isBaselinePool;
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
                   && Objects.equals( this.thresholds, input.thresholds )
                   && this.isBaselinePool == input.isBaselinePool;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.location, this.window, this.thresholds, this.isBaselinePool );
        }
    }

    /**
     * Adds the new statistics to the map.
     *
     * @param statistics the statistics to add
     * @param mappedStatistics the existing statistics which the new statistics should be added
     * @throws NullPointerException if the input is null
     */

    private static void addDoubleScoreStatisticsToPool( List<DoubleScoreStatisticOuter> statistics,
                                                        Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DoubleScoreStatisticOuter next : statistics )
        {
            SampleMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DoubleScoreStatisticOuter>> future = CompletableFuture.completedFuture( List.of( next ) );
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
            addDurationScoreStatisticsToPool( List<wres.datamodel.statistics.DurationScoreStatisticOuter> statistics,
                                              Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DurationScoreStatisticOuter next : statistics )
        {
            SampleMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DurationScoreStatisticOuter>> future =
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

    private static void addBoxPlotStatisticsToPool( List<wres.datamodel.statistics.BoxplotStatisticOuter> statistics,
                                                    Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics,
                                                    boolean perPool )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.BoxplotStatisticOuter next : statistics )
        {
            SampleMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.BoxplotStatisticOuter>> future =
                    CompletableFuture.completedFuture( List.of( next ) );
            if ( perPool )
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

    private static void addDiagramStatisticsToPool( List<wres.datamodel.statistics.DiagramStatisticOuter> statistics,
                                                    Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( wres.datamodel.statistics.DiagramStatisticOuter next : statistics )
        {
            SampleMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<wres.datamodel.statistics.DiagramStatisticOuter>> future =
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

    private static void addPairedStatisticsToPool( List<DurationDiagramStatisticOuter> statistics,
                                                   Map<PoolBoundaries, StatisticsForProject.Builder> mappedStatistics )
    {
        Objects.requireNonNull( mappedStatistics );

        for ( DurationDiagramStatisticOuter next : statistics )
        {
            SampleMetadata metadata = next.getMetadata();
            PoolBoundaries poolBoundaries = MessageFactory.getPoolBoundaries( metadata );

            StatisticsForProject.Builder another = mappedStatistics.get( poolBoundaries );

            if ( Objects.isNull( another ) )
            {
                another = new StatisticsForProject.Builder();
                mappedStatistics.put( poolBoundaries, another );
            }

            Future<List<DurationDiagramStatisticOuter>> future =
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
