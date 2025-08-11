package wres.vis;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.Timestamp;

import wres.config.yaml.components.ThresholdOperator;
import wres.datamodel.types.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.units.Units;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentType;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;

/**
 * Generates test datasets for graphics writing.
 */

public class TestDataGenerator
{
    private static final Geometry NWS_FEATURE = MessageUtilities.getGeometry( "DRRC2" );
    private static final Geometry USGS_FEATURE = MessageUtilities.getGeometry( "09165000",
                                                                               "DOLORES RIVER BELOW RICO, CO.",
                                                                               4326,
                                                                               "POINT ( -108.0603517 37.63888428 )" );
    private static final Geometry NWM_FEATURE = MessageUtilities.getGeometry( "18384141" );
    private static final FeatureGroup FEATURE_GROUP =
            FeatureGroup.of( MessageUtilities.getGeometryGroup( "DRRC2-09165000-18384141",
                                                                MessageUtilities.getGeometryTuple(
                                                                                      USGS_FEATURE,
                                                                                      NWS_FEATURE,
                                                                                      NWM_FEATURE ) ) );

    private static final String CMS = "CMS";

    private static final Evaluation EVALUATION = Evaluation.newBuilder()
                                                           .setRightDataName( "HEFS" )
                                                           .setBaselineDataName( "ESP" )
                                                           .setRightVariableName( "SQIN" )
                                                           .setMeasurementUnit( "CMS" )
                                                           .build();

    private static final OneOrTwoThresholds THRESHOLD_ONE =
            OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 4.9 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.OBSERVED,
                                                      MeasurementUnit.of( CMS ) ) );

    private static final OneOrTwoThresholds THRESHOLD_TWO =
            OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 7.8 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.OBSERVED,
                                                      MeasurementUnit.of( CMS ) ) );

    private static final OneOrTwoThresholds ALL_DATA_THRESHOLD = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );

    private static final Instant FIRST_INSTANT = Instant.parse( "2521-12-15T12:00:00Z" );

    private static final Instant SECOND_INSTANT = Instant.parse( "2521-12-15T18:00:00Z" );

    /**
     * @return a list of scores for two issued pools
     */

    public static List<DoubleScoreComponentOuter> getScoresForTwoIssuedDatePools()
    {
        List<DoubleScoreComponentOuter> statistics = new ArrayList<>();
        TimeWindow innerWindowOne = MessageUtilities.getTimeWindow( FIRST_INSTANT,
                                                                    FIRST_INSTANT,
                                                                    Duration.ofHours( 1 ),
                                                                    Duration.ofHours( 1 ) );
        TimeWindowOuter outerWindowOne = TimeWindowOuter.of( innerWindowOne );


        wres.statistics.generated.Pool innerPoolOne = MessageFactory.getPool( FEATURE_GROUP,
                                                                              outerWindowOne,
                                                                              null,
                                                                              THRESHOLD_ONE,
                                                                              false );

        PoolMetadata poolOne = PoolMetadata.of( EVALUATION, innerPoolOne );

        DoubleScoreStatisticComponent componentOne = DoubleScoreStatisticComponent.newBuilder()
                                                                                  .setValue( 0.1 )
                                                                                  .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                        .setName(
                                                                                                                                MetricName.MAIN ) )
                                                                                  .build();

        DoubleScoreComponentOuter componentOneOuter = DoubleScoreComponentOuter.of( componentOne, poolOne );

        statistics.add( componentOneOuter );

        TimeWindow innerWindowTwo = MessageUtilities.getTimeWindow( SECOND_INSTANT,
                                                                    SECOND_INSTANT,
                                                                    Duration.ofHours( 1 ),
                                                                    Duration.ofHours( 1 ) );
        TimeWindowOuter outerWindowTwo = TimeWindowOuter.of( innerWindowTwo );


        wres.statistics.generated.Pool innerPoolTwo = MessageFactory.getPool( FEATURE_GROUP,
                                                                              outerWindowTwo,
                                                                              null,
                                                                              THRESHOLD_ONE,
                                                                              false );

        PoolMetadata poolTwo = PoolMetadata.of( EVALUATION, innerPoolTwo );

        DoubleScoreStatisticComponent componentTwo =
                DoubleScoreStatisticComponent.newBuilder()
                                             .setValue( 0.2 )
                                             .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                   .setName( MetricName.MAIN ) )
                                             .build();

        DoubleScoreComponentOuter componentOuterTwo = DoubleScoreComponentOuter.of( componentTwo, poolTwo );

        statistics.add( componentOuterTwo );

        return Collections.unmodifiableList( statistics );
    }

    /**
     * @return a list of scores for two valid pools
     */

    public static List<DoubleScoreComponentOuter> getScoresForTwoValidDatePools()
    {
        // Create issued pools and modify to valid pools
        List<DoubleScoreComponentOuter> issuedPools = TestDataGenerator.getScoresForTwoIssuedDatePools();
        List<DoubleScoreComponentOuter> statistics = new ArrayList<>();

        long minSeconds = Instant.MIN.getEpochSecond();
        long maxSeconds = Instant.MAX.getEpochSecond();

        DoubleScoreComponentOuter first = issuedPools.get( 0 );
        PoolMetadata metaOne = first.getPoolMetadata();
        long firstSeconds = FIRST_INSTANT.getEpochSecond();
        TimeWindow windowOne = metaOne.getTimeWindow()
                                      .getTimeWindow()
                                      .toBuilder()
                                      .setEarliestReferenceTime( Timestamp.newBuilder()
                                                                          .setSeconds( minSeconds ) )
                                      .setLatestReferenceTime( Timestamp.newBuilder()
                                                                        .setSeconds( maxSeconds ) )
                                      .setEarliestValidTime( Timestamp.newBuilder()
                                                                      .setSeconds( firstSeconds ) )
                                      .setLatestValidTime( Timestamp.newBuilder()
                                                                    .setSeconds( firstSeconds ) )
                                      .build();
        TimeWindowOuter windowOneOuter = TimeWindowOuter.of( windowOne );
        PoolMetadata adjustedOne = PoolMetadata.of( metaOne, windowOneOuter );
        DoubleScoreComponentOuter firstAdjusted = DoubleScoreComponentOuter.of( first.getStatistic(), adjustedOne );
        statistics.add( firstAdjusted );

        DoubleScoreComponentOuter second = issuedPools.get( 1 );
        PoolMetadata metaTwo = first.getPoolMetadata();
        long secondSeconds = SECOND_INSTANT.getEpochSecond();
        TimeWindow windowTwo = metaTwo.getTimeWindow()
                                      .getTimeWindow()
                                      .toBuilder()
                                      .setEarliestReferenceTime( Timestamp.newBuilder()
                                                                          .setSeconds( minSeconds ) )
                                      .setLatestReferenceTime( Timestamp.newBuilder()
                                                                        .setSeconds( maxSeconds ) )
                                      .setEarliestValidTime( Timestamp.newBuilder()
                                                                      .setSeconds( secondSeconds ) )
                                      .setLatestValidTime( Timestamp.newBuilder()
                                                                    .setSeconds( secondSeconds ) )
                                      .build();
        TimeWindowOuter windowTwoOuter = TimeWindowOuter.of( windowTwo );
        PoolMetadata adjustedTwo = PoolMetadata.of( metaOne, windowTwoOuter );
        DoubleScoreComponentOuter secondAdjusted = DoubleScoreComponentOuter.of( second.getStatistic(), adjustedTwo );
        statistics.add( secondAdjusted );

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the bias fraction
     * for various pooling windows at one threshold (all data). Corresponds to the use case in Redmine ticket #46461.
     *
     * @return an output map of verification scores
     */
    public static List<DoubleScoreStatisticOuter> getScoresForIssuedTimePools()
    {
        List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            null,
                                            null,
                                            ALL_DATA_THRESHOLD,
                                            false );

        PoolMetadata source = PoolMetadata.of( EVALUATION, pool );

        double[] scores = new double[] {
                -0.39228763627058233,
                -0.38540392640098137,
                -0.37290595138891640,
                -0.29294118442636000,
                -0.21904815321579500,
                -0.15832253472025700,
                -0.29244152171401800,
                -0.28854939865963400,
                -0.32666816357502900,
                -0.29652842873636000,
                -0.28174289655134900,
                -0.26014386674719100,
                -0.20220839431888500,
                -0.26801048204027200,
                -0.28350781433349200,
                -0.27907401971041900,
                -0.25723312071583900,
                -0.28349542374488600,
                -0.27544986528110100,
                -0.25307837568226800,
                -0.24993043930250200,
                -0.27070337571167200,
                -0.25422214821455900,
                -0.28105802405674500
        };
        // Build the map
        for ( int i = 0; i < scores.length; i++ )
        {
            String nextDate = "2017-08-08T" + String.format( "%02d", i ) + ":00:00Z";

            TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( nextDate ),
                                                               Instant.parse( nextDate ),
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 18 ) );

            TimeWindowOuter timeWindow = TimeWindowOuter.of( inner );

            DoubleScoreStatistic one =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.BIAS_FRACTION ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( scores[i] )
                                                                                     .setMetric(
                                                                                             DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN ) ) )
                                        .build();

            rawData.add( DoubleScoreStatisticOuter.of( one, PoolMetadata.of( source, timeWindow ) ) );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * Returns a {@link List} of {@link DoubleScoreStatisticOuter} comprising the CRPSS for various
     * rolling time windows at one threshold (all data). Corresponds to the use case in Redmine ticket #40785.
     *
     * @return an output map of verification scores
     */
    public static List<DoubleScoreStatisticOuter> getScoresForIssuedTimeAndLeadDurationPools()
    {
        List<DoubleScoreStatisticOuter> rawData = new ArrayList<>();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            null,
                                            null,
                                            ALL_DATA_THRESHOLD,
                                            false );

        PoolMetadata source = PoolMetadata.of( EVALUATION, pool );

        // Rolling window parameters
        Instant start = Instant.parse( "2015-12-01T00:00:00Z" );
        Duration period = Duration.ofDays( 91 );
        Duration frequency = Duration.ofDays( 30 );

        // Source data for the outputs
        double[] sixHourOutputs = new double[] { 0.42, 0.32, 0.54, 0.56, 0.52, 0.82, 0.85, 0.63, 0.79, 0.86 };
        double[] twelveHourOutputs = new double[] { 0.37, 0.29, 0.49, 0.53, 0.49, 0.61, 0.67, 0.59, 0.48, 0.52 };
        double[] eighteenHourOutputs = new double[] { 0.28, 0.2, 0.29, 0.45, 0.36, 0.56, 0.48, 0.42, 0.295, 0.415 };
        double[] twentyFourHourOutputs = new double[] { 0.14, 0.11, 0.13, 0.16, 0.15, 0.2, 0.23, 0.16, 0.22, 0.35 };

        // Iterate through 10 rotations of the frequency
        for ( int i = 0; i < 10; i++ )
        {
            Instant begin = start.plus( frequency.multipliedBy( i ) );
            Instant end = begin.plus( period );
            //Add the 6h data
            TimeWindow innerSix = MessageUtilities.getTimeWindow( begin,
                                                                  end,
                                                                  Duration.ofHours( 6 ) );
            TimeWindowOuter sixHourWindow = TimeWindowOuter.of( innerSix );

            DoubleScoreStatistic sixHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( sixHourOutputs[i] )
                                                                                     .setMetric(
                                                                                             DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter sixHourOutput =
                    DoubleScoreStatisticOuter.of( sixHour, PoolMetadata.of( source, sixHourWindow ) );
            rawData.add( sixHourOutput );
            //Add the 12h data
            TimeWindow innerTwelve = MessageUtilities.getTimeWindow( begin,
                                                                     end,
                                                                     Duration.ofHours( 12 ) );
            TimeWindowOuter twelveHourWindow = TimeWindowOuter.of( innerTwelve );

            DoubleScoreStatistic twelveHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( twelveHourOutputs[i] )
                                                                                     .setMetric(
                                                                                             DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter twelveHourOutput =
                    DoubleScoreStatisticOuter.of( twelveHour, PoolMetadata.of( source, twelveHourWindow ) );
            rawData.add( twelveHourOutput );
            //Add the 18h data
            TimeWindow innerEighteen = MessageUtilities.getTimeWindow( begin,
                                                                       end,
                                                                       Duration.ofHours( 18 ) );
            TimeWindowOuter eighteenHourWindow = TimeWindowOuter.of( innerEighteen );

            DoubleScoreStatistic eighteenHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( eighteenHourOutputs[i] )
                                                                                     .setMetric(
                                                                                             DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter eighteenHourOutput =
                    DoubleScoreStatisticOuter.of( eighteenHour, PoolMetadata.of( source, eighteenHourWindow ) );
            rawData.add( eighteenHourOutput );
            //Add the 24h data
            TimeWindow innerTwentyFour = MessageUtilities.getTimeWindow( begin,
                                                                         end,
                                                                         Duration.ofHours( 24 ) );
            TimeWindowOuter twentyFourHourWindow = TimeWindowOuter.of( innerTwentyFour );

            DoubleScoreStatistic twentyFourHour =
                    DoubleScoreStatistic.newBuilder()
                                        .setMetric( DoubleScoreMetric.newBuilder()
                                                                     .setName( MetricName.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE ) )
                                        .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                     .setValue( twentyFourHourOutputs[i] )
                                                                                     .setMetric(
                                                                                             DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               MetricName.MAIN ) ) )
                                        .build();

            DoubleScoreStatisticOuter twentyFourHourOutput =
                    DoubleScoreStatisticOuter.of( twentyFourHour, PoolMetadata.of( source, twentyFourHourWindow ) );
            rawData.add( twentyFourHourOutput );
        }

        return Collections.unmodifiableList( rawData );
    }

    /**
     * @return a diagram dataset for each of two thresholds and three lead durations
     */

    public static List<DiagramStatisticOuter> getDiagramStatisticsForTwoThresholdsAndThreeLeadDurations()
    {
        List<DiagramStatisticOuter> statistics = new ArrayList<>();

        DiagramMetricComponent rankOrder =
                DiagramMetricComponent.newBuilder()
                                      .setName( MetricName.RANK_ORDER )
                                      .setType( DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                      .setMinimum( 0 ) // Strictly 1, but the zeroth position should be visible
                                      .setMaximum( Double.POSITIVE_INFINITY )
                                      .setUnits( Units.COUNT )
                                      .build();

        DiagramMetricComponent observedFrequency = DiagramMetricComponent.newBuilder()
                                                                         .setName( MetricName.OBSERVED_RELATIVE_FREQUENCY )
                                                                         .setType( DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                                         .setMinimum( 0 )
                                                                         .setMaximum( 1 )
                                                                         .setUnits( "PROBABILITY" )
                                                                         .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .setName( MetricName.RANK_HISTOGRAM )
                                            .build();

        List<Double> ranks = List.of( 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0 );
        List<Double> frequencies =
                List.of( 0.0995, 0.1041, 0.0976, 0.1041, 0.0993, 0.1044, 0.1014, 0.0952, 0.0972, 0.0972 );

        DiagramStatisticComponent ro = DiagramStatisticComponent.newBuilder()
                                                                .setMetric( rankOrder )
                                                                .addAllValues( ranks )
                                                                .build();

        DiagramStatisticComponent obs = DiagramStatisticComponent.newBuilder()
                                                                 .setMetric( observedFrequency )
                                                                 .addAllValues( frequencies )
                                                                 .build();

        DiagramStatistic diagram = DiagramStatistic.newBuilder()
                                                   .addStatistics( ro )
                                                   .addStatistics( obs )
                                                   .setMetric( metric )
                                                   .build();

        // Create several pools and use the same diagram for each
        TimeWindow innerWindowOne = MessageUtilities.getTimeWindow( Duration.ofHours( 1 ),
                                                                    Duration.ofHours( 1 ) );
        TimeWindowOuter outerWindowOne = TimeWindowOuter.of( innerWindowOne );


        wres.statistics.generated.Pool innerPoolOne = MessageFactory.getPool( FEATURE_GROUP,
                                                                              outerWindowOne,
                                                                              null,
                                                                              THRESHOLD_ONE,
                                                                              false );

        PoolMetadata poolOne = PoolMetadata.of( EVALUATION, innerPoolOne );
        DiagramStatisticOuter first = DiagramStatisticOuter.of( diagram, poolOne );
        statistics.add( first );

        PoolMetadata poolTwo = PoolMetadata.of( poolOne, THRESHOLD_TWO );
        DiagramStatisticOuter second = DiagramStatisticOuter.of( diagram, poolTwo );
        statistics.add( second );

        com.google.protobuf.Duration secondDuration = MessageUtilities.getDuration( Duration.ofHours( 2 ) );
        TimeWindow innerWindowTwo =
                innerWindowOne.toBuilder()
                              .setEarliestLeadDuration( secondDuration )
                              .setLatestLeadDuration( secondDuration )
                              .build();

        TimeWindowOuter outerWindowTwo = TimeWindowOuter.of( innerWindowTwo );

        PoolMetadata poolThree = PoolMetadata.of( poolOne, outerWindowTwo );
        DiagramStatisticOuter third = DiagramStatisticOuter.of( diagram, poolThree );
        statistics.add( third );

        PoolMetadata poolFour = PoolMetadata.of( poolTwo, outerWindowTwo );
        DiagramStatisticOuter fourth = DiagramStatisticOuter.of( diagram, poolFour );
        statistics.add( fourth );

        com.google.protobuf.Duration thirdDuration = MessageUtilities.getDuration( Duration.ofHours( 3 ) );
        TimeWindow innerWindowThree =
                innerWindowOne.toBuilder()
                              .setEarliestLeadDuration( thirdDuration )
                              .setLatestLeadDuration( thirdDuration )
                              .build();

        TimeWindowOuter outerWindowThree = TimeWindowOuter.of( innerWindowThree );

        PoolMetadata poolFive = PoolMetadata.of( poolOne, outerWindowThree );
        DiagramStatisticOuter fifth = DiagramStatisticOuter.of( diagram, poolFive );
        statistics.add( fifth );

        PoolMetadata poolSix = PoolMetadata.of( poolTwo, outerWindowThree );
        DiagramStatisticOuter sixth = DiagramStatisticOuter.of( diagram, poolSix );
        statistics.add( sixth );

        return Collections.unmodifiableList( statistics );
    }

    /**
     * Returns a {@link DurationDiagramStatisticOuter} that comprises a {@link Duration} that represents a time-to-peak 
     * error against an {@link Instant} that represents the origin (basis time) of the time-series from which the timing 
     * error originates. Contains results for forecasts issued at 12Z each day from 1985-01-01T12:00:00Z to
     * 1985-01-10T12:00:00Z and with a forecast horizon of 336h.
     *
     * @return a paired output of timing errors by basis time
     */

    public static List<DurationDiagramStatisticOuter> getTimeToPeakErrors()
    {

        Instant firstInstant = Instant.parse( "1985-01-01T00:00:00Z" );
        Instant secondInstant = Instant.parse( "1985-01-02T00:00:00Z" );
        Instant thirdInstant = Instant.parse( "1985-01-03T00:00:00Z" );
        Instant fourthInstant = Instant.parse( "1985-01-04T00:00:00Z" );
        Instant fifthInstant = Instant.parse( "1985-01-05T00:00:00Z" );
        Instant sixthInstant = Instant.parse( "1985-01-06T00:00:00Z" );
        Instant seventhInstant = Instant.parse( "1985-01-07T00:00:00Z" );
        Instant eighthInstant = Instant.parse( "1985-01-08T00:00:00Z" );
        Instant ninthInstant = Instant.parse( "1985-01-09T00:00:00Z" );
        Instant tenthInstant = Instant.parse( "1985-01-10T00:00:00Z" );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE )
                                                                                                     .setNanos(
                                                                                                             999_999_999 ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( 0 ) )
                                                            .build();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( firstInstant.getEpochSecond() )
                                                                                  .setNanos( firstInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -43200 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -7200 ) )
                                                               .build();

        PairOfInstantAndDuration three = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( thirdInstant.getEpochSecond() )
                                                                                    .setNanos( thirdInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds(
                                                                                                                   7200 ) )
                                                                 .build();

        PairOfInstantAndDuration four = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( fourthInstant.getEpochSecond() )
                                                                                   .setNanos( fourthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds(
                                                                                                                  14400 ) )
                                                                .build();

        PairOfInstantAndDuration five = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( fifthInstant.getEpochSecond() )
                                                                                   .setNanos( fifthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds(
                                                                                                                  28800 ) )
                                                                .build();

        PairOfInstantAndDuration six = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( sixthInstant.getEpochSecond() )
                                                                                  .setNanos( sixthInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -43200 ) )
                                                               .build();

        PairOfInstantAndDuration seven = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( seventhInstant.getEpochSecond() )
                                                                                    .setNanos( seventhInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds(
                                                                                                                   -57600 ) )
                                                                 .build();

        PairOfInstantAndDuration eight = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( eighthInstant.getEpochSecond() )
                                                                                    .setNanos( eighthInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds(
                                                                                                                   -79200 ) )
                                                                 .build();

        PairOfInstantAndDuration nine = PairOfInstantAndDuration.newBuilder()
                                                                .setTime( Timestamp.newBuilder()
                                                                                   .setSeconds( ninthInstant.getEpochSecond() )
                                                                                   .setNanos( ninthInstant.getNano() ) )
                                                                .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                          .setSeconds( 0 ) )
                                                                .build();

        PairOfInstantAndDuration ten = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( tenthInstant.getEpochSecond() )
                                                                                  .setNanos( tenthInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds(
                                                                                                                 86400 ) )
                                                               .build();

        DurationDiagramStatistic expectedSource = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( metric )
                                                                          .addStatistics( one )
                                                                          .addStatistics( two )
                                                                          .addStatistics( three )
                                                                          .addStatistics( four )
                                                                          .addStatistics( five )
                                                                          .addStatistics( six )
                                                                          .addStatistics( seven )
                                                                          .addStatistics( eight )
                                                                          .addStatistics( nine )
                                                                          .addStatistics( ten )
                                                                          .build();

        // Create the metadata
        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1985-01-10T00:00:00Z" ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 336 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.OBSERVED ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "Streamflow" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            window,
                                            null,
                                            threshold,
                                            false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        return List.of( DurationDiagramStatisticOuter.of( expectedSource, meta ) );
    }

    /**
     * <p>Returns a {@link DurationScoreStatisticOuter} that summarizes the time-to-peak errors. The output
     * includes:</p>
     * <ol>
     * <li>{@link MetricConstants#MEAN}</li>
     * <li>{@link MetricConstants#MEDIAN}</li>
     * <li>{@link MetricConstants#STANDARD_DEVIATION}</li>
     * <li>{@link MetricConstants#MINIMUM}</li>
     * <li>{@link MetricConstants#MAXIMUM}</li>
     * <li>{@link MetricConstants#MEAN_ABSOLUTE}</li>
     * </ol>
     *
     * @return a list of summary statistics for time-to-peak errors
     */

    public static List<DurationScoreStatisticOuter> getTimeToPeakErrorStatistics()
    {
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        TimeWindow inner = MessageUtilities.getTimeWindow( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                           Instant.parse( "1985-01-10T00:00:00Z" ),
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 336 ) );
        TimeWindowOuter window = TimeWindowOuter.of( inner );

        Pool pool = MessageFactory.getPool( FEATURE_GROUP,
                                            window,
                                            null,
                                            ALL_DATA_THRESHOLD,
                                            false );

        PoolMetadata meta = PoolMetadata.of( EVALUATION, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .setMetric( metric )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.MEAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    9360 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.MEDIAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    -3600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.STANDARD_DEVIATION ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    48_364 )
                                                                                                                            .setNanos(
                                                                                                                                    615_000_000 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.MINIMUM ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    -79_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.MAXIMUM ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    86_400 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric(
                                                                                             DurationScoreMetricComponent.newBuilder()
                                                                                                                         .setName(
                                                                                                                                 MetricName.MEAN_ABSOLUTE ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds(
                                                                                                                                    36_720 ) ) )
                                      .build();

        return List.of( DurationScoreStatisticOuter.of( score, meta ) );
    }


    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for several pairs.
     *
     * @return a box plot per pair
     */

    public static List<BoxplotStatisticOuter> getBoxPlotPerPairForOnePool()
    {
        // location id
        Geometry geometry = MessageUtilities.getGeometry( "JUNP1" );

        // Create fake outputs
        TimeWindow innerOne = MessageUtilities.getTimeWindow( Duration.ofHours( 24 ),
                                                              Duration.ofHours( 24 ) );
        TimeWindowOuter timeOne = TimeWindowOuter.of( innerOne );

        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, null );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( "JUNP1_JUNP1", geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            timeOne,
                                            null,
                                            ALL_DATA_THRESHOLD,
                                            false );

        PoolMetadata fakeMetadata = PoolMetadata.of( EVALUATION, pool );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                            .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box first = Box.newBuilder()
                       .addAllQuantiles( List.of( 2.0, 3.0, 4.0, 5.0, 6.0 ) )
                       .setLinkedValue( 1.0 )
                       .build();

        Box second = Box.newBuilder()
                        .addAllQuantiles( List.of( 7.0, 9.0, 11.0, 13.0, 15.0 ) )
                        .setLinkedValue( 3.0 )
                        .build();

        Box third = Box.newBuilder()
                       .addAllQuantiles( List.of( 21.0, 24.0, 27.0, 30.0, 33.0 ) )
                       .setLinkedValue( 5.0 )
                       .build();

        BoxplotStatistic boxOne = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( first )
                                                  .addStatistics( second )
                                                  .addStatistics( third )
                                                  .build();

        return List.of( BoxplotStatisticOuter.of( boxOne, fakeMetadata ) );
    }

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for two pools of data.
     *
     * @return a box plot per pool for two pools
     */

    public static List<BoxplotStatisticOuter> getBoxPlotPerPoolForTwoPools()
    {
        // location id
        Geometry geometry = MessageUtilities.getGeometry( "JUNP1" );

        // Create fake outputs
        TimeWindow innerOne = MessageUtilities.getTimeWindow( Duration.ofHours( 24 ),
                                                              Duration.ofHours( 24 ) );
        TimeWindowOuter timeOne = TimeWindowOuter.of( innerOne );

        OneOrTwoThresholds threshold = OneOrTwoThresholds.of( ThresholdOuter.ALL_DATA );

        GeometryTuple geoTuple = MessageUtilities.getGeometryTuple( geometry, geometry, null );
        GeometryGroup geoGroup = MessageUtilities.getGeometryGroup( "JUNP1_JUNP1", geoTuple );

        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        Pool pool = MessageFactory.getPool( featureGroup,
                                            timeOne,
                                            null,
                                            threshold,
                                            false );

        PoolMetadata fakeMetadataOne = PoolMetadata.of( EVALUATION, pool );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 1.0, 3.0, 5.0, 7.0, 9.0 ) )
                     .build();

        BoxplotStatistic boxOne = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( box )
                                                  .build();

        BoxplotStatisticOuter fakeOutputsOne = BoxplotStatisticOuter.of( boxOne, fakeMetadataOne );

        TimeWindow innerTwo = MessageUtilities.getTimeWindow( Instant.MIN,
                                                              Instant.MAX,
                                                              Duration.ofHours( 48 ),
                                                              Duration.ofHours( 48 ) );
        TimeWindowOuter timeTwo = TimeWindowOuter.of( innerTwo );

        Pool poolTwo = MessageFactory.getPool( featureGroup,
                                               timeTwo,
                                               null,
                                               threshold,
                                               false );

        PoolMetadata fakeMetadataTwo = PoolMetadata.of( EVALUATION, poolTwo );

        Box anotherBox = Box.newBuilder()
                            .addAllQuantiles( List.of( 11.0, 33.0, 55.0, 77.0, 99.0 ) )
                            .build();

        BoxplotStatistic boxTwo = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( anotherBox )
                                                  .build();

        BoxplotStatisticOuter fakeOutputsTwo = BoxplotStatisticOuter.of( boxTwo, fakeMetadataTwo );

        return List.of( fakeOutputsOne, fakeOutputsTwo );
    }

}
