package wres.io.writing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.google.protobuf.Timestamp;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.GraphicalType;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.BoxplotStatistic.Box;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;

/**
 * Helpers for writing outputs.
 */

public class WriterTestHelper
{

    /**
     * Returns a fake project configuration for a specified feature.
     * 
     * @param feature the feature
     * @param destinationType the destination type
     * @return fake project configuration
     */

    public static ProjectConfig getMockedProjectConfig( Feature feature, DestinationType destinationType )
    {
        // Use the system temp directory so that checks for writeability pass.
        GraphicalType graphics = new GraphicalType( null, null, 800, 600, null );
        DestinationConfig destinationConfig =
                new DestinationConfig( null,
                                       graphics,
                                       null,
                                       destinationType,
                                       null );

        List<DestinationConfig> destinations = new ArrayList<>();
        destinations.add( destinationConfig );

        ProjectConfig.Outputs outputsConfig =
                new ProjectConfig.Outputs( destinations, null );

        List<Feature> features = new ArrayList<>();
        features.add( feature );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( null,
                                                         pairConfig,
                                                         null,
                                                         outputsConfig,
                                                         null,
                                                         "test" );
        return projectConfig;
    }

    /**
     * Returns a fake feature for a specified location identifier.
     * 
     * @param locationId the location identifier
     */

    public static Feature getMockedFeature( String locationId )
    {
        return new Feature( locationId,
                            locationId,
                            null );
    }

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for two pools of data.
     * 
     * @return a box plot per pool for two pools
     */

    public static List<BoxplotStatisticOuter> getBoxPlotPerPoolForTwoPools()
    {
        // location id
        FeatureKey feature = FeatureKey.of( "JUNP1" );

        // Create fake outputs
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 24 ),
                                    Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadataOne = SampleMetadata.of( evaluation, pool );

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

        TimeWindowOuter timeTwo =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 48 ),
                                    Duration.ofHours( 48 ) );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( feature,
                                                               feature,
                                                               null ),
                                             timeTwo,
                                             null,
                                             threshold,
                                             false );

        SampleMetadata fakeMetadataTwo = SampleMetadata.of( evaluation, poolTwo );

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

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for several pairs.
     * 
     * @return a box plot per pair
     */

    public static List<BoxplotStatisticOuter> getBoxPlotPerPairForOnePool()
    {
        // location id
        FeatureKey feature = FeatureKey.of( "JUNP1" );

        // Create fake outputs
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 24 ),
                                    Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

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
     * Returns a {@link List} containing a {@link DiagramStatisticOuter} that
     * represents the output of a reliability diagram for one pool.
     * 
     * @return a reliability diagram for one pool
     */

    public static List<DiagramStatisticOuter> getReliabilityDiagramForOnePool()
    {

        // location id
        FeatureKey feature = FeatureKey.of( "CREC1" );

        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 24 ),
                                    Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( 11.94128 ),
                                                                           OneOrTwoDoubles.of( 0.9 ),
                                                                           Operator.GREATER_EQUAL,
                                                                           ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

        DiagramMetricComponent forecastComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.FORECAST_PROBABILITY )
                                      .build();

        DiagramMetricComponent observedComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                      .build();

        DiagramMetricComponent sampleComponent =
                DiagramMetricComponent.newBuilder()
                                      .setName( DiagramComponentName.SAMPLE_SIZE )
                                      .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                            .build();

        DiagramStatisticComponent forecastProbability =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( forecastComponent )
                                         .addAllValues( List.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) )
                                         .build();

        DiagramStatisticComponent observedFrequency =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( observedComponent )
                                         .addAllValues( List.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) )
                                         .build();

        DiagramStatisticComponent sampleSize =
                DiagramStatisticComponent.newBuilder()
                                         .setMetric( sampleComponent )
                                         .addAllValues( List.of( 5926.0, 371.0, 540.0, 650.0, 1501.0 ) )
                                         .build();

        DiagramStatistic statistic = DiagramStatistic.newBuilder()
                                                     .addStatistics( forecastProbability )
                                                     .addStatistics( observedFrequency )
                                                     .addStatistics( sampleSize )
                                                     .setMetric( metric )
                                                     .build();

        // Fake output wrapper.
        List<DiagramStatisticOuter> fakeOutputData =
                Collections.singletonList( DiagramStatisticOuter.of( statistic, fakeMetadata ) );

        return fakeOutputData;
    }

    /**
     * Returns a {@link List} containing a {@link DurationDiagramStatisticOuter} that
     * represents the output of time-to-peak error for each pair in a pool.
     * 
     * @return time time-to-peak errors for one pool
     */

    public static List<DurationDiagramStatisticOuter> getTimeToPeakErrorsForOnePool()
    {

        // location id
        FeatureKey feature = FeatureKey.of( "FTSC1" );

        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

        Instant firstInstant = Instant.parse( "1985-01-01T00:00:00Z" );
        Instant secondInstant = Instant.parse( "1985-01-02T00:00:00Z" );
        Instant thirdInstant = Instant.parse( "1985-01-03T00:00:00Z" );

        DurationDiagramMetric metric = DurationDiagramMetric.newBuilder()
                                                            .setName( MetricName.TIME_TO_PEAK_ERROR )
                                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( Long.MIN_VALUE )
                                                                                                     .setNanos( 999_999_999 ) )
                                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                     .setSeconds( 0 ) )
                                                            .build();


        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( firstInstant.getEpochSecond() )
                                                                                  .setNanos( firstInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 3600 ) )
                                                               .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( secondInstant.getEpochSecond() )
                                                                                  .setNanos( secondInstant.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 7200 ) )
                                                               .build();

        PairOfInstantAndDuration three = PairOfInstantAndDuration.newBuilder()
                                                                 .setTime( Timestamp.newBuilder()
                                                                                    .setSeconds( thirdInstant.getEpochSecond() )
                                                                                    .setNanos( thirdInstant.getNano() ) )
                                                                 .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                           .setSeconds( 10800 ) )
                                                                 .build();

        DurationDiagramStatistic expectedSource = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( metric )
                                                                          .addStatistics( one )
                                                                          .addStatistics( two )
                                                                          .addStatistics( three )
                                                                          .build();

        return Collections.singletonList( DurationDiagramStatisticOuter.of( expectedSource, fakeMetadata ) );
    }

    /**
     * Returns a {@link List} containing a {@link DoubleScoreStatisticOuter} that
     * represents the output of several score statistics for one pool.
     * 
     * @return several score statistics for one pool
     */

    public static List<DoubleScoreStatisticOuter> getScoreStatisticsForOnePool()
    {

        // location id
        final FeatureKey feature = FeatureKey.of( "DRRC2" );

        TimeWindowOuter timeOne = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setMinimum( 0 )
                                                                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                                                                       .setOptimum( 0.0 ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setMinimum( Double.NEGATIVE_INFINITY )
                                                                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                                                                       .setOptimum( 0.0 ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN )
                                                                                                                       .setMinimum( 0 )
                                                                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                                                                       .setOptimum( 0.0 ) ) )
                                    .build();

        List<DoubleScoreStatisticOuter> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( DoubleScoreStatisticOuter.of( one, fakeMetadata ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( two, fakeMetadata ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( three, fakeMetadata ) );

        return Collections.unmodifiableList( fakeOutputs );
    }

    /**
     * Returns a {@link List} containing a {@link DurationScoreStatisticOuter} that
     * represents the summary statistics of the time-to-peak-errors for one pool.
     * 
     * @return the summary statistics of time-to-peak errors for one pool
     */

    public static List<DurationScoreStatisticOuter> getDurationScoreStatisticsForOnePool()
    {

        // location id
        final FeatureKey feature = FeatureKey.of( "DOLC2" );

        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .setMetric( metric )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MEAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 3_600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MEDIAN ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 7_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setMetric( DurationScoreMetricComponent.newBuilder()
                                                                                                                             .setName( DurationScoreMetricComponent.ComponentName.MAXIMUM ) )

                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 10_800 ) ) )
                                      .build();

        // Fake output wrapper.
        return Collections.singletonList( DurationScoreStatisticOuter.of( score, fakeMetadata ) );
    }

    /**
     * Returns a {@link List} containing a {@link DoubleScoreStatisticOuter} that
     * represents the output of one score statistic for several pools of data, including
     * missing values for some pools.
     * 
     * @return one score statistics for several pools
     */

    public static List<DoubleScoreStatisticOuter> getScoreStatisticsForThreePoolsWithMissings()
    {

        // location id
        final FeatureKey feature = FeatureKey.of( "FTSC1" );

        TimeWindowOuter timeOne = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setLeftVariableName( "QINE" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          thresholdOne,
                                          false );

        SampleMetadata fakeMetadataA = SampleMetadata.of( evaluation, pool );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName( ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatisticOuter fakeOutputA = DoubleScoreStatisticOuter.of( one, fakeMetadataA );

        // Add the data for another threshold at the same time
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( feature,
                                                               feature,
                                                               null ),
                                             timeOne,
                                             null,
                                             thresholdTwo,
                                             false );

        SampleMetadata fakeMetadataB = SampleMetadata.of( evaluation, poolTwo );

        DoubleScoreStatisticOuter fakeOutputB = DoubleScoreStatisticOuter.of( one, fakeMetadataB );

        // Add data for another time, and one threshold only
        TimeWindowOuter timeTwo = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        Pool poolThree = MessageFactory.parse( new FeatureTuple( feature,
                                                                 feature,
                                                                 null ),
                                               timeTwo,
                                               null,
                                               thresholdOne,
                                               false );

        SampleMetadata fakeMetadataC = SampleMetadata.of( evaluation, poolThree );

        DoubleScoreStatisticOuter fakeOutputC = DoubleScoreStatisticOuter.of( one, fakeMetadataC );

        // Fake output wrapper.
        return Arrays.asList( fakeOutputA, fakeOutputB, fakeOutputC );
    }

}
