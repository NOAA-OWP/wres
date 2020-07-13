package wres.io.writing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.GraphicalType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatistic;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.PairedStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent;
import wres.statistics.generated.DiagramMetric.DiagramMetricComponent.DiagramComponentName;
import wres.statistics.generated.DiagramStatistic.DiagramStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
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
        return new Feature( null,
                            null,
                            null,
                            null,
                            null,
                            locationId,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
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
        String LID = "JUNP1";

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

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadataOne =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS,
                                      null );

        List<BoxplotStatistic> fakeOutputsOne = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputsOne.add( BoxplotStatistic.of( probs,
                                                 VectorOfDoubles.of( 1, 3, 5, 7, 9 ),
                                                 fakeMetadataOne ) );

        TimeWindowOuter timeTwo =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 48 ),
                                    Duration.ofHours( 48 ) );

        StatisticMetadata fakeMetadataTwo =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeTwo,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS,
                                      null );

        List<BoxplotStatistic> fakeOutputsTwo = Collections.singletonList( BoxplotStatistic.of( probs,
                                                                                                VectorOfDoubles.of( 11,
                                                                                                                    33,
                                                                                                                    55,
                                                                                                                    77,
                                                                                                                    99 ),
                                                                                                fakeMetadataTwo ) );

        // Fake output wrapper.
        List<BoxplotStatisticOuter> fakeOutputData =
                Arrays.asList( BoxplotStatisticOuter.of( fakeOutputsOne,
                                                         fakeMetadataOne ),
                               BoxplotStatisticOuter.of( fakeOutputsTwo,
                                                         fakeMetadataTwo ) );
        return fakeOutputData;
    }

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for several pairs.
     * 
     * @return a box plot per pair
     */

    public static List<BoxplotStatisticOuter> getBoxPlotPerPairForOnePool()
    {
        // location id
        String LID = "JUNP1";

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

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                      null );

        List<BoxplotStatistic> fakeOutputs = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputs.add( BoxplotStatistic.of( probs,
                                              VectorOfDoubles.of( 2, 3, 4, 5, 6 ),
                                              fakeMetadata,
                                              1,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxplotStatistic.of( probs,
                                              VectorOfDoubles.of( 7, 9, 11, 13, 15 ),
                                              fakeMetadata,
                                              3,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxplotStatistic.of( probs,
                                              VectorOfDoubles.of( 21, 24, 27, 30, 33 ),
                                              fakeMetadata,
                                              5,
                                              MetricDimension.OBSERVED_VALUE ) );

        // Fake output wrapper.
        List<BoxplotStatisticOuter> fakeOutputData =
                Collections.singletonList( BoxplotStatisticOuter.of( fakeOutputs,
                                                                     fakeMetadata ) );

        return fakeOutputData;
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
        String LID = "CREC1";

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

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.RELIABILITY_DIAGRAM,
                                      null );

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
                                            .addComponents( forecastComponent )
                                            .addComponents( observedComponent )
                                            .addComponents( sampleComponent )
                                            .setName( MetricName.RELIABILITY_DIAGRAM )
                                            .build();

        DiagramStatisticComponent forecastProbability =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.FORECAST_PROBABILITY )
                                         .addAllValues( List.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) )
                                         .build();

        DiagramStatisticComponent observedFrequency =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.OBSERVED_RELATIVE_FREQUENCY )
                                         .addAllValues( List.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) )
                                         .build();

        DiagramStatisticComponent sampleSize =
                DiagramStatisticComponent.newBuilder()
                                         .setName( DiagramComponentName.SAMPLE_SIZE )
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
     * Returns a {@link List} containing a {@link PairedStatisticOuter} that 
     * represents the output of time-to-peak error for each pair in a pool.
     * 
     * @return time time-to-peak errors for one pool
     */

    public static List<PairedStatisticOuter<Instant, Duration>> getTimeToPeakErrorsForOnePool()
    {

        // location id
        String LID = "FTSC1";

        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.TIME_TO_PEAK_ERROR,
                                      null );

        List<Pair<Instant, Duration>> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 2 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-03T00:00:00Z" ), Duration.ofHours( 3 ) ) );

        // Fake output wrapper.
        return Collections.singletonList( PairedStatisticOuter.of( fakeOutputs, fakeMetadata ) );
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
        final String LID = "DRRC2";

        TimeWindowOuter timeOne = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadataA =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        StatisticMetadata fakeMetadataB =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ERROR,
                                      MetricConstants.MAIN );
        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ABSOLUTE_ERROR,
                                      MetricConstants.MAIN );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 2.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.MEAN_ABSOLUTE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 3.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        List<DoubleScoreStatisticOuter> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( DoubleScoreStatisticOuter.of( one, fakeMetadataA ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( two, fakeMetadataB ) );
        fakeOutputs.add( DoubleScoreStatisticOuter.of( three, fakeMetadataC ) );

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
        final String LID = "DOLC2";

        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 1 ),
                                    Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      null );

        DurationScoreStatistic score =
                DurationScoreStatistic.newBuilder()
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MEAN )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 3_600 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MEDIAN )
                                                                                     .setValue( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 7_200 ) ) )
                                      .addStatistics( DurationScoreStatisticComponent.newBuilder()
                                                                                     .setName( DurationScoreMetricComponent.ComponentName.MAXIMUM )
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
        final String LID = "FTSC1";

        TimeWindowOuter timeOne = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP", LeftOrRightOrBaseline.RIGHT );

        StatisticMetadata fakeMetadataA =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         thresholdOne ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.MEAN_SQUARE_ERROR ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 1.0 )
                                                                                 .setName( ComponentName.MAIN ) )
                                    .build();

        DoubleScoreStatisticOuter fakeOutputA = DoubleScoreStatisticOuter.of( one, fakeMetadataA );

        // Add the data for another threshold at the same time
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 23.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        StatisticMetadata fakeMetadataB =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         thresholdTwo ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        DoubleScoreStatisticOuter fakeOutputB = DoubleScoreStatisticOuter.of( one, fakeMetadataB );

        // Add data for another time, and one threshold only
        TimeWindowOuter timeTwo = TimeWindowOuter.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeTwo,
                                                         thresholdOne ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        DoubleScoreStatisticOuter fakeOutputC = DoubleScoreStatisticOuter.of( one, fakeMetadataC );

        // Fake output wrapper.
        return Arrays.asList( fakeOutputA, fakeOutputB, fakeOutputC );
    }

}
