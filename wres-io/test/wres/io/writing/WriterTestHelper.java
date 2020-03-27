package wres.io.writing;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.GraphicalType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.DiagramStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindow;

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
        GraphicalType graphics = new GraphicalType( null, 800, 600, null );
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
     * Returns a {@link ListOfStatistics} containing {@link BoxPlotStatistics} for two pools of data.
     * 
     * @return a box plot per pool for two pools
     */

    public static ListOfStatistics<BoxPlotStatistics> getBoxPlotPerPoolForTwoPools()
    {
        // location id
        String LID = "JUNP1";

        // Create fake outputs
        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        List<BoxPlotStatistic> fakeOutputsOne = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputsOne.add( BoxPlotStatistic.of( probs,
                                                 VectorOfDoubles.of( 1, 3, 5, 7, 9 ),
                                                 fakeMetadataOne ) );

        TimeWindow timeTwo =
                TimeWindow.of( Instant.MIN,
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

        List<BoxPlotStatistic> fakeOutputsTwo = Collections.singletonList( BoxPlotStatistic.of( probs,
                                                                                                VectorOfDoubles.of( 11,
                                                                                                                    33,
                                                                                                                    55,
                                                                                                                    77,
                                                                                                                    99 ),
                                                                                                fakeMetadataTwo ) );

        // Fake output wrapper.
        ListOfStatistics<BoxPlotStatistics> fakeOutputData =
                ListOfStatistics.of( Arrays.asList( BoxPlotStatistics.of( fakeOutputsOne,
                                                                          fakeMetadataOne ),
                                                    BoxPlotStatistics.of( fakeOutputsTwo,
                                                                          fakeMetadataTwo ) ) );
        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing {@link BoxPlotStatistics} for several pairs.
     * 
     * @return a box plot per pair
     */

    public static ListOfStatistics<BoxPlotStatistics> getBoxPlotPerPairForOnePool()
    {
        // location id
        String LID = "JUNP1";

        // Create fake outputs
        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        List<BoxPlotStatistic> fakeOutputs = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 2, 3, 4, 5, 6 ),
                                              fakeMetadata,
                                              1,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 7, 9, 11, 13, 15 ),
                                              fakeMetadata,
                                              3,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 21, 24, 27, 30, 33 ),
                                              fakeMetadata,
                                              5,
                                              MetricDimension.OBSERVED_VALUE ) );

        // Fake output wrapper.
        ListOfStatistics<BoxPlotStatistics> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( BoxPlotStatistics.of( fakeOutputs,
                                                                                      fakeMetadata ) ) );

        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing a {@link DiagramStatistic} that 
     * represents the output of a reliability diagram for one pool.
     * 
     * @return a reliability diagram for one pool
     */

    public static ListOfStatistics<DiagramStatistic> getReliabilityDiagramForOnePool()
    {

        // location id
        String LID = "CREC1";

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 11.94128 ),
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

        Map<MetricDimension, VectorOfDoubles> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricDimension.FORECAST_PROBABILITY,
                         VectorOfDoubles.of( 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 ) );
        fakeOutputs.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                         VectorOfDoubles.of( 0.06294, 0.2938, 0.5, 0.73538, 0.93937 ) );
        fakeOutputs.put( MetricDimension.SAMPLE_SIZE, VectorOfDoubles.of( 5926, 371, 540, 650, 1501 ) );

        // Fake output wrapper.
        ListOfStatistics<DiagramStatistic> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( DiagramStatistic.of( fakeOutputs, fakeMetadata ) ) );

        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing a {@link PairedStatistic} that 
     * represents the output of time-to-peak error for each pair in a pool.
     * 
     * @return time time-to-peak errors for one pool
     */

    public static ListOfStatistics<PairedStatistic<Instant, Duration>> getTimeToPeakErrorsForOnePool()
    {

        // location id
        String LID = "FTSC1";

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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
        ListOfStatistics<PairedStatistic<Instant, Duration>> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( PairedStatistic.of( fakeOutputs, fakeMetadata ) ) );

        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing a {@link DoubleScoreStatistic} that 
     * represents the output of several score statistics for one pool.
     * 
     * @return several score statistics for one pool
     */

    public static ListOfStatistics<DoubleScoreStatistic> getScoreStatisticsForOnePool()
    {

        // location id
        final String LID = "DRRC2";

        TimeWindow timeOne = TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        List<DoubleScoreStatistic> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( DoubleScoreStatistic.of( 1.0, fakeMetadataA ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 2.0, fakeMetadataB ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 3.0, fakeMetadataC ) );

        // Fake output wrapper.
        ListOfStatistics<DoubleScoreStatistic> fakeOutputData =
                ListOfStatistics.of( fakeOutputs );

        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing a {@link DurationScoreStatistic} that 
     * represents the summary statistics of the time-to-peak-errors for one pool.
     * 
     * @return the summary statistics of time-to-peak errors for one pool
     */

    public static ListOfStatistics<DurationScoreStatistic> getDurationScoreStatisticsForOnePool()
    {

        // location id
        final String LID = "DOLC2";

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        Map<MetricConstants, Duration> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricConstants.MEAN, Duration.ofHours( 1 ) );
        fakeOutputs.put( MetricConstants.MEDIAN, Duration.ofHours( 2 ) );
        fakeOutputs.put( MetricConstants.MAXIMUM, Duration.ofHours( 3 ) );

        // Fake output wrapper.
        ListOfStatistics<DurationScoreStatistic> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( DurationScoreStatistic.of( fakeOutputs,
                                                                                           fakeMetadata ) ) );

        return fakeOutputData;
    }

    /**
     * Returns a {@link ListOfStatistics} containing a {@link DoubleScoreStatistic} that 
     * represents the output of one score statistic for several pools of data, including
     * missing values for some pools.
     * 
     * @return one score statistics for several pools
     */

    public static ListOfStatistics<DoubleScoreStatistic> getScoreStatisticsForThreePoolsWithMissings()
    {

        // location id
        final String LID = "FTSC1";

        TimeWindow timeOne = TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
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

        DoubleScoreStatistic fakeOutputA = DoubleScoreStatistic.of( 1.0, fakeMetadataA );

        // Add the data for another threshold at the same time
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
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

        DoubleScoreStatistic fakeOutputB = DoubleScoreStatistic.of( 1.0, fakeMetadataB );

        // Add data for another time, and one threshold only
        TimeWindow timeTwo = TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeTwo,
                                                         thresholdOne ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        DoubleScoreStatistic fakeOutputC = DoubleScoreStatistic.of( 1.0, fakeMetadataC );

        // Fake output wrapper.
        ListOfStatistics<DoubleScoreStatistic> fakeOutputData =
                ListOfStatistics.of( Arrays.asList( fakeOutputA, fakeOutputB, fakeOutputC ) );

        return fakeOutputData;
    }

}
