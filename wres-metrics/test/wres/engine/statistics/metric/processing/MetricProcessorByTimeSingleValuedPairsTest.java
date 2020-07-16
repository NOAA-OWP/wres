package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.math3.util.Precision;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.protobuf.Timestamp;

import wres.config.MetricConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeSeriesMetricConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.*;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link MetricProcessorByTimeSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorByTimeSingleValuedPairsTest
{

    /**
     * Test source.
     */

    private static final String TEST_SOURCE =
            "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

    /**
     * Streamflow for metadata.
     */

    private static final String STREAMFLOW = "Streamflow";

    /**
     * Duration for metadata.
     */

    private static final String DURATION = "DURATION";

    /**
     * Location for metadata.
     */

    private static final String DRRC2 = "DRRC2";

    /**
     * A date for testing.
     */

    private static final Instant FIRST_DATE = Instant.parse( "1985-01-01T00:00:00Z" );

    /**
     * Another date for testing.
     */

    private static final Instant SECOND_DATE = Instant.parse( "1985-01-02T00:00:00Z" );

    /**
     * Test thresholds.
     */

    private static final String TEST_THRESHOLDS = "0.1,0.2,0.3";

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     ThresholdsGenerator.getThresholdsFromConfig( config ),
                                                                     Executors.newSingleThreadExecutor(),
                                                                     Executors.newSingleThreadExecutor(),
                                                                     null );
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        StatisticsForProject results = processor.apply( pairs );
        List<DoubleScoreStatisticOuter> bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION );
        List<DoubleScoreStatisticOuter> cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION );
        List<DoubleScoreStatisticOuter> rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        List<DoubleScoreStatisticOuter> mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR );
        List<DoubleScoreStatisticOuter> me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR );
        List<DoubleScoreStatisticOuter> rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        List<DoubleScoreStatisticOuter> ve =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.VOLUMETRIC_EFFICIENCY );
        List<BoxplotStatisticOuter> bpe =
                Slicer.filter( results.getBoxPlotStatisticsPerPool(), MetricConstants.BOX_PLOT_OF_ERRORS );

        //Test contents
        assertEquals( 1.6666666666666667,
                      bias.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        assertEquals( 1.0,
                      cod.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 1.0,
                      rho.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      mae.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      me.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( 5.0,
                      rmse.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );
        assertEquals( -0.6666666666666666,
                      ve.get( 0 ).getComponent( MetricConstants.MAIN ).getData().getValue(),
                      Precision.EPSILON );

        assertEquals( List.of( 5.0, 5.0, 5.0, 5.0, 5.0 ),
                      bpe.get( 0 ).getData().getStatistics( 0 ).getQuantilesList() );
    }

    @Test
    public void testApplyWithThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config, StatisticType.set() );
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );
            SampleMetadata meta = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                              .setIdentifier( DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) )
                                                              .setTimeWindow( window )
                                                              .build();

            PoolOfPairs<Double, Double> next =
                    new PoolOfPairsBuilder<Double, Double>().addTimeSeries( pairs ).setMetadata( meta ).build();

            processor.apply( next );
        }

        // Validate a subset of the data 
        assertEquals( 10,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     MetricConstants.THREAT_SCORE )
                            .size() );

        assertEquals( 20 * 8 + 10,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        final TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.MIN,
                                                                   Instant.MAX,
                                                                   Duration.ofHours( 1 ) );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT,
                                                          MeasurementUnit.of( "CMS" ) ) );

        SampleMetadata expectedMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                               "SQIN",
                                                                               "HEFS" ),
                                                         expectedWindow,
                                                         expectedThreshold );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                 .setValue( 400 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                 .setValue( 100 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                          meta -> meta.getMetadata()
                                                                      .getThresholds()
                                                                      .equals( expectedThreshold )
                                                                  && meta.getMetadata()
                                                                         .getTimeWindow()
                                                                         .equals( expectedWindow )
                                                                  && meta.getMetricName() == MetricConstants.CONTINGENCY_TABLE )
                                                 .get( 0 );

        assertEquals( expected, actual );
    }

    @Test
    public void testForExpectedMetricsWhenAllValidConfigured() throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     StatisticType.set() );

        //Check for the expected number of metrics
        int expected = SampleDataGroup.SINGLE_VALUED.getMetrics().size()
                       + SampleDataGroup.DICHOTOMOUS.getMetrics().size();
        int actual = processor.metrics.size();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesMetrics() throws MetricParameterException, InterruptedException
    {
        // Mock some metrics
        List<TimeSeriesMetricConfig> metrics = new ArrayList<>();
        metrics.add( new TimeSeriesMetricConfig( null, null, TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR, null ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, null, metrics ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( mockedConfig,
                                                                     StatisticType.set() );

        //Break into two time-series to test sequential calls
        PoolOfPairs<Double, Double> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        PoolOfPairs<Double, Double> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        List<DurationDiagramStatisticOuter> actual =
                processor.getCachedMetricOutput().getInstantDurationPairStatistics();

        //Build the expected output
        // Metadata for the output
        TimeWindowOuter firstWindow = TimeWindowOuter.of( FIRST_DATE,
                                                          FIRST_DATE,
                                                          Duration.ofHours( 6 ),
                                                          Duration.ofHours( 18 ) );
        TimeWindowOuter secondWindow = TimeWindowOuter.of( SECOND_DATE,
                                                           SECOND_DATE,
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT_AND_RIGHT ) );

        SampleMetadata m1 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( Location.of( "A" ),
                                                                     STREAMFLOW ),
                                               firstWindow,
                                               thresholds );

        SampleMetadata m2 = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                               DatasetIdentifier.of( Location.of( "A" ),
                                                                     STREAMFLOW ),
                                               secondWindow,
                                               thresholds );

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .build();

        DurationDiagramStatistic expectedFirst = DurationDiagramStatistic.newBuilder()
                                                                         .setMetric( TimeToPeakError.METRIC )
                                                                         .addStatistics( one )
                                                                         .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( SECOND_DATE.getEpochSecond() )
                                                                                  .setNanos( SECOND_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 43200 ) )
                                                               .build();

        DurationDiagramStatistic expectedSecond = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakError.METRIC )
                                                                          .addStatistics( two )
                                                                          .build();

        List<DurationDiagramStatisticOuter> expected = new ArrayList<>();
        expected.add( DurationDiagramStatisticOuter.of( expectedFirst, m1 ) );
        expected.add( DurationDiagramStatisticOuter.of( expectedSecond, m2 ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesMetricsWithThresholds() throws MetricParameterException, InterruptedException
    {
        // Mock some metrics
        List<TimeSeriesMetricConfig> metrics = new ArrayList<>();
        metrics.add( new TimeSeriesMetricConfig( null, null, TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR, null ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.VALUE,
                                              wres.config.generated.ThresholdDataType.LEFT_AND_RIGHT,
                                              "5.0",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, null, metrics ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( mockedConfig,
                                                                     StatisticType.set() );

        //Break into two time-series to test sequential calls
        PoolOfPairs<Double, Double> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        PoolOfPairs<Double, Double> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        List<DurationDiagramStatisticOuter> actual = processor.getCachedMetricOutput()
                                                              .getInstantDurationPairStatistics();

        //Build the expected output
        // Metadata for the output
        TimeWindowOuter firstWindow = TimeWindowOuter.of( FIRST_DATE,
                                                          FIRST_DATE,
                                                          Duration.ofHours( 6 ),
                                                          Duration.ofHours( 18 ) );
        TimeWindowOuter secondWindow = TimeWindowOuter.of( SECOND_DATE,
                                                           SECOND_DATE,
                                                           Duration.ofHours( 6 ),
                                                           Duration.ofHours( 18 ) );

        OneOrTwoThresholds firstThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT_AND_RIGHT ) );
        OneOrTwoThresholds secondThreshold = OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 5.0 ),
                                                                                       Operator.GREATER,
                                                                                       ThresholdDataType.LEFT_AND_RIGHT ) );

        SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                   DatasetIdentifier.of( Location.of( "A" ),
                                                                         STREAMFLOW ) );

        List<DurationDiagramStatisticOuter> expected = new ArrayList<>();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .build();

        DurationDiagramStatistic expectedFirst = DurationDiagramStatistic.newBuilder()
                                                                         .setMetric( TimeToPeakError.METRIC )
                                                                         .addStatistics( one )
                                                                         .build();

        PairOfInstantAndDuration two = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( SECOND_DATE.getEpochSecond() )
                                                                                  .setNanos( SECOND_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( 43200 ) )
                                                               .build();

        DurationDiagramStatistic expectedSecond = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakError.METRIC )
                                                                          .addStatistics( two )
                                                                          .build();

        expected.add( DurationDiagramStatisticOuter.of( expectedFirst,
                                                        SampleMetadata.of( source,
                                                                           firstWindow,
                                                                           firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( DurationDiagramStatistic.newBuilder()
                                                                                .setMetric( TimeToPeakError.METRIC )
                                                                                .build(),
                                                        SampleMetadata.of( source,
                                                                           firstWindow,
                                                                           secondThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        SampleMetadata.of( source,
                                                                           secondWindow,
                                                                           firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        SampleMetadata.of( source,
                                                                           secondWindow,
                                                                           secondThreshold ) ) );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesSummaryStats()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     StatisticType.set() );

        //Break into two time-series to test sequential calls
        PoolOfPairs<Double, Double> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();

        PoolOfPairs<Double, Double> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        List<DurationScoreStatisticOuter> actualScores =
                processor.getCachedMetricOutput().getDurationScoreStatistics();

        //Build the expected statistics
        Map<MetricConstants, Duration> expectedSource = new EnumMap<>( MetricConstants.class );
        expectedSource.put( MetricConstants.MEAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MEDIAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MINIMUM, Duration.ofHours( -6 ) );
        expectedSource.put( MetricConstants.MAXIMUM, Duration.ofHours( 12 ) );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, Duration.ofHours( 9 ) );

        //Metadata
        TimeWindowOuter combinedWindow = TimeWindowOuter.of( FIRST_DATE,
                                                             SECOND_DATE,
                                                             Duration.ofHours( 6 ),
                                                             Duration.ofHours( 18 ) );
        final TimeWindowOuter timeWindow = combinedWindow;

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT_AND_RIGHT ) );

        SampleMetadata scoreMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                      DatasetIdentifier.of( Location.of( "A" ),
                                                                            STREAMFLOW ),
                                                      timeWindow,
                                                      thresholds );

        com.google.protobuf.Duration expectedMean = MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMedian = MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMin = MessageFactory.parse( Duration.ofHours( -6 ) );
        com.google.protobuf.Duration expectedMax = MessageFactory.parse( Duration.ofHours( 12 ) );
        com.google.protobuf.Duration expectedMeanAbs = MessageFactory.parse( Duration.ofHours( 9 ) );

        DurationScoreStatisticComponent meanComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .setValue( expectedMean )
                                                                                       .build();

        DurationScoreStatisticComponent medianComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                         .setName( ComponentName.MEDIAN )
                                                                                         .setValue( expectedMedian )
                                                                                         .build();

        DurationScoreStatisticComponent minComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .setValue( expectedMin )
                                                                                      .build();

        DurationScoreStatisticComponent maxComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .setValue( expectedMax )
                                                                                      .build();

        DurationScoreStatisticComponent meanAbsComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                          .setValue( expectedMeanAbs )
                                                                                          .build();

        DurationScoreMetricComponent meanMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .build();

        DurationScoreMetricComponent medianMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                         .setName( ComponentName.MEDIAN )
                                                                                         .build();

        DurationScoreMetricComponent minMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .build();

        DurationScoreMetricComponent maxMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .build();

        DurationScoreMetricComponent meanAbsMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                          .build();

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .addComponents( meanMetricComponent )
                                                        .addComponents( medianMetricComponent )
                                                        .addComponents( minMetricComponent )
                                                        .addComponents( maxMetricComponent )
                                                        .addComponents( meanAbsMetricComponent )
                                                        .build();
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .addStatistics( meanComponent )
                                                             .addStatistics( medianComponent )
                                                             .addStatistics( minComponent )
                                                             .addStatistics( maxComponent )
                                                             .addStatistics( meanAbsComponent )
                                                             .build();

        DurationScoreStatisticOuter expectedScoresSource = DurationScoreStatisticOuter.of( score, scoreMeta );
        List<DurationScoreStatisticOuter> expectedScores = new ArrayList<>();
        expectedScores.add( expectedScoresSource );

        assertEquals( expectedScores, actualScores );
    }


    @Test
    public void testApplyWithThresholdsFromSource()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        // Define the external thresholds to use
        Map<MetricConstants, Set<ThresholdOuter>> canonical = new EnumMap<>( MetricConstants.class );

        Set<ThresholdOuter> thresholds =
                new HashSet<>( Arrays.asList( ThresholdOuter.of( OneOrTwoDoubles.of( 0.5 ),
                                                                 Operator.GREATER_EQUAL,
                                                                 ThresholdDataType.LEFT ) ) );

        ThresholdsByMetricBuilder builder = new ThresholdsByMetricBuilder();

        canonical.put( MetricConstants.MEAN_ERROR, thresholds );
        canonical.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, thresholds );
        canonical.put( MetricConstants.MEAN_ABSOLUTE_ERROR, thresholds );
        canonical.put( MetricConstants.MEAN_SQUARE_ERROR, thresholds );
        canonical.put( MetricConstants.BIAS_FRACTION, thresholds );
        canonical.put( MetricConstants.COEFFICIENT_OF_DETERMINATION, thresholds );
        canonical.put( MetricConstants.ROOT_MEAN_SQUARE_ERROR, thresholds );
        canonical.put( MetricConstants.THREAT_SCORE, thresholds );
        canonical.put( MetricConstants.SAMPLE_SIZE, thresholds );
        canonical.put( MetricConstants.CONTINGENCY_TABLE, thresholds );
        canonical.put( MetricConstants.QUANTILE_QUANTILE_DIAGRAM, thresholds );

        builder.addThresholds( canonical, ThresholdConstants.ThresholdGroup.PROBABILITY );
        ThresholdsByMetric thresholdsByMetric = builder.build();

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();

        // Ensure that the entire set of thresholds is assembled to be passed to the processor
        thresholdsByMetric =
                ThresholdsGenerator.getThresholdsFromConfig( config ).unionWithThisStore( thresholdsByMetric );
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     thresholdsByMetric,
                                                                     StatisticType.set() );

        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        // Generate results for 20 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );
            SampleMetadata meta = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                              .setIdentifier( DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) )
                                                              .setTimeWindow( window )
                                                              .build();
            PoolOfPairs<Double, Double> next =
                    new PoolOfPairsBuilder<Double, Double>().addTimeSeries( pairs ).setMetadata( meta ).build();

            processor.apply( next );
        }

        // Validate a subset of the data       
        assertEquals( 20,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     MetricConstants.THREAT_SCORE )
                            .size() );

        assertEquals( 30 * 8 + 20,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX,
                                                             Duration.ofHours( 1 ) );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.5 ),
                                                          Operator.GREATER_EQUAL,
                                                          ThresholdDataType.LEFT ) );

        SampleMetadata expectedMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                               "SQIN",
                                                                               "HEFS" ),
                                                         expectedWindow,
                                                         expectedThreshold );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                 .setValue( 500 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();


        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                          meta -> meta.getMetadata()
                                                                      .getThresholds()
                                                                      .equals( expectedThreshold )
                                                                  && meta.getMetadata()
                                                                         .getTimeWindow()
                                                                         .equals( expectedWindow )
                                                                  && meta.getMetricName() == MetricConstants.CONTINGENCY_TABLE )
                                                 .get( 0 );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithThresholdsAndNoData() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config, StatisticType.set() );
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsSeven();

        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );
            SampleMetadata meta = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                              .setIdentifier( DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) )
                                                              .setTimeWindow( window )
                                                              .build();

            PoolOfPairs<Double, Double> next =
                    new PoolOfPairsBuilder<Double, Double>().addTimeSeries( pairs ).setMetadata( meta ).build();

            processor.apply( next );
        }

        // Validate a subset of the data       
        assertEquals( 10,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     MetricConstants.THREAT_SCORE )
                            .size() );

        assertEquals( 20 * 8 + 10,
                      Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX,
                                                             Duration.ofHours( 1 ) );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT,
                                                          MeasurementUnit.of( "CMS" ) ) );

        SampleMetadata expectedMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                               "SQIN",
                                                                               "HEFS" ),
                                                         expectedWindow,
                                                         expectedThreshold );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_POSITIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_POSITIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setName( DoubleScoreMetricComponent.ComponentName.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                          meta -> meta.getMetadata()
                                                                      .getThresholds()
                                                                      .equals( expectedThreshold )
                                                                  && meta.getMetadata()
                                                                         .getTimeWindow()
                                                                         .equals( expectedWindow )
                                                                  && meta.getMetricName() == MetricConstants.CONTINGENCY_TABLE )
                                                 .get( 0 );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesSummaryStatsWithNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();

        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     StatisticType.set() );

        PoolOfPairs<Double, Double> pairs =
                MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        //Compute the metrics
        processor.apply( pairs );

        //Validate the outputs
        List<DurationScoreStatisticOuter> actualScores =
                processor.getCachedMetricOutput().getDurationScoreStatistics();

        //Metadata
        TimeWindowOuter combinedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX );
        final TimeWindowOuter timeWindow = combinedWindow;

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT_AND_RIGHT ) );

        SampleMetadata scoreMeta = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                      DatasetIdentifier.of( Location.of( "A" ),
                                                                            STREAMFLOW ),
                                                      timeWindow,
                                                      thresholds );

        DurationScoreMetricComponent mean = DurationScoreMetricComponent.newBuilder()
                                                                        .setName( ComponentName.MEAN )
                                                                        .build();
        DurationScoreMetricComponent median = DurationScoreMetricComponent.newBuilder()
                                                                          .setName( ComponentName.MEDIAN )
                                                                          .build();
        DurationScoreMetricComponent minimum = DurationScoreMetricComponent.newBuilder()
                                                                           .setName( ComponentName.MINIMUM )
                                                                           .build();
        DurationScoreMetricComponent maximum = DurationScoreMetricComponent.newBuilder()
                                                                           .setName( ComponentName.MAXIMUM )
                                                                           .build();
        DurationScoreMetricComponent meanAbsolute = DurationScoreMetricComponent.newBuilder()
                                                                                .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                .build();

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .addComponents( mean )
                                                        .addComponents( median )
                                                        .addComponents( minimum )
                                                        .addComponents( maximum )
                                                        .addComponents( meanAbsolute )
                                                        .build();
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .build();

        DurationScoreStatisticOuter expectedScoresSource = DurationScoreStatisticOuter.of( score, scoreMeta );
        List<DurationScoreStatisticOuter> expectedScores = new ArrayList<>();
        expectedScores.add( expectedScoresSource );

        assertEquals( expectedScores, actualScores );
    }

    @Test
    public void testApplyWithMissingPairsForRemoval() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config, StatisticType.set() );
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsEight();

        // Generate results
        TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                     Instant.MAX,
                                                     Duration.ZERO );
        SampleMetadata meta = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                          .setIdentifier( DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                "SQIN",
                                                                                                "AHPS" ) )
                                                          .setTimeWindow( window )
                                                          .build();
        PoolOfPairs<Double, Double> next =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( pairs ).setMetadata( meta ).build();

        processor.apply( next );

        // Check the sample size
        double size = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     sampleMeta -> sampleMeta.getMetricName() == MetricConstants.SAMPLE_SIZE
                                                   && !sampleMeta.getMetadata()
                                                                 .getThresholds()
                                                                 .first()
                                                                 .isFinite() )
                            .get( 0 )
                            .getComponent( MetricConstants.MAIN )
                            .getData()
                            .getValue();

        assertEquals( 10.0, size, Precision.EPSILON );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Expected non-null input to the metric processor." );
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( new ProjectConfig( null,
                                                                                        null,
                                                                                        null,
                                                                                        null,
                                                                                        null,
                                                                                        null ),
                                                                     Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( null );
    }

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Cannot configure 'FREQUENCY BIAS' without thresholds to define the "
                                 + "events: add one or more thresholds to the configuration." );

        MetricsConfig metrics =
                new MetricsConfig( null,
                                   Arrays.asList( new MetricConfig( null, null, MetricConfigName.FREQUENCY_BIAS ) ),
                                   null );
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( new ProjectConfig( null,
                                                                                        null,
                                                                                        Arrays.asList( metrics ),
                                                                                        null,
                                                                                        null,
                                                                                        null ),
                                                                     Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );

    }

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Unable to determine quantile threshold from probability threshold: no climatological "
                                 + "observations were available in the input" );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( mockedConfig,
                                                                     Collections.singleton( StatisticType.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
    }

    @Test
    public void testExceptionOnConstructionWithEnsembleMetric()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Cannot configure 'CONTINUOUS RANKED PROBABILITY SCORE' for single-valued inputs: "
                                 + "correct the configuration." );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorForSingleValuedPairs( mockedConfig,
                                                             null );
    }

    @Test
    public void testExceptionOnConstructionWhenMixingTimeSeriesMetricsWithOtherMetrics()
            throws MetricParameterException
    {
        exception.expect( MetricConfigException.class );
        exception.expectMessage( "Cannot configure time-series metrics together with non-time-series metrics: correct "
                                 + "the configuration." );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.THREAT_SCORE ) );

        List<TimeSeriesMetricConfig> timeMetrics = new ArrayList<>();
        timeMetrics.add( new TimeSeriesMetricConfig( null,
                                                     null,
                                                     TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR,
                                                     null ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              TEST_THRESHOLDS,
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, timeMetrics ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.ofMetricProcessorForSingleValuedPairs( mockedConfig,
                                                             null );
    }


}
