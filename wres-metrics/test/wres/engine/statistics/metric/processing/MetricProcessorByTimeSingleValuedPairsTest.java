package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.junit.Test;

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
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.*;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.Boilerplate;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DurationDiagramStatistic.PairOfInstantAndDuration;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link MetricProcessorByTimeSingleValuedPairs}.
 * 
 * @author James Brown
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

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) )
                                                .getProjectConfig();

        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        FeatureTuple featureTuple = Boilerplate.getFeatureTuple();
        Map<FeatureTuple, ThresholdsByMetric> thresholdsByMetricAndFeature = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholdsByMetricAndFeature, 0 );

        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                new MetricProcessorByTimeSingleValuedPairs( metrics,
                                                            Executors.newSingleThreadExecutor(),
                                                            Executors.newSingleThreadExecutor() );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsSix();

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

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) )
                                                .getProjectConfig();
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsSix();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        // Generate results for 10 nominal lead times
        List<DoubleScoreStatisticOuter> scores = new ArrayList<>();

        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );

            wres.statistics.generated.Pool pool = pairs.getMetadata()
                                                       .getPool()
                                                       .toBuilder()
                                                       .setTimeWindow( window.getTimeWindow() )
                                                       .build();

            PoolMetadata meta = PoolMetadata.of( evaluation, pool );

            Pool<TimeSeries<Pair<Double, Double>>> next =
                    new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( pairs, false )
                                                                        .setMetadata( meta )
                                                                        .build();

            StatisticsForProject statistics = processor.apply( next );
            scores.addAll( statistics.getDoubleScoreStatistics() );
        }

        // Validate a subset of the data 
        assertEquals( 10, Slicer.filter( scores, MetricConstants.THREAT_SCORE ).size() );

        assertEquals( 20 * 8 + 10,
                      Slicer.filter( scores,
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX,
                                                             Instant.MIN,
                                                             Instant.MAX,
                                                             Duration.ofHours( 1 ),
                                                             Duration.ofHours( 1 ) );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT,
                                                          MeasurementUnit.of( "CMS" ) ) );

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    expectedWindow,
                                                                    null,
                                                                    expectedThreshold,
                                                                    false );

        PoolMetadata expectedMeta = PoolMetadata.of( evaluation, pool );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 400 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 100 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( scores,
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
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );

        //Check for the expected number of metrics
        int expected = SampleDataGroup.SINGLE_VALUED.getMetrics().size()
                       + SampleDataGroup.DICHOTOMOUS.getMetrics().size()
                       - MetricConstants.CONTINGENCY_TABLE.getAllComponents().size();
        int actual = processor.metrics.getMetrics().size();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyTimeSeriesMetrics() throws MetricParameterException, InterruptedException
    {
        // Mock some metrics
        List<TimeSeriesMetricConfig> metrics = new ArrayList<>();
        metrics.add( new TimeSeriesMetricConfig( null, TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR, null ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( null, 0, null, metrics ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( mockedConfig );

        //Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        Pool<TimeSeries<Pair<Double, Double>>> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        List<DurationDiagramStatisticOuter> actual = new ArrayList<>();
        StatisticsForProject some = processor.apply( first );
        StatisticsForProject more = processor.apply( second );
        actual.addAll( some.getInstantDurationPairStatistics() );
        actual.addAll( more.getInstantDurationPairStatistics() );

        //Validate the outputs
        //Compare the errors against the benchmark

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

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    firstWindow,
                                                                    null,
                                                                    thresholds,
                                                                    false );

        PoolMetadata m1 = PoolMetadata.of( evaluation, pool );

        wres.statistics.generated.Pool poolTwo = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                       secondWindow,
                                                                       null,
                                                                       thresholds,
                                                                       false );

        PoolMetadata m2 = PoolMetadata.of( evaluation, poolTwo );

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
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
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
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
        metrics.add( new TimeSeriesMetricConfig( null, TimeSeriesMetricConfigName.TIME_TO_PEAK_ERROR, null ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, 0, null, metrics ) ),
                                   null,
                                   null,
                                   null );

        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( mockedConfig );

        //Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        Pool<TimeSeries<Pair<Double, Double>>> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        List<DurationDiagramStatisticOuter> actual = new ArrayList<>();
        StatisticsForProject some = processor.apply( first );
        StatisticsForProject more = processor.apply( second );
        actual.addAll( some.getInstantDurationPairStatistics() );
        actual.addAll( more.getInstantDurationPairStatistics() );

        //Validate the outputs
        //Compare the errors against the benchmark

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

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    null,
                                                                    null,
                                                                    null,
                                                                    false );

        PoolMetadata source = PoolMetadata.of( evaluation, pool );

        List<DurationDiagramStatisticOuter> expected = new ArrayList<>();

        PairOfInstantAndDuration one = PairOfInstantAndDuration.newBuilder()
                                                               .setTime( Timestamp.newBuilder()
                                                                                  .setSeconds( FIRST_DATE.getEpochSecond() )
                                                                                  .setNanos( FIRST_DATE.getNano() ) )
                                                               .setDuration( com.google.protobuf.Duration.newBuilder()
                                                                                                         .setSeconds( -21600 ) )
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
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
                                                               .setReferenceTimeType( ReferenceTimeType.T0 )
                                                               .build();

        DurationDiagramStatistic expectedSecond = DurationDiagramStatistic.newBuilder()
                                                                          .setMetric( TimeToPeakError.METRIC )
                                                                          .addStatistics( two )
                                                                          .build();

        expected.add( DurationDiagramStatisticOuter.of( expectedFirst,
                                                        PoolMetadata.of( source,
                                                                         firstWindow,
                                                                         firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( DurationDiagramStatistic.newBuilder()
                                                                                .setMetric( TimeToPeakError.METRIC )
                                                                                .build(),
                                                        PoolMetadata.of( source,
                                                                         firstWindow,
                                                                         secondThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        PoolMetadata.of( source,
                                                                         secondWindow,
                                                                         firstThreshold ) ) );

        expected.add( DurationDiagramStatisticOuter.of( expectedSecond,
                                                        PoolMetadata.of( source,
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
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );

        //Break into two time-series to test sequential calls
        Pool<TimeSeries<Pair<Double, Double>>> first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();

        Pool<TimeSeries<Pair<Double, Double>>> second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        Pool<TimeSeries<Pair<Double, Double>>> aggPool =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( first.get() )
                                                                    .addData( second.get() )
                                                                    .setMetadata( PoolSlicer.unionOf( List.of( first.getMetadata(),
                                                                                                               second.getMetadata() ) ) )
                                                                    .build();

        //Compute the metrics
        StatisticsForProject project = processor.apply( aggPool );

        //Validate the outputs
        //Compare the errors against the benchmark
        List<DurationScoreStatisticOuter> actualScores = project.getDurationScoreStatistics();

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

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    timeWindow,
                                                                    null,
                                                                    thresholds,
                                                                    false );

        PoolMetadata scoreMeta = PoolMetadata.of( evaluation, pool );

        com.google.protobuf.Duration expectedMean = MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMedian = MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMin = MessageFactory.parse( Duration.ofHours( -6 ) );
        com.google.protobuf.Duration expectedMax = MessageFactory.parse( Duration.ofHours( 12 ) );
        com.google.protobuf.Duration expectedMeanAbs = MessageFactory.parse( Duration.ofHours( 9 ) );

        DurationScoreMetricComponent baseMetric =
                DurationScoreMetricComponent.newBuilder()
                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MAX_VALUE )
                                                                                     .setNanos( 999_999_999 ) )
                                            .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( 0 ) )
                                            .build();

        DurationScoreMetricComponent meanMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .build();

        DurationScoreMetricComponent medianMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                         .setName( ComponentName.MEDIAN )
                                                                                         .build();

        DurationScoreMetricComponent minMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .build();

        DurationScoreMetricComponent maxMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .build();

        DurationScoreMetricComponent meanAbsMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                          .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( 0 ) )
                                                                                          .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( Long.MAX_VALUE )
                                                                                                                                   .setNanos( 999_999_999 ) )
                                                                                          .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( 0 ) )
                                                                                          .build();


        DurationScoreStatisticComponent meanComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                       .setMetric( meanMetricComponent )
                                                                                       .setValue( expectedMean )
                                                                                       .build();

        DurationScoreStatisticComponent medianComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                         .setMetric( medianMetricComponent )
                                                                                         .setValue( expectedMedian )
                                                                                         .build();

        DurationScoreStatisticComponent minComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( minMetricComponent )
                                                                                      .setValue( expectedMin )
                                                                                      .build();

        DurationScoreStatisticComponent maxComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( maxMetricComponent )
                                                                                      .setValue( expectedMax )
                                                                                      .build();

        DurationScoreStatisticComponent meanAbsComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                          .setMetric( meanAbsMetricComponent )
                                                                                          .setValue( expectedMeanAbs )
                                                                                          .build();
        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
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

        ThresholdsByMetric.Builder builder = new ThresholdsByMetric.Builder();

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

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) )
                                                .getProjectConfig();

        // Ensure that the entire set of thresholds is assembled to be passed to the processor
        builder.addThresholds( ThresholdsGenerator.getThresholdsFromConfig( config ).getOneOrTwoThresholds() );

        ThresholdsByMetric thresholdsByMetric = builder.build();
        FeatureTuple featureTuple = Boilerplate.getFeatureTuple();
        Map<FeatureTuple, ThresholdsByMetric> thresholdsByMetricAndFeature = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholdsByMetricAndFeature, 0 );

        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                new MetricProcessorByTimeSingleValuedPairs( metrics,
                                                            ForkJoinPool.commonPool(),
                                                            ForkJoinPool.commonPool() );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsSix();

        // Generate results for 20 nominal lead times
        List<DoubleScoreStatisticOuter> statistics = new ArrayList<>();
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );

            FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

            Evaluation evaluation = Evaluation.newBuilder()
                                              .setRightVariableName( "SQIN" )
                                              .setMeasurementUnit( "CMS" )
                                              .setRightDataName( "HEFS" )
                                              .build();

            wres.statistics.generated.Pool pool = MessageFactory.parse( featureGroup,
                                                                        window,
                                                                        null,
                                                                        null,
                                                                        false );

            PoolMetadata meta = PoolMetadata.of( evaluation, pool );

            Pool<TimeSeries<Pair<Double, Double>>> next =
                    new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( pairs, false )
                                                                        .setMetadata( meta )
                                                                        .build();

            StatisticsForProject some = processor.apply( next );
            statistics.addAll( some.getDoubleScoreStatistics() );
        }

        // Validate a subset of the data       
        assertEquals( 20,
                      Slicer.filter( statistics,
                                     MetricConstants.THREAT_SCORE )
                            .size() );

        assertEquals( 30 * 8 + 20,
                      Slicer.filter( statistics,
                                     metric -> metric.getMetricName() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        TimeWindowOuter expectedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX,
                                                             Instant.MIN,
                                                             Instant.MAX,
                                                             Duration.ofHours( 1 ),
                                                             Duration.ofHours( 1 ) );

        OneOrTwoThresholds expectedThreshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 0.5 ),
                                                          Operator.GREATER_EQUAL,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "HEFS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    expectedWindow,
                                                                    null,
                                                                    expectedThreshold,
                                                                    false );

        PoolMetadata expectedMeta = PoolMetadata.of( evaluation, pool );

        DoubleScoreStatistic table =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ContingencyTable.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                 .setValue( 500 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.FALSE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                 .setValue( 0 ) )
                                    .build();


        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( table, expectedMeta );

        DoubleScoreStatisticOuter actual = Slicer.filter( statistics,
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
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsSeven();

        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( i ) );

            FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

            Evaluation evaluation = Evaluation.newBuilder()
                                              .setRightVariableName( "SQIN" )
                                              .setRightDataName( "HEFS" )
                                              .setMeasurementUnit( "CMS" )
                                              .build();

            wres.statistics.generated.Pool pool = MessageFactory.parse( featureGroup,
                                                                        window,
                                                                        null,
                                                                        null,
                                                                        false );

            PoolMetadata meta = PoolMetadata.of( evaluation, pool );

            Pool<TimeSeries<Pair<Double, Double>>> next =
                    new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( pairs.get() )
                                                                        .setMetadata( meta )
                                                                        .build();

            StatisticsForProject statistics = processor.apply( next );
            assertTrue( statistics.getDoubleScoreStatistics().isEmpty() );
        }
    }

    @Test
    public void testApplyTimeSeriesSummaryStatsWithNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();

        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );

        Pool<TimeSeries<Pair<Double, Double>>> pairs =
                MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        //Compute the metrics
        StatisticsForProject statistics = processor.apply( pairs );

        //Validate the outputs
        List<DurationScoreStatisticOuter> actualScores = statistics.getDurationScoreStatistics();

        //Metadata
        TimeWindowOuter combinedWindow = TimeWindowOuter.of( Instant.MIN,
                                                             Instant.MAX );
        TimeWindowOuter timeWindow = combinedWindow;

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT_AND_RIGHT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( STREAMFLOW )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( Boilerplate.getFeatureGroup(),
                                                                    timeWindow,
                                                                    null,
                                                                    thresholds,
                                                                    false );

        PoolMetadata scoreMeta = PoolMetadata.of( evaluation, pool );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
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
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config );
        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsEight();

        // Generate results
        TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                     Instant.MAX,
                                                     Duration.ZERO );

        FeatureGroup featureGroup = Boilerplate.getFeatureGroup();

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "SQIN" )
                                          .setRightDataName( "AHPS" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        wres.statistics.generated.Pool pool = MessageFactory.parse( featureGroup,
                                                                    window,
                                                                    null,
                                                                    null,
                                                                    false );

        PoolMetadata meta = PoolMetadata.of( evaluation, pool );

        Pool<TimeSeries<Pair<Double, Double>>> next =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addPool( pairs, false )
                                                                    .setMetadata( meta )
                                                                    .build();

        StatisticsForProject statistics = processor.apply( next );

        // Check the sample size
        double size = Slicer.filter( statistics.getDoubleScoreStatistics(),
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
        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( new ProjectConfig( null,
                                                                                                                     null,
                                                                                                                     null,
                                                                                                                     null,
                                                                                                                     null,
                                                                                                                     null ) );

        NullPointerException actual = assertThrows( NullPointerException.class, () -> processor.apply( null ) );

        assertEquals( "Expected non-null input to the metric processor.", actual.getMessage() );
    }

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricParameterException
    {
        MetricsConfig metrics =
                new MetricsConfig( null,
                                   0,
                                   Arrays.asList( new MetricConfig( null, MetricConfigName.FREQUENCY_BIAS ) ),
                                   null );

        ProjectConfig config = new ProjectConfig( null,
                                                  null,
                                                  Arrays.asList( metrics ),
                                                  null,
                                                  null,
                                                  null );

        MetricConfigException actual =
                assertThrows( MetricConfigException.class,
                              () -> MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( config ) );

        assertEquals( "Cannot configure 'FREQUENCY BIAS' without thresholds to define the "
                      + "events: add one or more thresholds to the configuration for each instance of "
                      + "'FREQUENCY BIAS'.",
                      actual.getMessage() );

    }

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricParameterException
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.THREAT_SCORE ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>> processor =
                MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( mockedConfig );

        Pool<TimeSeries<Pair<Double, Double>>> pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTen();

        ThresholdException actual = assertThrows( ThresholdException.class, () -> processor.apply( pairs ) );

        assertEquals( "Cannot add quantiles to probability thresholds without a climatological data source. Add a "
                      + "climatological data source to pool PoolMetadata[leftDataName=,rightDataName=,"
                      + "baselineDataName=,leftVariableName=,rightVariableName=MAP,baselineVariableName=,"
                      + "isBaselinePool=false,features=FeatureGroup[name=,features=[FeatureTuple[left="
                      + "FeatureKey[name=A,description=,srid=0,wkt=],right=FeatureKey[name=A,description=,"
                      + "srid=0,wkt=],baseline=FeatureKey[name=A,description=,srid=0,wkt=]]]],timeWindow="
                      + "[1985-01-01T00:00:00Z,2010-12-31T11:59:59Z,-1000000000-01-01T00:00:00Z,"
                      + "+1000000000-12-31T23:59:59.999999999Z,PT24H,PT24H],thresholds=<null>,timeScale=<null>,"
                      + "measurementUnit=MM/DAY] and try again.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnConstructionWithEnsembleMetric()
            throws MetricParameterException
    {
        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );

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
                                   Arrays.asList( new MetricsConfig( thresholds, 0, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricConfigException actual =
                assertThrows( MetricConfigException.class,
                              () -> MetricProcessorByTimeSingleValuedPairsTest.ofMetricProcessorForSingleValuedPairs( mockedConfig ) );

        assertEquals( "Cannot configure 'CONTINUOUS RANKED PROBABILITY SCORE' for single-valued inputs: "
                      + "correct the configuration.",
                      actual.getMessage() );
    }

    /**
     * @param config project declaration
     * @return a single-valued processor instance
     */

    private static MetricProcessor<Pool<TimeSeries<Pair<Double, Double>>>>
            ofMetricProcessorForSingleValuedPairs( ProjectConfig config )
    {
        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        FeatureTuple featureTuple = Boilerplate.getFeatureTuple();
        Map<FeatureTuple, ThresholdsByMetric> thresholds = Map.of( featureTuple, thresholdsByMetric );
        ThresholdsByMetricAndFeature metrics = ThresholdsByMetricAndFeature.of( thresholds, 0 );

        return new MetricProcessorByTimeSingleValuedPairs( metrics,
                                                           ForkJoinPool.commonPool(),
                                                           ForkJoinPool.commonPool() );
    }

}
