package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

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

    private static final String SECOND_DATE = "1985-01-02T00:00:00Z";

    /**
     * Another date for testing.
     */

    private static final String FIRST_DATE = "1985-01-01T00:00:00Z";

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
                                                                     null,
                                                                     Executors.newSingleThreadExecutor(),
                                                                     Executors.newSingleThreadExecutor(),
                                                                     null );
        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        StatisticsForProject results = processor.apply( pairs );
        List<DoubleScoreStatistic> bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION );
        List<DoubleScoreStatistic> cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION );
        List<DoubleScoreStatistic> rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        List<DoubleScoreStatistic> mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR );
        List<DoubleScoreStatistic> me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR );
        List<DoubleScoreStatistic> rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        List<DoubleScoreStatistic> ve =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.VOLUMETRIC_EFFICIENCY );
        List<BoxPlotStatistics> bpe =
                Slicer.filter( results.getBoxPlotStatisticsPerPool(), MetricConstants.BOX_PLOT_OF_ERRORS );

        //Test contents
        assertEquals( Double.valueOf( 1.6666666666666667 ), bias.get( 0 ).getData() );
        assertEquals( Double.valueOf( 1.0 ), cod.get( 0 ).getData() );
        assertEquals( Double.valueOf( 1.0 ), rho.get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), mae.get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), me.get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), rmse.get( 0 ).getData() );
        assertEquals( Double.valueOf( -0.6666666666666666 ), ve.get( 0 ).getData() );
        assertEquals( VectorOfDoubles.of( 5, 5, 5, 5, 5 ), bpe.get( 0 ).getData().get( 0 ).getData() );
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
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     Duration.ofHours( i ) );
            final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
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
                                     metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( 1 ) );

        final OneOrTwoThresholds expectedThreshold = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                          Operator.GREATER,
                                                                                          ThresholdDataType.LEFT,
                                                                                          MeasurementUnit.of( "CMS" ) ) );

        StatisticMetadata expectedMeta =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                               "SQIN",
                                                                               "HEFS" ),
                                                         expectedWindow,
                                                         expectedThreshold ),
                                      500,
                                      MeasurementUnit.of(),
                                      MetricConstants.CONTINGENCY_TABLE,
                                      null );

        Map<MetricConstants, Double> elements = new HashMap<>();
        elements.put( MetricConstants.TRUE_POSITIVES, 400.0 );
        elements.put( MetricConstants.TRUE_NEGATIVES, 0.0 );
        elements.put( MetricConstants.FALSE_POSITIVES, 100.0 );
        elements.put( MetricConstants.FALSE_NEGATIVES, 0.0 );

        DoubleScoreStatistic expected = DoubleScoreStatistic.of( elements, expectedMeta );

        DoubleScoreStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                     meta -> meta.getSampleMetadata()
                                                                 .getThresholds()
                                                                 .equals( expectedThreshold )
                                                             && meta.getSampleMetadata()
                                                                    .getTimeWindow()
                                                                    .equals( expectedWindow )
                                                             && meta.getMetricID() == MetricConstants.CONTINGENCY_TABLE )
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
        List<PairedStatistic<Instant, Duration>> actual =
                processor.getCachedMetricOutput().getPairedStatistics();

        //Build the expected output
        List<Pair<Instant, Duration>> expectedFirst = new ArrayList<>();
        List<Pair<Instant, Duration>> expectedSecond = new ArrayList<>();
        expectedFirst.add( Pair.of( Instant.parse( FIRST_DATE ), Duration.ofHours( -6 ) ) );
        expectedSecond.add( Pair.of( Instant.parse( SECOND_DATE ), Duration.ofHours( 12 ) ) );
        // Metadata for the output
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( FIRST_DATE ),
                                                Instant.parse( FIRST_DATE ),
                                                Duration.ofHours( 6 ),
                                                Duration.ofHours( 18 ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( SECOND_DATE ),
                                                 Instant.parse( SECOND_DATE ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT ) );

        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( Location.of( "A" ),
                                                                                              STREAMFLOW ),
                                                                        firstWindow,
                                                                        thresholds ),
                                                     1,
                                                     MeasurementUnit.of( DURATION ),
                                                     MetricConstants.TIME_TO_PEAK_ERROR,
                                                     MetricConstants.MAIN );

        StatisticMetadata m2 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( Location.of( "A" ),
                                                                                              STREAMFLOW ),
                                                                        secondWindow,
                                                                        thresholds ),
                                                     1,
                                                     MeasurementUnit.of( DURATION ),
                                                     MetricConstants.TIME_TO_PEAK_ERROR,
                                                     MetricConstants.MAIN );

        List<PairedStatistic<Instant, Duration>> expected = new ArrayList<>();
        expected.add( PairedStatistic.of( expectedFirst, m1 ) );
        expected.add( PairedStatistic.of( expectedSecond, m2 ) );

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
        List<PairedStatistic<Instant, Duration>> actual =
                processor.getCachedMetricOutput().getPairedStatistics();

        //Build the expected output
        List<Pair<Instant, Duration>> expectedFirst = new ArrayList<>();
        List<Pair<Instant, Duration>> expectedSecond = new ArrayList<>();
        expectedFirst.add( Pair.of( Instant.parse( FIRST_DATE ), Duration.ofHours( -6 ) ) );
        expectedSecond.add( Pair.of( Instant.parse( SECOND_DATE ), Duration.ofHours( 12 ) ) );

        // Metadata for the output
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( FIRST_DATE ),
                                                Instant.parse( FIRST_DATE ),
                                                Duration.ofHours( 6 ),
                                                Duration.ofHours( 18 ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( SECOND_DATE ),
                                                 Instant.parse( SECOND_DATE ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );

        OneOrTwoThresholds firstThreshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT ) );
        OneOrTwoThresholds secondThreshold = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 5.0 ),
                                                                                  Operator.GREATER,
                                                                                  ThresholdDataType.LEFT_AND_RIGHT ) );

        SampleMetadata source = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                   DatasetIdentifier.of( Location.of( "A" ),
                                                                         STREAMFLOW ) );

        List<PairedStatistic<Instant, Duration>> expected = new ArrayList<>();

        expected.add( PairedStatistic.of( expectedFirst,
                                          StatisticMetadata.of( SampleMetadata.of( source,
                                                                                   firstWindow,
                                                                                   firstThreshold ),
                                                                1,
                                                                MeasurementUnit.of( DURATION ),
                                                                MetricConstants.TIME_TO_PEAK_ERROR,
                                                                MetricConstants.MAIN ) ) );

        expected.add( PairedStatistic.of( Arrays.asList(),
                                          StatisticMetadata.of( SampleMetadata.of( source,
                                                                                   firstWindow,
                                                                                   secondThreshold ),
                                                                0,
                                                                MeasurementUnit.of( DURATION ),
                                                                MetricConstants.TIME_TO_PEAK_ERROR,
                                                                MetricConstants.MAIN ) ) );

        expected.add( PairedStatistic.of( expectedSecond,
                                          StatisticMetadata.of( SampleMetadata.of( source,
                                                                                   secondWindow,
                                                                                   firstThreshold ),
                                                                1,
                                                                MeasurementUnit.of( DURATION ),
                                                                MetricConstants.TIME_TO_PEAK_ERROR,
                                                                MetricConstants.MAIN ) ) );

        expected.add( PairedStatistic.of( expectedSecond,
                                          StatisticMetadata.of( SampleMetadata.of( source,
                                                                                   secondWindow,
                                                                                   secondThreshold ),
                                                                1,
                                                                MeasurementUnit.of( DURATION ),
                                                                MetricConstants.TIME_TO_PEAK_ERROR,
                                                                MetricConstants.MAIN ) ) );

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
        List<DurationScoreStatistic> actualScores =
                processor.getCachedMetricOutput().getDurationScoreStatistics();

        //Build the expected statistics
        Map<MetricConstants, Duration> expectedSource = new EnumMap<>( MetricConstants.class );
        expectedSource.put( MetricConstants.MEAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MEDIAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MINIMUM, Duration.ofHours( -6 ) );
        expectedSource.put( MetricConstants.MAXIMUM, Duration.ofHours( 12 ) );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, Duration.ofHours( 9 ) );

        //Metadata
        TimeWindow combinedWindow = TimeWindow.of( Instant.parse( FIRST_DATE ),
                                                   Instant.parse( SECOND_DATE ),
                                                   Duration.ofHours( 6 ),
                                                   Duration.ofHours( 18 ) );
        final TimeWindow timeWindow = combinedWindow;

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT ) );

        StatisticMetadata scoreMeta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                               DatasetIdentifier.of( Location.of( "A" ),
                                                                                                     STREAMFLOW ),
                                                                               timeWindow,
                                                                               thresholds ),
                                                            2,
                                                            MeasurementUnit.of( DURATION ),
                                                            MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                            null );

        DurationScoreStatistic expectedScoresSource = DurationScoreStatistic.of( expectedSource, scoreMeta );
        List<DurationScoreStatistic> expectedScores = new ArrayList<>();
        expectedScores.add( expectedScoresSource );

        assertEquals( expectedScores, actualScores );
    }

    @Test
    public void testApplyWithThresholdsFromSource()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        // Define the external thresholds to use
        Map<MetricConstants, Set<Threshold>> canonical = new EnumMap<>( MetricConstants.class );

        Set<Threshold> thresholds =
                new HashSet<>( Arrays.asList( Threshold.of( OneOrTwoDoubles.of( 0.5 ),
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
        MetricProcessor<PoolOfPairs<Double, Double>> processor =
                MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                     thresholdsByMetric,
                                                                     StatisticType.set() );

        PoolOfPairs<Double, Double> pairs = MetricTestDataFactory.getSingleValuedPairsFour();

        // Generate results for 20 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     Duration.ofHours( i ) );
            final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
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
                                     metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( 1 ) );

        final OneOrTwoThresholds expectedThreshold = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 0.5 ),
                                                                                          Operator.GREATER_EQUAL,
                                                                                          ThresholdDataType.LEFT ) );

        StatisticMetadata expectedMeta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                                  DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                        "SQIN",
                                                                                                        "HEFS" ),
                                                                                  expectedWindow,
                                                                                  expectedThreshold ),
                                                               500,
                                                               MeasurementUnit.of(),
                                                               MetricConstants.CONTINGENCY_TABLE,
                                                               null );

        Map<MetricConstants, Double> elements = new HashMap<>();
        elements.put( MetricConstants.TRUE_POSITIVES, 500.0 );
        elements.put( MetricConstants.TRUE_NEGATIVES, 0.0 );
        elements.put( MetricConstants.FALSE_POSITIVES, 0.0 );
        elements.put( MetricConstants.FALSE_NEGATIVES, 0.0 );

        DoubleScoreStatistic expected = DoubleScoreStatistic.of( elements, expectedMeta );

        DoubleScoreStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                     meta -> meta.getSampleMetadata()
                                                                 .getThresholds()
                                                                 .equals( expectedThreshold )
                                                             && meta.getSampleMetadata()
                                                                    .getTimeWindow()
                                                                    .equals( expectedWindow )
                                                             && meta.getMetricID() == MetricConstants.CONTINGENCY_TABLE )
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
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     Duration.ofHours( i ) );
            final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
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
                                     metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                            .size() );

        // Expected result
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         Duration.ofHours( 1 ) );

        final OneOrTwoThresholds expectedThreshold = OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                                                          Operator.GREATER,
                                                                                          ThresholdDataType.LEFT,
                                                                                          MeasurementUnit.of( "CMS" ) ) );

        StatisticMetadata expectedMeta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                                  DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                        "SQIN",
                                                                                                        "HEFS" ),
                                                                                  expectedWindow,
                                                                                  expectedThreshold ),
                                                               0,
                                                               MeasurementUnit.of(),
                                                               MetricConstants.CONTINGENCY_TABLE,
                                                               null );

        Map<MetricConstants, Double> elements = new HashMap<>();
        elements.put( MetricConstants.TRUE_POSITIVES, 0.0 );
        elements.put( MetricConstants.TRUE_NEGATIVES, 0.0 );
        elements.put( MetricConstants.FALSE_POSITIVES, 0.0 );
        elements.put( MetricConstants.FALSE_NEGATIVES, 0.0 );

        DoubleScoreStatistic expected = DoubleScoreStatistic.of( elements, expectedMeta );

        DoubleScoreStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                                     meta -> meta.getSampleMetadata()
                                                                 .getThresholds()
                                                                 .equals( expectedThreshold )
                                                             && meta.getSampleMetadata()
                                                                    .getTimeWindow()
                                                                    .equals( expectedWindow )
                                                             && meta.getMetricID() == MetricConstants.CONTINGENCY_TABLE )
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
        List<DurationScoreStatistic> actualScores =
                processor.getCachedMetricOutput().getDurationScoreStatistics();

        //Build the expected statistics
        Map<MetricConstants, Duration> expectedSource = new EnumMap<>( MetricConstants.class );
        expectedSource.put( MetricConstants.MEAN, null );
        expectedSource.put( MetricConstants.MEDIAN, null );
        expectedSource.put( MetricConstants.MINIMUM, null );
        expectedSource.put( MetricConstants.MAXIMUM, null );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, null );

        //Metadata
        TimeWindow combinedWindow = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX );
        final TimeWindow timeWindow = combinedWindow;

        OneOrTwoThresholds thresholds =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT_AND_RIGHT ) );

        StatisticMetadata scoreMeta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                               DatasetIdentifier.of( Location.of( "A" ),
                                                                                                     STREAMFLOW ),
                                                                               timeWindow,
                                                                               thresholds ),
                                                            0,
                                                            MeasurementUnit.of( DURATION ),
                                                            MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                            null );

        DurationScoreStatistic expectedScoresSource = DurationScoreStatistic.of( expectedSource, scoreMeta );
        List<DurationScoreStatistic> expectedScores = new ArrayList<>();
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
        final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX,
                                                 Duration.ZERO );
        final SampleMetadata meta = new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                               .setIdentifier( DatasetIdentifier.of( Location.of( DRRC2 ),
                                                                                                     "SQIN",
                                                                                                     "AHPS" ) )
                                                               .setTimeWindow( window )
                                                               .build();
        PoolOfPairs<Double, Double> next =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( pairs ).setMetadata( meta ).build();

        processor.apply( next );

        // Check the sample size
        Double size = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     sampleMeta -> sampleMeta.getMetricID() == MetricConstants.SAMPLE_SIZE
                                                   && !sampleMeta.getSampleMetadata()
                                                                 .getThresholds()
                                                                 .first()
                                                                 .isFinite() )
                            .get( 0 )
                            .getData();

        assertTrue( FunctionFactory.doubleEquals().test( 10.0, size ) );
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
