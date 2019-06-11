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
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.MetricConstants.SampleDataGroup;
import wres.datamodel.MetricConstants.StatisticGroup;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticException;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
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

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml 
     * and pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        null,
                                                                        Executors.newSingleThreadExecutor(),
                                                                        Executors.newSingleThreadExecutor(),
                                                                        null );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        StatisticsForProject results = processor.apply( pairs );
        ListOfStatistics<DoubleScoreStatistic> bias =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.BIAS_FRACTION );
        ListOfStatistics<DoubleScoreStatistic> cod =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.COEFFICIENT_OF_DETERMINATION );
        ListOfStatistics<DoubleScoreStatistic> rho =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        ListOfStatistics<DoubleScoreStatistic> mae =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ABSOLUTE_ERROR );
        ListOfStatistics<DoubleScoreStatistic> me =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.MEAN_ERROR );
        ListOfStatistics<DoubleScoreStatistic> rmse =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        ListOfStatistics<DoubleScoreStatistic> ve =
                Slicer.filter( results.getDoubleScoreStatistics(), MetricConstants.VOLUMETRIC_EFFICIENCY );
        ListOfStatistics<BoxPlotStatistics> bpe =
                Slicer.filter( results.getBoxPlotStatisticsPerPool(), MetricConstants.BOX_PLOT_OF_ERRORS );

        //Test contents
        assertEquals( Double.valueOf( 1.6666666666666667 ), bias.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( 1.0 ), cod.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( 1.0 ), rho.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), mae.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), me.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( 5.0 ), rmse.getData().get( 0 ).getData() );
        assertEquals( Double.valueOf( -0.6666666666666666 ), ve.getData().get( 0 ).getData() );
        assertEquals( VectorOfDoubles.of( 5, 5, 5, 5, 5 ), bpe.getData().get( 0 ).getData().get( 0 ).getData() );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml and 
     * pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}. Tests the output for multiple 
     * calls with separate forecast lead times.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyWithThresholds() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config, StatisticGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();

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
            processor.apply( SingleValuedPairs.of( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data 
        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 10 );

        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 20 * 8 );

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
                                      MetricConstants.MAIN );

        MatrixStatistic expected =
                MatrixStatistic.of( new double[][] { { 400.0, 100.0 }, { 0.0, 0.0 } },
                                    Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                   MetricDimension.FALSE_POSITIVES,
                                                   MetricDimension.FALSE_NEGATIVES,
                                                   MetricDimension.TRUE_NEGATIVES ),
                                    expectedMeta );

        MatrixStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getMatrixStatistics(),
                                                meta -> meta.getSampleMetadata()
                                                            .getThresholds()
                                                            .equals( expectedThreshold )
                                                        && meta.getSampleMetadata()
                                                               .getTimeWindow()
                                                               .equals( expectedWindow ) )
                                       .getData()
                                       .get( 0 );

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} for all valid metrics associated
     * with single-valued inputs.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testForExpectedMetricsWhenAllValidConfigured() throws IOException, MetricParameterException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        //Check for the expected number of metrics
        int actual = SampleDataGroup.SINGLE_VALUED.getMetrics().size()
                     + SampleDataGroup.DICHOTOMOUS.getMetrics().size();
        int expected =
                processor.metrics.size();

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to a time-series metric using 
     * pairs obtained from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. Tests the output for 
     * multiple calls with subsets of data, caching the results across calls.
     * 
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

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

        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        StatisticGroup.set() );

        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        TimeSeriesOfSingleValuedPairs second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        ListOfStatistics<PairedStatistic<Instant, Duration>> actual =
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

        List<PairedStatistic<Instant, Duration>> inList = new ArrayList<>();
        inList.add( PairedStatistic.of( expectedFirst, m1 ) );
        inList.add( PairedStatistic.of( expectedSecond, m2 ) );
        ListOfStatistics<PairedStatistic<Instant, Duration>> expected = ListOfStatistics.of( inList );

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to a time-series metric using 
     * pairs obtained from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. The test includes a
     * thershold constraint on the paired data.
     * 
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

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

        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        StatisticGroup.set() );

        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        TimeSeriesOfSingleValuedPairs second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        ListOfStatistics<PairedStatistic<Instant, Duration>> actual =
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

        List<PairedStatistic<Instant, Duration>> inList = new ArrayList<>();

        inList.add( PairedStatistic.of( expectedFirst,
                                        StatisticMetadata.of( SampleMetadata.of( source,
                                                                                 firstWindow,
                                                                                 firstThreshold ),
                                                              1,
                                                              MeasurementUnit.of( DURATION ),
                                                              MetricConstants.TIME_TO_PEAK_ERROR,
                                                              MetricConstants.MAIN ) ) );

        inList.add( PairedStatistic.of( Arrays.asList(),
                                        StatisticMetadata.of( SampleMetadata.of( source,
                                                                                 firstWindow,
                                                                                 secondThreshold ),
                                                              0,
                                                              MeasurementUnit.of( DURATION ),
                                                              MetricConstants.TIME_TO_PEAK_ERROR,
                                                              MetricConstants.MAIN ) ) );

        inList.add( PairedStatistic.of( expectedSecond,
                                        StatisticMetadata.of( SampleMetadata.of( source,
                                                                                 secondWindow,
                                                                                 firstThreshold ),
                                                              1,
                                                              MeasurementUnit.of( DURATION ),
                                                              MetricConstants.TIME_TO_PEAK_ERROR,
                                                              MetricConstants.MAIN ) ) );

        inList.add( PairedStatistic.of( expectedSecond,
                                        StatisticMetadata.of( SampleMetadata.of( source,
                                                                                 secondWindow,
                                                                                 secondThreshold ),
                                                              1,
                                                              MeasurementUnit.of( DURATION ),
                                                              MetricConstants.TIME_TO_PEAK_ERROR,
                                                              MetricConstants.MAIN ) ) );

        ListOfStatistics<PairedStatistic<Instant, Duration>> expected = ListOfStatistics.of( inList );

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from 
     * testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml and pairs obtained 
     * from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. Tests the summary statistics generated 
     * at the end of multiple calls with subsets of time-series data.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesSummaryStats()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();

        TimeSeriesOfSingleValuedPairs second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        ListOfStatistics<DurationScoreStatistic> actualScores =
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
        List<DurationScoreStatistic> scoreInList = new ArrayList<>();
        scoreInList.add( expectedScoresSource );

        ListOfStatistics<DurationScoreStatistic> expectedScores = ListOfStatistics.of( scoreInList );

        assertEquals( expectedScores, actualScores );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} with a canonical source of thresholds
     * and the application of {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to configuration 
     * obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml and pairs obtained 
     * from {@link MetricTestDataFactory#getSingleValuedPairsFour()}. Tests the output for multiple calls with separate 
     * forecast lead times.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

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
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        thresholdsByMetric,
                                                                        StatisticGroup.set() );

        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();

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
            processor.apply( SingleValuedPairs.of( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data       
        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 20 );

        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 30 * 8 );

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
                                                               MetricConstants.MAIN );

        MatrixStatistic expected =
                MatrixStatistic.of( new double[][] { { 500.0, 0.0 }, { 0.0, 0.0 } },
                                    Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                   MetricDimension.FALSE_POSITIVES,
                                                   MetricDimension.FALSE_NEGATIVES,
                                                   MetricDimension.TRUE_NEGATIVES ),
                                    expectedMeta );

        MatrixStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getMatrixStatistics(),
                                                meta -> meta.getSampleMetadata()
                                                            .getThresholds()
                                                            .equals( expectedThreshold )
                                                        && meta.getSampleMetadata()
                                                               .getTimeWindow()
                                                               .equals( expectedWindow ) )
                                       .getData()
                                       .get( 0 );

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml and 
     * pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsSeven()}. Tests the output for multiple 
     * calls with separate forecast lead times.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyWithThresholdsAndNoData() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config, StatisticGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsSeven();

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
            processor.apply( SingleValuedPairs.of( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data       
        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 10 );

        assertTrue( Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                   metric -> metric.getMetricID() != MetricConstants.THREAT_SCORE )
                          .getData()
                          .size() == 20 * 8 );

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
                                                               MetricConstants.MAIN );

        MatrixStatistic expected =
                MatrixStatistic.of( new double[][] { { 0.0, 0.0 }, { 0.0, 0.0 } },
                                    Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                   MetricDimension.FALSE_POSITIVES,
                                                   MetricDimension.FALSE_NEGATIVES,
                                                   MetricDimension.TRUE_NEGATIVES ),
                                    expectedMeta );

        MatrixStatistic actual = Slicer.filter( processor.getCachedMetricOutput().getMatrixStatistics(),
                                                meta -> meta.getSampleMetadata()
                                                            .getThresholds()
                                                            .equals( expectedThreshold )
                                                        && meta.getSampleMetadata()
                                                               .getTimeWindow()
                                                               .equals( expectedWindow ) )
                                       .getData()
                                       .get( 0 );

        assertEquals( expected, actual );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from 
     * testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml and pairs obtained 
     * from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsFour()}. Tests the summary statistics when 
     * supplied with empty pairs.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricConfigException if the metric configuration is incorrect
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesSummaryStatsWithNoData()
            throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();

        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        StatisticGroup.set() );

        TimeSeriesOfSingleValuedPairs pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        //Compute the metrics
        processor.apply( pairs );

        //Validate the outputs
        ListOfStatistics<DurationScoreStatistic> actualScores =
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
        List<DurationScoreStatistic> scoreInList = new ArrayList<>();
        scoreInList.add( expectedScoresSource );

        ListOfStatistics<DurationScoreStatistic> expectedScores = ListOfStatistics.of( scoreInList );

        assertEquals( expectedScores, actualScores );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml and 
     * pairs that contain missing values, which should be removed.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws StatisticException if the results could not be generated 
     */

    @Test
    public void testApplyWithMissingPairsForRemoval() throws IOException, MetricParameterException, InterruptedException
    {
        String configPath = TEST_SOURCE;

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config, StatisticGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsEight();

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
        processor.apply( SingleValuedPairs.of( pairs.getRawData(), meta ) );

        // Check the sample size
        Double size = Slicer.filter( processor.getCachedMetricOutput().getDoubleScoreStatistics(),
                                     sampleMeta -> sampleMeta.getMetricID() == MetricConstants.SAMPLE_SIZE
                                                   && !sampleMeta.getSampleMetadata()
                                                                 .getThresholds()
                                                                 .first()
                                                                 .isFinite() )
                            .getData()
                            .get( 0 )
                            .getData();

        assertTrue( FunctionFactory.doubleEquals().test( 10.0, size ) );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception on receiving null input.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Expected non-null input to the metric processor." );
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( new ProjectConfig( null,
                                                                                           null,
                                                                                           null,
                                                                                           null,
                                                                                           null,
                                                                                           null ),
                                                                        Collections.singleton( StatisticGroup.DOUBLE_SCORE ) );
        processor.apply( null );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when attempting to compute metrics that require thresholds and no thresholds are configured.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

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
        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( new ProjectConfig( null,
                                                                                           null,
                                                                                           Arrays.asList( metrics ),
                                                                                           null,
                                                                                           null,
                                                                                           null ),
                                                                        Collections.singleton( StatisticGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );

    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when climatological observations are required but missing.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

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


        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        Collections.singleton( StatisticGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when time-series metrics are configured and the input pairs are not 
     * {@link TimeSeriesOfSingleValuedPairs}.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    @Test
    public void testApplyThrowsExceptionWhenOrdinaryPairsSuppliedForTimeSeriesMetrics()
            throws MetricParameterException
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "The project configuration includes time-series metrics. "
                                 + "Expected a time-series of single-valued pairs as input." );

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

        MetricProcessor<SingleValuedPairs, StatisticsForProject> processor =
                MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        Collections.singleton( StatisticGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsFour() );
    }

    /**
     * Tests that the construction of a {@link MetricProcessorByTime} fails when the configuration
     * contains an ensemble metric. 
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

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

        MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                null );
    }

    /**
     * Tests that the construction of a {@link MetricProcessorByTime} fails when time-series metrics
     * are configured alongside non-time-series metrics.
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

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

        MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                null );
    }


}
