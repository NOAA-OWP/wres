package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.CoreMatchers;
import org.junit.Before;
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
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DefaultMetadataFactory;
import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsByMetric.ThresholdsByMetricBuilder;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MetricProcessorByTimeSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MetricProcessorByTimeSingleValuedPairsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Instance of a data factory.
     */

    private DataFactory dataFac;

    @Before
    public void setupBeforeEachTest()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml 
     * and pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithoutThresholds() throws IOException, MetricProcessorException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithoutThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        null,
                                                                        Executors.newSingleThreadExecutor(),
                                                                        Executors.newSingleThreadExecutor(),
                                                                        null );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        MetricOutputForProjectByTimeAndThreshold results = processor.apply( pairs );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> bias = results.getDoubleScoreOutput()
                                                                           .get( MetricConstants.BIAS_FRACTION );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> cod =
                results.getDoubleScoreOutput()
                       .get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rho = results.getDoubleScoreOutput()
                                                                          .get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> mae = results.getDoubleScoreOutput()
                                                                          .get( MetricConstants.MEAN_ABSOLUTE_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> me =
                results.getDoubleScoreOutput().get( MetricConstants.MEAN_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> rmse = results.getDoubleScoreOutput()
                                                                           .get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> ve = results.getDoubleScoreOutput()
                                                                         .get( MetricConstants.VOLUMETRIC_EFFICIENCY );

        //Test contents
        assertTrue( "Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                    bias.getValue( 0 ).getData().equals( 1.6666666666666667 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                    cod.getValue( 0 ).getData().equals( 1.0 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                    rho.getValue( 0 ).getData().equals( 1.0 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                    mae.getValue( 0 ).getData().equals( 5.0 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.MEAN_ERROR,
                    me.getValue( 0 ).getData().equals( 5.0 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                    rmse.getValue( 0 ).getData().equals( 5.0 ) );
        assertTrue( "Unexpected difference in " + MetricConstants.VOLUMETRIC_EFFICIENCY,
                    ve.getValue( 0 ).getData().equals( -0.6666666666666666 ) );
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
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithThresholds() throws IOException, MetricProcessorException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config, MetricOutputGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        final MetadataFactory metFac = dataFac.getMetadataFactory();
        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( i ) );
            final Metadata meta = metFac.getMetadata( metFac.getDimension( "CMS" ),
                                                      metFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ),
                                                      window );
            processor.apply( dataFac.ofSingleValuedPairs( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data            
        processor.getCachedMetricOutput().getDoubleScoreOutput().forEach( ( key, value ) -> {
            if ( key.getKey() == MetricConstants.THREAT_SCORE )
            {
                assertTrue( "Expected ten results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 10 );
            }
            else
            {
                assertTrue( "Expected twenty results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 20 );
            }
        } );

        // Expected result
        MatrixOfDoubles expected = dataFac.matrixOf( new double[][] { { 400.0, 100.0 }, { 0.0, 0.0 } } );
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 1 ) );
        Pair<TimeWindow, OneOrTwoThresholds> key =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 1.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT,
                                                                     metFac.getDimension( "CMS" ) ) ) );
        assertTrue( "Unexpected results for the contingency table.",
                    expected.equals( processor.getCachedMetricOutput()
                                              .getMatrixOutput()
                                              .get( MetricConstants.CONTINGENCY_TABLE )
                                              .get( key )
                                              .getData() ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} for all valid metrics associated
     * with single-valued inputs.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     */

    @Test
    public void testForExpectedMetricsWhenAllValidConfigured() throws IOException, MetricProcessorException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testAllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );

        //Check for the expected number of metrics
        assertTrue( "Unexpected number of metrics.",
                    processor.metrics.size() == MetricInputGroup.SINGLE_VALUED.getMetrics().size()
                                                + MetricInputGroup.DICHOTOMOUS.getMetrics().size() );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to a time-series metric using 
     * pairs obtained from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. Tests the output for 
     * multiple calls with subsets of data, caching the results across calls.
     * 
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesMetrics() throws MetricProcessorException, InterruptedException
    {
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();

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

        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        MetricOutputGroup.set() );

        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        TimeSeriesOfSingleValuedPairs second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> actual =
                processor.getCachedMetricOutput().getPairedOutput();

        //Build the expected output
        List<Pair<Instant, Duration>> expectedFirst = new ArrayList<>();
        List<Pair<Instant, Duration>> expectedSecond = new ArrayList<>();
        expectedFirst.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSecond.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        // Metadata for the output
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1985-01-01T00:00:00Z" ),
                                                ReferenceTime.ISSUE_TIME,
                                                Duration.ofHours( 6 ),
                                                Duration.ofHours( 18 ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( 1,
                                                             metaFac.getDimension( "DURATION" ),
                                                             metaFac.getDimension( "CMS" ),
                                                             MetricConstants.TIME_TO_PEAK_ERROR,
                                                             MetricConstants.MAIN,
                                                             metaFac.getDatasetIdentifier( "A",
                                                                                           "Streamflow" ),
                                                             firstWindow );

        PairedOutput<Instant, Duration> expectedErrorsFirst = dataFac.ofPairedOutput( expectedFirst, m1 );
        PairedOutput<Instant, Duration> expectedErrorsSecond =
                dataFac.ofPairedOutput( expectedSecond, metaFac.getOutputMetadata( m1, secondWindow ) );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, PairedOutput<Instant, Duration>> inMap = new HashMap<>();
        inMap.put( Pair.of( firstWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   expectedErrorsFirst );
        inMap.put( Pair.of( secondWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   expectedErrorsSecond );
        MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> mapped =
                dataFac.ofMetricOutputMapByTimeAndThreshold( inMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<PairedOutput<Instant, Duration>> builder =
                dataFac.ofMetricOutputMultiMapByTimeAndThresholdBuilder();
        builder.put( dataFac.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR ), mapped );
        MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> expected =
                (MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>>) builder.build();

        assertTrue( "Actual output differs from expected output for time-series metrics. ", actual.equals( expected ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to a time-series metric using 
     * pairs obtained from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. The test includes a
     * thershold constraint on the paired data.
     * 
     * @throws InterruptedException if the outputs were interrupted
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesMetricsWithThresholds() throws MetricProcessorException, InterruptedException
    {
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();

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

        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        MetricOutputGroup.set() );

        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsTwo();
        TimeSeriesOfSingleValuedPairs second = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsThree();

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> actual =
                processor.getCachedMetricOutput().getPairedOutput();

        //Build the expected output
        List<Pair<Instant, Duration>> expectedFirst = new ArrayList<>();
        List<Pair<Instant, Duration>> expectedSecond = new ArrayList<>();
        expectedFirst.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSecond.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );

        // Metadata for the output
        TimeWindow firstWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                Instant.parse( "1985-01-01T00:00:00Z" ),
                                                ReferenceTime.ISSUE_TIME,
                                                Duration.ofHours( 6 ),
                                                Duration.ofHours( 18 ) );
        TimeWindow secondWindow = TimeWindow.of( Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( 1,
                                                             metaFac.getDimension( "DURATION" ),
                                                             metaFac.getDimension( "CMS" ),
                                                             MetricConstants.TIME_TO_PEAK_ERROR,
                                                             MetricConstants.MAIN,
                                                             metaFac.getDatasetIdentifier( "A",
                                                                                           "Streamflow" ),
                                                             firstWindow );

        PairedOutput<Instant, Duration> expectedErrorsFirst = dataFac.ofPairedOutput( expectedFirst, m1 );
        PairedOutput<Instant, Duration> expectedErrorsSecond =
                dataFac.ofPairedOutput( expectedSecond, metaFac.getOutputMetadata( m1, secondWindow ) );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, PairedOutput<Instant, Duration>> inMap = new HashMap<>();
        inMap.put( Pair.of( firstWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   expectedErrorsFirst );

        inMap.put( Pair.of( firstWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 5.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   dataFac.ofPairedOutput( Arrays.asList(), metaFac.getOutputMetadata( m1, 0 ) ) );

        inMap.put( Pair.of( secondWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   expectedErrorsSecond );

        inMap.put( Pair.of( secondWindow,
                            OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 5.0 ),
                                                                        Operator.GREATER,
                                                                        ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                   expectedErrorsSecond );

        MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> mapped =
                dataFac.ofMetricOutputMapByTimeAndThreshold( inMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<PairedOutput<Instant, Duration>> builder =
                dataFac.ofMetricOutputMultiMapByTimeAndThresholdBuilder();
        builder.put( dataFac.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR ), mapped );
        MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> expected =
                (MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>>) builder.build();

        assertTrue( "Actual output differs from expected output for time-series metrics. ", actual.equals( expected ) );
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
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesSummaryStats()
            throws IOException, MetricProcessorException, InterruptedException
    {
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        TimeSeriesOfSingleValuedPairs pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();
        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first =
                dataFac.getSlicer().filterByBasisTime( pairs,
                                                       a -> a.equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        TimeSeriesOfSingleValuedPairs second =
                dataFac.getSlicer().filterByBasisTime( pairs,
                                                       a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );

        //Compute the metrics
        processor.apply( first );
        processor.apply( second );

        //Validate the outputs
        //Compare the errors against the benchmark
        MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> actualScores =
                processor.getCachedMetricOutput().getDurationScoreOutput();
        //Build the expected statistics
        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MEDIAN, Duration.ofHours( 3 ) );
        expectedSource.put( MetricConstants.MINIMUM, Duration.ofHours( -6 ) );
        expectedSource.put( MetricConstants.MAXIMUM, Duration.ofHours( 12 ) );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, Duration.ofHours( 9 ) );
        //Metadata
        TimeWindow combinedWindow = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                   Instant.parse( "1985-01-02T00:00:00Z" ),
                                                   ReferenceTime.ISSUE_TIME,
                                                   Duration.ofHours( 6 ),
                                                   Duration.ofHours( 18 ) );
        MetricOutputMetadata scoreMeta = metaFac.getOutputMetadata( 2,
                                                                    metaFac.getDimension( "DURATION" ),
                                                                    metaFac.getDimension( "CMS" ),
                                                                    MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                    null,
                                                                    metaFac.getDatasetIdentifier( "A",
                                                                                                  "Streamflow" ),
                                                                    combinedWindow );
        DurationScoreOutput expectedScoresSource = dataFac.ofDurationScoreOutput( expectedSource, scoreMeta );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, DurationScoreOutput> scoreInMap = new HashMap<>();
        scoreInMap.put( Pair.of( combinedWindow,
                                 OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                        expectedScoresSource );
        MetricOutputMapByTimeAndThreshold<DurationScoreOutput> mappedScores =
                dataFac.ofMetricOutputMapByTimeAndThreshold( scoreInMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<DurationScoreOutput> scoreBuilder =
                dataFac.ofMetricOutputMultiMapByTimeAndThresholdBuilder();
        scoreBuilder.put( dataFac.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ), mappedScores );
        MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> expectedScores =
                (MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput>) scoreBuilder.build();
        assertTrue( "Actual output differs from expected output for time-series metrics. ",
                    actualScores.equals( expectedScores ) );
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
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithThresholdsFromSource()
            throws IOException, MetricOutputAccessException, MetricProcessorException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

        // Define the external thresholds to use
        Map<MetricConstants, Set<Threshold>> canonical = new HashMap<>();

        Set<Threshold> thresholds =
                new HashSet<>( Arrays.asList( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.5 ),
                                                                   Operator.GREATER_EQUAL,
                                                                   ThresholdDataType.LEFT ) ) );

        ThresholdsByMetricBuilder builder = dataFac.ofThresholdsByMetricBuilder();

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
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        thresholdsByMetric,
                                                                        MetricOutputGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        final MetadataFactory metFac = dataFac.getMetadataFactory();

        // Generate results for 20 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( i ) );
            final Metadata meta = metFac.getMetadata( metFac.getDimension( "CMS" ),
                                                      metFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ),
                                                      window );
            processor.apply( dataFac.ofSingleValuedPairs( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data            
        processor.getCachedMetricOutput().getDoubleScoreOutput().forEach( ( key, value ) -> {
            if ( key.getKey() == MetricConstants.THREAT_SCORE )
            {
                assertTrue( "Expected twenty results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 20 );
            }
            else
            {
                assertTrue( "Expected thirty results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 30 );
            }
        } );

        // Expected result
        MatrixOfDoubles expected = dataFac.matrixOf( new double[][] { { 500.0, 0.0 }, { 0.0, 0.0 } } );
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 1 ) );
        Pair<TimeWindow, OneOrTwoThresholds> key =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 0.5 ),
                                                                     Operator.GREATER_EQUAL,
                                                                     ThresholdDataType.LEFT ) ) );
        assertTrue( "Unexpected results for the contingency table.",
                    expected.equals( processor.getCachedMetricOutput()
                                              .getMatrixOutput()
                                              .get( MetricConstants.CONTINGENCY_TABLE )
                                              .get( key )
                                              .getData() ) );
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
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyWithThresholdsAndNoData() throws IOException, MetricProcessorException, InterruptedException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyWithThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config, MetricOutputGroup.set() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsSeven();
        final MetadataFactory metFac = dataFac.getMetadataFactory();
        // Generate results for 10 nominal lead times
        for ( int i = 1; i < 11; i++ )
        {
            final TimeWindow window = TimeWindow.of( Instant.MIN,
                                                     Instant.MAX,
                                                     ReferenceTime.VALID_TIME,
                                                     Duration.ofHours( i ) );
            final Metadata meta = metFac.getMetadata( metFac.getDimension( "CMS" ),
                                                      metFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ),
                                                      window );
            processor.apply( dataFac.ofSingleValuedPairs( pairs.getRawData(), meta ) );
        }

        // Validate a subset of the data            
        processor.getCachedMetricOutput().getDoubleScoreOutput().forEach( ( key, value ) -> {
            if ( key.getKey() == MetricConstants.THREAT_SCORE )
            {
                assertTrue( "Expected ten results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 10 );
            }
            else
            {
                assertTrue( "Expected twenty results for the " + key.getKey()
                            + ": "
                            + value.size(),
                            value.size() == 20 );
            }
        } );

        // Expected result
        MatrixOfDoubles expected = dataFac.matrixOf( new double[][] { { 0.0, 0.0 }, { 0.0, 0.0 } } );
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                         Instant.MAX,
                                                         ReferenceTime.VALID_TIME,
                                                         Duration.ofHours( 1 ) );
        Pair<TimeWindow, OneOrTwoThresholds> key =
                Pair.of( expectedWindow,
                         OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( 1.0 ),
                                                                     Operator.GREATER,
                                                                     ThresholdDataType.LEFT,
                                                                     metFac.getDimension( "CMS" ) ) ) );
        assertTrue( "Unexpected results for the contingency table.",
                    expected.equals( processor.getCachedMetricOutput()
                                              .getMatrixOutput()
                                              .get( MetricConstants.CONTINGENCY_TABLE )
                                              .get( key )
                                              .getData() ) );
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
     * @throws MetricProcessorException if the metric processor could not be built
     * @throws MetricOutputException if the results could not be generated 
     */

    @Test
    public void testApplyTimeSeriesSummaryStatsWithNoData()
            throws IOException, MetricProcessorException, InterruptedException
    {
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/testApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.set() );
        TimeSeriesOfSingleValuedPairs pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        //Compute the metrics
        processor.apply( pairs );

        //Validate the outputs
        MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> actualScores =
                processor.getCachedMetricOutput().getDurationScoreOutput();

        //Build the expected statistics
        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, null );
        expectedSource.put( MetricConstants.MEDIAN, null );
        expectedSource.put( MetricConstants.MINIMUM, null );
        expectedSource.put( MetricConstants.MAXIMUM, null );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, null );

        //Metadata
        TimeWindow combinedWindow = TimeWindow.of( Instant.MIN,
                                                   Instant.MAX );
        MetricOutputMetadata scoreMeta = metaFac.getOutputMetadata( 0,
                                                                    metaFac.getDimension( "DURATION" ),
                                                                    metaFac.getDimension( "CMS" ),
                                                                    MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                    null,
                                                                    metaFac.getDatasetIdentifier( "A",
                                                                                                  "Streamflow" ),
                                                                    combinedWindow );
        DurationScoreOutput expectedScoresSource = dataFac.ofDurationScoreOutput( expectedSource, scoreMeta );
        Map<Pair<TimeWindow, OneOrTwoThresholds>, DurationScoreOutput> scoreInMap = new HashMap<>();
        scoreInMap.put( Pair.of( combinedWindow,
                                 OneOrTwoThresholds.of( dataFac.ofThreshold( dataFac.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                             Operator.GREATER,
                                                                             ThresholdDataType.LEFT_AND_RIGHT ) ) ),
                        expectedScoresSource );
        MetricOutputMapByTimeAndThreshold<DurationScoreOutput> mappedScores =
                dataFac.ofMetricOutputMapByTimeAndThreshold( scoreInMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<DurationScoreOutput> scoreBuilder =
                dataFac.ofMetricOutputMultiMapByTimeAndThresholdBuilder();
        scoreBuilder.put( dataFac.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ), mappedScores );
        MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> expectedScores =
                (MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput>) scoreBuilder.build();

        assertTrue( "Actual output differs from expected output for time-series metrics. ",
                    actualScores.equals( expectedScores ) );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception on receiving null input.
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricProcessorException
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Expected non-null input to the metric processor." );
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( new ProjectConfig( null,
                                                                                           null,
                                                                                           null,
                                                                                           null,
                                                                                           null,
                                                                                           null ),
                                                                        Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( null );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when attempting to compute metrics that require thresholds and no thresholds are configured.
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testApplyThrowsExceptionWhenThresholdMetricIsConfiguredWithoutThresholds()
            throws MetricProcessorException, IOException
    {
        exception.expect( MetricProcessorException.class );
        exception.expectCause( CoreMatchers.isA( MetricConfigException.class ) );

        MetricsConfig metrics =
                new MetricsConfig( null,
                                   Arrays.asList( new MetricConfig( null, null, MetricConfigName.FREQUENCY_BIAS ) ),
                                   null );
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( new ProjectConfig( null,
                                                                                           null,
                                                                                           Arrays.asList( metrics ),
                                                                                           null,
                                                                                           null,
                                                                                           null ),
                                                                        Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );

    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when climatological observations are required but missing.
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testApplyThrowsExceptionWhenClimatologicalObservationsAreMissing()
            throws MetricProcessorException, IOException
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
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );


        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
    }

    /**
     * Tests that the {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} throws an expected
     * exception when time-series metrics are configured and the input pairs are not 
     * {@link TimeSeriesOfSingleValuedPairs}.
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testApplyThrowsExceptionWhenOrdinaryPairsSuppliedForTimeSeriesMetrics()
            throws MetricProcessorException, IOException
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

        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFac )
                             .ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                        Collections.singleton( MetricOutputGroup.DOUBLE_SCORE ) );
        processor.apply( MetricTestDataFactory.getSingleValuedPairsFour() );
    }

    /**
     * Tests that the construction of a {@link MetricProcessorByTimeSingleValuedPair} fails when the configuration
     * contains an ensemble metric. 
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testExceptionOnConstructionWithEnsembleMetric()
            throws MetricProcessorException, IOException
    {
        exception.expect( MetricProcessorException.class );
        exception.expectCause( CoreMatchers.isA( MetricConfigException.class ) );

        // Mock some metrics
        List<MetricConfig> metrics = new ArrayList<>();
        metrics.add( new MetricConfig( null, null, MetricConfigName.CONTINUOUS_RANKED_PROBABILITY_SCORE ) );

        // Mock some thresholds
        List<ThresholdsConfig> thresholds = new ArrayList<>();
        thresholds.add( new ThresholdsConfig( ThresholdType.PROBABILITY,
                                              wres.config.generated.ThresholdDataType.LEFT,
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, null ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.getInstance( dataFac ).ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                                       null );
    }

    /**
     * Tests that the construction of a {@link MetricProcessorByTimeSingleValuedPair} fails when time-series metrics
     * are configured alongside non-time-series metrics.
     * @throws MetricProcessorException if the exceptional behavior could not be tested for unexpected reasons
     */

    @Test
    public void testExceptionOnConstructionWhenMixingTimeSeriesMetricsWithOtherMetrics()
            throws MetricProcessorException, IOException
    {
        exception.expect( MetricProcessorException.class );
        exception.expectCause( CoreMatchers.isA( MetricConfigException.class ) );

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
                                              "0.1,0.2,0.3",
                                              ThresholdOperator.GREATER_THAN ) );

        // Check discrete probability metric
        ProjectConfig mockedConfig =
                new ProjectConfig( null,
                                   null,
                                   Arrays.asList( new MetricsConfig( thresholds, metrics, timeMetrics ) ),
                                   null,
                                   null,
                                   null );

        MetricFactory.getInstance( dataFac ).ofMetricProcessorByTimeSingleValuedPairs( mockedConfig,
                                                                                       null );
    }


}
