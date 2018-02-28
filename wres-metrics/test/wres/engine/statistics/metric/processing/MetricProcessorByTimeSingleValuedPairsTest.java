package wres.engine.statistics.metric.processing;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DefaultMetadataFactory;
import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
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
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold.MetricOutputMultiMapByTimeAndThresholdBuilder;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorByTimeSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorByTimeSingleValuedPairsTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/test1ApplyNoThresholds.xml and pairs 
     * obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricOutputAccessException if the outputs could not be accessed
     * @throws MetricProcessorException if the metric processor could not be built 
     */

    @Test
    public void test1ApplyNoThresholds() throws IOException, MetricOutputAccessException, MetricProcessorException
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/test1ApplyNoThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( dataFactory )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        null,
                                                                        Executors.newSingleThreadExecutor(),
                                                                        Executors.newSingleThreadExecutor(),
                                                                        (MetricOutputGroup[]) null );
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
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/test1ApplyNoThresholds.xml and 
     * pairs obtained from {@link MetricTestDataFactory#getSingleValuedPairsFour()}. Tests the output for multiple 
     * calls with separate forecast lead times.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricOutputAccessException if the outputs could not be accessed
     * @throws MetricProcessorException if the metric processor could not be built
     */

    @Test
    public void test2ApplyThresholds() throws IOException, MetricOutputAccessException, MetricProcessorException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/test2ApplyThresholds.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );
        SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
        final MetadataFactory metFac = metIn.getMetadataFactory();
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
            processor.apply( metIn.ofSingleValuedPairs( pairs.getData(), meta ) );
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
        MatrixOfDoubles expected = metIn.matrixOf( new double[][] { { 400.0, 100.0 }, { 0.0, 0.0 } } );
        final TimeWindow expectedWindow = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX,
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 1 ) );
        Pair<TimeWindow,Threshold> key = Pair.of( expectedWindow, metIn.ofThreshold( 1.0, Operator.GREATER ) );
        assertTrue( "Unexpected results for the contingency table.",
                    expected.equals( processor.getCachedMetricOutput()
                                              .getMatrixOutput()
                                              .get( MetricConstants.CONTINGENCY_TABLE )
                                              .get( key )
                                              .getData() ) );       
    }

    /**
     * Tests for exceptions associated with a {@link MetricProcessorByTimeSingleValuedPairs}.
     * @throws MetricProcessorException if the metric processor could not be constructed
     * @throws IOException if a project could not be read
     */

    @Test
    public void test3Exceptions() throws MetricProcessorException, IOException
    {

        //Check for null input
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String testOne = "testinput/metricProcessorSingleValuedPairsByTimeTest/test3ExceptionsOne.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testOne ) ).getProjectConfig();
            MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            MetricOutputGroup.DOUBLE_SCORE );
            processor.apply( null );
            fail( "Expected a checked exception on processing the project configuration '" + testOne + "'." );
        }
        catch ( NullPointerException e )
        {
        }
        //Check for absence of thresholds on metrics that require them
        String testTwo = "testinput/metricProcessorSingleValuedPairsByTimeTest/test3ExceptionsFour.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testTwo ) ).getProjectConfig();
            MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            MetricOutputGroup.DOUBLE_SCORE );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testTwo
                  + "' "
                  + "with no thresholds for metrics that require them." );
        }
        catch ( MetricProcessorException e )
        {
        }
        //Checked for value thresholds that do not apply to left
        String testThree = "testinput/metricProcessorSingleValuedPairsByTimeTest/test3ExceptionsFive.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testThree ) ).getProjectConfig();
            MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            MetricOutputGroup.DOUBLE_SCORE );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testThree
                  + "' "
                  + "with value thresholds that do not apply to left." );
        }
        catch ( MetricProcessorException e )
        {
        }
        //Checked for probability thresholds that do not apply to left
        String testFour = "testinput/metricProcessorSingleValuedPairsByTimeTest/test3ExceptionsSix.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testFour ) ).getProjectConfig();
            MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            MetricOutputGroup.DOUBLE_SCORE );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testFour
                  + "' "
                  + "with probability thresholds that do not apply to left." );
        }
        catch ( MetricProcessorException e )
        {
        }
        //Check for insufficient data to compute climatological probability thresholds
        String testFive = "testinput/metricProcessorSingleValuedPairsByTimeTest/test3ExceptionsEight.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from( Paths.get( testFive ) ).getProjectConfig();
            MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                    MetricFactory.getInstance( metIn )
                                 .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                            MetricOutputGroup.DOUBLE_SCORE );
            processor.apply( MetricTestDataFactory.getSingleValuedPairsSix() );
            fail( "Expected a checked exception on processing the project configuration '" + testFive
                  + "' "
                  + "with metric-local thresholds that are not supported." );
        }
        catch ( MetricCalculationException e )
        {
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} for all valid metrics associated
     * with single-valued inputs.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricProcessorException if the metric processor could not be built
     */

    @Test
    public void test4AllValid() throws IOException, MetricProcessorException
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/test4AllValid.xml";
        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );
        //Check for the expected number of metrics
        assertTrue( "Unexpected number of metrics.",
                    processor.metrics.size() == MetricInputGroup.SINGLE_VALUED.getMetrics().size()
                                                + MetricInputGroup.DICHOTOMOUS.getMetrics().size() );
    }


    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from testinput/metricProcessorSingleValuedPairsByTimeTest/test5ApplyTimeSeriesMetrics.xml and 
     * pairs obtained from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. Tests the output for 
     * multiple calls with subsets of data, caching the results across calls.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricOutputAccessException if the outputs could not be accessed
     * @throws MetricProcessorException if the metric processor could not be built
     */

    @Test
    public void test5ApplyTimeSeriesMetrics() throws IOException, MetricOutputAccessException, MetricProcessorException
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/test5ApplyTimeSeriesMetrics.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );
        TimeSeriesOfSingleValuedPairs pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();
        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first =
                (TimeSeriesOfSingleValuedPairs) pairs.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        TimeSeriesOfSingleValuedPairs second =
                (TimeSeriesOfSingleValuedPairs) pairs.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );

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
        PairedOutput<Instant, Duration> expectedErrorsFirst = metIn.ofPairedOutput( expectedFirst, m1 );
        PairedOutput<Instant, Duration> expectedErrorsSecond =
                metIn.ofPairedOutput( expectedSecond, metaFac.getOutputMetadata( m1, secondWindow ) );
        Map<Pair<TimeWindow, Threshold>, PairedOutput<Instant, Duration>> inMap = new HashMap<>();
        inMap.put( Pair.of( firstWindow, metIn.ofThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) ),
                   expectedErrorsFirst );
        inMap.put( Pair.of( secondWindow, metIn.ofThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) ),
                   expectedErrorsSecond );
        MetricOutputMapByTimeAndThreshold<PairedOutput<Instant, Duration>> mapped = metIn.ofMap( inMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<PairedOutput<Instant, Duration>> builder = metIn.ofMultiMap();
        builder.put( metIn.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR ), mapped );
        MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>> expected =
                (MetricOutputMultiMapByTimeAndThreshold<PairedOutput<Instant, Duration>>) builder.build();
        assertTrue( "Actual output differs from expected output for time-series metrics. ", actual.equals( expected ) );
    }

    /**
     * Tests the construction of a {@link MetricProcessorByTimeSingleValuedPairs} and application of
     * {@link MetricProcessorByTimeSingleValuedPairs#apply(SingleValuedPairs)} to 
     * configuration obtained from 
     * testinput/metricProcessorSingleValuedPairsByTimeTest/test6ApplyTimeSeriesSummaryStats.xml and pairs obtained 
     * from {@link MetricTestDataFactory#getTimeSeriesOfSingleValuedPairsOne()}. Tests the summary statistics generated 
     * at the end of multiple calls with subsets of t ime-series data.
     * 
     * @throws IOException if the input data could not be read
     * @throws MetricOutputAccessException if the outputs could not be accessed
     * @throws MetricProcessorException if the metric processor could not be built
     */

    @Test
    public void test6ApplyTimeSeriesSummaryStats() throws IOException, MetricOutputAccessException, MetricProcessorException
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = DefaultMetadataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsByTimeTest/test6ApplyTimeSeriesSummaryStats.xml";

        ProjectConfig config = ProjectConfigPlus.from( Paths.get( configPath ) ).getProjectConfig();
        MetricProcessor<SingleValuedPairs, MetricOutputForProjectByTimeAndThreshold> processor =
                MetricFactory.getInstance( metIn )
                             .ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                        MetricOutputGroup.values() );
        TimeSeriesOfSingleValuedPairs pairs = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();
        //Break into two time-series to test sequential calls
        TimeSeriesOfSingleValuedPairs first =
                (TimeSeriesOfSingleValuedPairs) pairs.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-01T00:00:00Z" ) ) );
        TimeSeriesOfSingleValuedPairs second =
                (TimeSeriesOfSingleValuedPairs) pairs.filterByBasisTime( a -> a.equals( Instant.parse( "1985-01-02T00:00:00Z" ) ) );

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
        DurationScoreOutput expectedScoresSource = metIn.ofDurationScoreOutput( expectedSource, scoreMeta );
        Map<Pair<TimeWindow, Threshold>, DurationScoreOutput> scoreInMap = new HashMap<>();
        scoreInMap.put( Pair.of( combinedWindow, metIn.ofThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) ),
                        expectedScoresSource );
        MetricOutputMapByTimeAndThreshold<DurationScoreOutput> mappedScores = metIn.ofMap( scoreInMap );
        MetricOutputMultiMapByTimeAndThresholdBuilder<DurationScoreOutput> scoreBuilder = metIn.ofMultiMap();
        scoreBuilder.put( metIn.getMapKey( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ), mappedScores );
        MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput> expectedScores =
                (MetricOutputMultiMapByTimeAndThreshold<DurationScoreOutput>) scoreBuilder.build();
        assertTrue( "Actual output differs from expected output for time-series metrics. ",
                    actualScores.equals( expectedScores ) );
    }    
    
}
