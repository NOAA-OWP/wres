package wres.io.writing;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.Threshold;
import wres.datamodel.Threshold.Operator;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

// uncomment these if we figure out what was wrong with powermockito setup
//@RunWith( PowerMockRunner.class )
//@PrepareForTest( { Files.class, BufferedWriter.class, Writer.class } )
//@PowerMockIgnore( "javax.management.*" )
public class CommaSeparatedTest
{

// what follows is a started attempt at avoiding filesystem (use powermockito)
//
//    StringWriter outputString;
//    BufferedWriter bufferedWriter;
//
//    @Before
//    public void setup() throws Exception //yuck, but that's sig of whenNew
//    {
//        // Fake out the file that is written, write to String instead
//        outputString = new StringWriter();
//        bufferedWriter = new BufferedWriter( outputString );
//
//        PowerMockito.mockStatic( Files.class );
//        //PowerMockito.mockStatic( BufferedWriter.class );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withAnyArguments()
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( Writer.class )
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( any() )
//                    .thenReturn( bufferedWriter );
//
//        // does not work:
//        PowerMockito.whenNew( BufferedWriter.class )
//                    .withArguments( any(Writer.class), anyInt() )
//                    .thenReturn( bufferedWriter );
//
//    }

    @Test
    public void writeDoubleScores()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {
        
        // location id
        final String LID = "DRRC2";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne = TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 1 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadataA =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.MEAN_SQUARE_ERROR,
                                           MetricConstants.MAIN,
                                           datasetIdentifier );
        MetricOutputMetadata fakeMetadataB =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.MEAN_ERROR,
                                           MetricConstants.MAIN,
                                           datasetIdentifier );
        MetricOutputMetadata fakeMetadataC =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricConstants.MAIN,
                                           datasetIdentifier );
        
        List<DoubleScoreOutput> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( outputFactory.ofDoubleScoreOutput( 1.0, fakeMetadataA ) );
        fakeOutputs.add( outputFactory.ofDoubleScoreOutput( 2.0, fakeMetadataB ) );
        fakeOutputs.add( outputFactory.ofDoubleScoreOutput( 3.0, fakeMetadataC ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<DoubleScoreOutput> fakeOutputData =
                outputFactory.ofMap( fakeOutputs );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<DoubleScoreOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                outputFactory.getMapKeyByTimeThreshold( timeOne,
                                                        Double.NEGATIVE_INFINITY,
                                                        Threshold.Operator.GREATER );

        outputBuilder.addDoubleScoreOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFirstFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "DRRC2_MEAN_SQUARE_ERROR_SQIN.csv" );
        List<String> firstResult = Files.readAllLines( pathToFirstFile );

        assertTrue( firstResult.get(0).contains( "," ) );
        assertTrue( firstResult.get(0).contains( "ERROR" ) );
        assertTrue( firstResult.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,1,"
                                  + "1.0" ) );
        Path pathToSecondFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                          "DRRC2_MEAN_ERROR_SQIN.csv" );
        List<String> secondResult = Files.readAllLines( pathToSecondFile );

        assertTrue( secondResult.get( 0 ).contains( "," ) );
        assertTrue( secondResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( secondResult.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,1,"
                                   + "2.0" ) );
        Path pathToThirdFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                           "DRRC2_MEAN_ABSOLUTE_ERROR_SQIN.csv" );
        List<String> thirdResult = Files.readAllLines( pathToThirdFile );

        assertTrue( thirdResult.get( 0 ).contains( "," ) );
        assertTrue( thirdResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( thirdResult.get( 1 )
                                .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,1,"
                                         + "3.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFirstFile );
        Files.deleteIfExists( pathToSecondFile );
        Files.deleteIfExists( pathToThirdFile );
    }
    
    /**
     * Tests the writing of {@link DurationScoreOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     */
    
    @Test
    public void writeDurationScores()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {

        // location id
        final String LID = "DOLC2";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                           null,
                                           datasetIdentifier );
        
        Map<MetricConstants,Duration> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricConstants.MEAN, Duration.ofHours( 1 ) );
        fakeOutputs.put( MetricConstants.MEDIAN, Duration.ofHours( 2 ) );
        fakeOutputs.put( MetricConstants.MAXIMUM, Duration.ofHours( 3 ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<DurationScoreOutput> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofDurationScoreOutput( fakeOutputs, fakeMetadata ) ) );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<DurationScoreOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                outputFactory.getMapKeyByTimeThreshold( timeOne,
                                                        Double.NEGATIVE_INFINITY,
                                                        Threshold.Operator.GREATER );

        outputBuilder.addDurationScoreOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "DOLC2_TIME_TO_PEAK_ERROR_STATISTIC_SQIN.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get(0).contains( "," ) );
        assertTrue( result.get(0).contains( "ERROR" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                  + "PT1H,PT2H,PT3H" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }    
    
    /**
     * Tests the writing of {@link PairedOutput} to file, where the left pair comprises an {@link Instant} and the
     * right pair comprises an (@link Duration).
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     */
    
    @Test
    public void writePairedOutputForTimeSeriesMetrics()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {

        // location id
        final String LID = "FTSC1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.TIME_TO_PEAK_ERROR,
                                           null,
                                           datasetIdentifier );
        
        List<Pair<Instant,Duration>> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( 1 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 2 ) ) );
        fakeOutputs.add( Pair.of( Instant.parse( "1985-01-03T00:00:00Z" ), Duration.ofHours( 3 ) ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<PairedOutput<Instant,Duration>> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofPairedOutput( fakeOutputs, fakeMetadata ) ) );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<PairedOutput<Instant,Duration>>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                outputFactory.getMapKeyByTimeThreshold( timeOne,
                                                        Double.NEGATIVE_INFINITY,
                                                        Threshold.Operator.GREATER );

        outputBuilder.addPairedOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "FTSC1_TIME_TO_PEAK_ERROR_SQIN.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get(0).contains( "," ) );
        assertTrue( result.get(0).contains( "ERROR" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                  + "1985-01-01T00:00:00Z,PT1H" ) );
        assertTrue( result.get( 2 )
                    .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                            + "1985-01-02T00:00:00Z,PT2H" ) );
        assertTrue( result.get( 3 )
                    .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                            + "1985-01-03T00:00:00Z,PT3H" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }     
    
    /**
     * Tests the writing of {@link MultiVectorOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     */
    
    @Test
    public void writeDiagramOutput()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {

        // location id
        final String LID = "CREC1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.RELIABILITY_DIAGRAM,
                                           null,
                                           datasetIdentifier );

        Map<MetricDimension, double[]> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricDimension.FORECAST_PROBABILITY,
                         new double[] { 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 } );
        fakeOutputs.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                         new double[] { 0.06294, 0.2938, 0.5, 0.73538, 0.93937 } );
        fakeOutputs.put( MetricDimension.SAMPLE_SIZE, new double[] { 5926, 371, 540, 650, 1501 } );

        // Fake output wrapper.
        MetricOutputMapByMetric<MultiVectorOutput> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofMultiVectorOutput( fakeOutputs, fakeMetadata ) ) );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<MultiVectorOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );


        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                Pair.of( timeOne, outputFactory.getQuantileThreshold( 11.94128,
                                                                      0.9,
                                                                      Operator.GREATER_EQUAL ) );

        outputBuilder.addMultiVectorOutput( mapKeyByLeadThreshold,
                                            outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "CREC1_RELIABILITY_DIAGRAM_SQIN_24.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "FORECAST PROBABILITY" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "0.08625,0.06294,5926.0" ) );
        assertTrue( result.get( 2 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "0.2955,0.2938,371.0" ) );
        assertTrue( result.get( 3 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "0.50723,0.5,540.0" ) );
        assertTrue( result.get( 4 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "0.70648,0.73538,650.0" ) );
        assertTrue( result.get( 5 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "0.92682,0.93937,1501.0" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }
    
    /**
     * Tests the writing of {@link BoxPlotOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     */
    
    @Test
    public void writeBoxPlotOutput()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {

        // location id
        final String LID = "JUNP1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                metaFac.getDatasetIdentifier( LID,
                                              "SQIN",
                                              "HEFS",
                                              "ESP" );

        MetricOutputMetadata fakeMetadata =
                metaFac.getOutputMetadata( 1000,
                                           metaFac.getDimension(),
                                           metaFac.getDimension( "CMS" ),
                                           MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                           null,
                                           datasetIdentifier );

        List<PairOfDoubleAndVectorOfDoubles> fakeOutputs = new ArrayList<>();
        VectorOfDoubles probs = outputFactory.vectorOf( new double[] { 0, 0.25, 0.5, 0.75, 1.0 } );

        fakeOutputs.add( outputFactory.pairOf( 1, new double[] { 2, 3, 4, 5, 6 } ) );
        fakeOutputs.add( outputFactory.pairOf( 3, new double[] { 7, 9, 11, 13, 15 } ) );
        fakeOutputs.add( outputFactory.pairOf( 5, new double[] { 21, 24, 27, 30, 33 } ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<BoxPlotOutput> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofBoxPlotOutput( fakeOutputs,
                                                                                   probs,
                                                                                   fakeMetadata,
                                                                                   MetricDimension.OBSERVED_VALUE,
                                                                                   MetricDimension.FORECAST_ERROR ) ) );
        // wrap outputs in future
        Future<MetricOutputMapByMetric<BoxPlotOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );


        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                Pair.of( timeOne, outputFactory.getThreshold( Double.NEGATIVE_INFINITY, Operator.GREATER ) );

        outputBuilder.addBoxPlotOutput( mapKeyByLeadThreshold,
                                            outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "JUNP1_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_SQIN_24.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "OBSERVED VALUE" ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );
        
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "1.0,2.0,3.0,4.0,5.0,6.0" ) );
        assertTrue( result.get( 2 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "3.0,7.0,9.0,11.0,13.0,15.0" ) );
        assertTrue( result.get( 3 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "5.0,21.0,24.0,27.0,30.0,33.0" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }       
    
    /**
     * Returns a fake project configuration for a specified feature.
     * 
     * @param feature the feature
     * @return fake project configuration
     */
    
    private ProjectConfig getMockedProjectConfig( Feature feature )
    {
        // Use the system temp directory so that checks for writeability pass.
        DestinationConfig destinationConfig =
                new DestinationConfig( System.getProperty( "java.io.tmpdir" ),
                                       null,
                                       null,
                                       DestinationType.NUMERIC,
                                       null );

        List<DestinationConfig> destinations = new ArrayList<>();
        destinations.add( destinationConfig );

        ProjectConfig.Outputs outputsConfig =
                new ProjectConfig.Outputs( destinations );

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
    
    private Feature getMockedFeature( String locationId )
    {
        return new Feature( null,
                            null,
                            null,
                            locationId,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null );
    }
    
}
