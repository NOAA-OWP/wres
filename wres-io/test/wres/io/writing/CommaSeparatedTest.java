package wres.io.writing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

import wres.config.ProjectConfigException;
import wres.config.generated.Conditions;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.Location;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MapBiKey;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByMetric;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.Threshold;

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
    public void writeSimpleFile()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException
    {

        // location id will be filename:
        final String LID = "DRRC2";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByLeadThreshold.MetricOutputForProjectByLeadThresholdBuilder
                outputBuilder =
                outputFactory.ofMetricOutputForProjectByLeadThreshold();

        Integer leadTimeOne = 1;
        Integer leadTimeTwo = 2;

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
                                           MetricConstants.MEAN_SQUARE_ERROR,
                                           MetricConstants.MAIN,
                                           datasetIdentifier );

        List<ScalarOutput> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( outputFactory.ofScalarOutput( 1.0, fakeMetadata ) );
        fakeOutputs.add( outputFactory.ofScalarOutput( 2.0, fakeMetadata ) );
        fakeOutputs.add( outputFactory.ofScalarOutput( 3.0, fakeMetadata ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<ScalarOutput> fakeOutputData =
                outputFactory.ofMap( fakeOutputs );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<ScalarOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        MapBiKey<Integer, Threshold> mapKeyByLeadThreshold =
                outputFactory.getMapKeyByLeadThreshold( leadTimeOne,
                                                        Double.NEGATIVE_INFINITY,
                                                        Threshold.Operator.GREATER );

        outputBuilder.addScalarOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByLeadThreshold output = outputBuilder.build();

        // Construct a fake configuration file.

        // Use the system temp directory so that checks for writeability pass.
        DestinationConfig destinationConfig =
                new DestinationConfig( System.getProperty( "java.io.tmpdir" ),
                                       null,
                                       DestinationType.NUMERIC,
                                       null );

        List<DestinationConfig> destinations = new ArrayList<>();
        destinations.add( destinationConfig );

        ProjectConfig.Outputs outputsConfig =
                new ProjectConfig.Outputs( null,
                                           null,
                                           null,
                                           destinations );

        Location location = new Location( LID,
                                          null,
                                          null,
                                          null,
                                          null );

        Feature feature = new Feature( null,
                                       null,
                                       location,
                                       null,
                                       null,
                                       null );

        List<Feature> features = new ArrayList<>();
        features.add( feature );
        Conditions conditions = new Conditions( null,
                                                null,
                                                null,
                                                features,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( null,
                                                         conditions,
                                                         null,
                                                         outputsConfig,
                                                         null,
                                                         null,
                                                         "test");

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparated.writeOutputFiles( projectConfig, feature, output );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     LID + ".csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get(0).contains( "," ) );
        assertTrue( result.get(0).contains( "ERROR" ) );
        assertTrue( result.get(1).equals( "1,3.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }
}
