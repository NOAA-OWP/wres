package wres.io.writing.commaseparated;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MetricOutputAccessException;
import wres.datamodel.statistics.MetricOutputForProject;
import wres.datamodel.statistics.MultiVectorOutput;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of diagram outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedDiagramOutputTest extends CommaSeparatedWriterTestHelper
{

    /**
     * Tests the writing of {@link MultiVectorOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writeDiagramOutput()
            throws IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "CREC1";

        // Create fake outputs
        MetricOutputForProject.MetricOutputForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
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
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        MetricOutputMetadata fakeMetadata =
                MetricOutputMetadata.of( 1000,
                                         MeasurementUnit.of(),
                                         MeasurementUnit.of( "CMS" ),
                                         MetricConstants.RELIABILITY_DIAGRAM,
                                         null,
                                         datasetIdentifier,
                                         timeOne,
                                         threshold,
                                         null  );

        Map<MetricDimension, double[]> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricDimension.FORECAST_PROBABILITY,
                         new double[] { 0.08625, 0.2955, 0.50723, 0.70648, 0.92682 } );
        fakeOutputs.put( MetricDimension.OBSERVED_RELATIVE_FREQUENCY,
                         new double[] { 0.06294, 0.2938, 0.5, 0.73538, 0.93937 } );
        fakeOutputs.put( MetricDimension.SAMPLE_SIZE, new double[] { 5926, 371, 540, 650, 1501 } );

        // Fake output wrapper.
        ListOfMetricOutput<MultiVectorOutput> fakeOutputData =
                ListOfMetricOutput.of( Collections.singletonList( MultiVectorOutput.ofMultiVectorOutput( fakeOutputs,
                                                                                                         fakeMetadata ) ) );

        // wrap outputs in future
        Future<ListOfMetricOutput<MultiVectorOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addMultiVectorOutput( outputMapByMetricFuture );

        MetricOutputForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedDiagramWriter.of( projectConfig ).accept( output.getMultiVectorOutput() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "CREC1_SQIN_HEFS_RELIABILITY_DIAGRAM_24_HOUR.csv" );
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

}
