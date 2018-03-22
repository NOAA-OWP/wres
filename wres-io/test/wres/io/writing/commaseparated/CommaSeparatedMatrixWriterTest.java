package wres.io.writing.commaseparated;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.OneOrTwoThresholds;
import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;

/**
 * Tests the writing of matrix outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedMatrixWriterTest extends CommaSeparatedWriterTest
{

    /**
     * Tests the writing of {@link MatrixOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writeMatrixOutput()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "BDAC1";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder outputBuilder =
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
                                           MetricConstants.CONTINGENCY_TABLE,
                                           null,
                                           datasetIdentifier );

        double[][] fakeOutputs = new double[][] { { 23, 79 }, { 56, 342 } };


        // Fake output wrapper.
        MetricOutputMapByMetric<MatrixOutput> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofMatrixOutput( fakeOutputs,
                                                                                  Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                                                 MetricDimension.FALSE_POSITIVES,
                                                                                                 MetricDimension.FALSE_NEGATIVES,
                                                                                                 MetricDimension.TRUE_NEGATIVES ),
                                                                                  fakeMetadata ) ) );
        // wrap outputs in future
        Future<MetricOutputMapByMetric<MatrixOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );


        // Fake lead time and threshold
        Pair<TimeWindow, OneOrTwoThresholds> mapKeyByLeadThreshold =
                Pair.of( timeOne,
                         OneOrTwoThresholds.of( outputFactory.ofThreshold( outputFactory.ofOneOrTwoDoubles( Double.NEGATIVE_INFINITY ),
                                                                   Operator.GREATER ) ) );

        outputBuilder.addMatrixOutput( mapKeyByLeadThreshold,
                                       outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedMatrixWriter.of( projectConfig ).accept( output.getMatrixOutput() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "BDAC1_SQIN_HEFS_CONTINGENCY_TABLE_24_HOUR.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "TRUE POSITIVES" ) );
        assertTrue( result.get( 0 ).contains( "FALSE POSITIVES" ) );

        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,24,24,"
                                   + "23.0,79.0,56.0,342.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
