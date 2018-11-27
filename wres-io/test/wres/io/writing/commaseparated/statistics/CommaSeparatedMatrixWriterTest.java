package wres.io.writing.commaseparated.statistics;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of matrix outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedMatrixWriterTest extends CommaSeparatedWriterTestHelper
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link MatrixStatistic} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeMatrixOutput()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "BDAC1";

        // Create fake outputs
        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier...
        // which requires a location...

        Location fakeLocation = Location.of( LID );
        final Location geospatialID = fakeLocation;

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( geospatialID, "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadata = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                                  datasetIdentifier,
                                                                                  timeOne,
                                                                                  threshold ),
                                                               1000,
                                                               MeasurementUnit.of(),
                                                               MetricConstants.CONTINGENCY_TABLE,
                                                               null );

        double[][] fakeOutputs = new double[][] { { 23, 79 }, { 56, 342 } };


        // Fake output wrapper.
        ListOfStatistics<MatrixStatistic> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( MatrixStatistic.of( fakeOutputs,
                                                                                   Arrays.asList( MetricDimension.TRUE_POSITIVES,
                                                                                                  MetricDimension.FALSE_POSITIVES,
                                                                                                  MetricDimension.FALSE_NEGATIVES,
                                                                                                  MetricDimension.TRUE_NEGATIVES ),
                                                                                   fakeMetadata ) ) );
        // wrap outputs in future
        Future<ListOfStatistics<MatrixStatistic>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addMatrixStatistics( outputMapByMetricFuture );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedMatrixWriter.of( projectConfig,
                                       ChronoUnit.SECONDS,
                                       this.outputDirectory )
                                  .accept( output.getMatrixStatistics() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( this.outputDirectory.toString(),
                                     "BDAC1_SQIN_HEFS_CONTINGENCY_TABLE_86400_SECONDS.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "TRUE POSITIVES" ) );
        assertTrue( result.get( 0 ).contains( "FALSE POSITIVES" ) );

        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                                   + "23.0,79.0,56.0,342.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
