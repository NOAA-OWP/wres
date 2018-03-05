package wres.io.writing.commaseparated;

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
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.Threshold;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMapByMetric;

/**
 * Tests the writing of score outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedScoreWriterTest extends CommaSeparatedWriterTest
{
    /**
     * Tests the writing of {@link DoubleScoreOutput} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     * @throws ExecutionException if the execution fails
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writeDoubleScores()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "DRRC2";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder outputBuilder =
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
                outputFactory.ofMapKeyByTimeThreshold( timeOne,
                                                       Double.NEGATIVE_INFINITY,
                                                       Threshold.Operator.GREATER );

        outputBuilder.addDoubleScoreOutput( mapKeyByLeadThreshold,
                                            outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreOutput> writer = CommaSeparatedScoreWriter.of( projectConfig ); 
        writer.accept( output.getDoubleScoreOutput() ); 

        // read the file, verify it has what we wanted:
        Path pathToFirstFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                          "DRRC2_SQIN_HEFS_MEAN_SQUARE_ERROR.csv" );
        List<String> firstResult = Files.readAllLines( pathToFirstFile );

        assertTrue( firstResult.get( 0 ).contains( "," ) );
        assertTrue( firstResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( firstResult.get( 1 )
                               .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,1,"
                                        + "1.0" ) );
        Path pathToSecondFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                           "DRRC2_SQIN_HEFS_MEAN_ERROR.csv" );
        List<String> secondResult = Files.readAllLines( pathToSecondFile );

        assertTrue( secondResult.get( 0 ).contains( "," ) );
        assertTrue( secondResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( secondResult.get( 1 )
                                .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,1,"
                                         + "2.0" ) );
        Path pathToThirdFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                          "DRRC2_SQIN_HEFS_MEAN_ABSOLUTE_ERROR.csv" );
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
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    @Test
    public void writeDurationScores()
            throws ProjectConfigException, IOException, InterruptedException,
            ExecutionException, MetricOutputAccessException
    {

        // location id
        final String LID = "DOLC2";

        // Create fake outputs

        DataFactory outputFactory = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outputFactory.getMetadataFactory();

        MetricOutputForProjectByTimeAndThreshold.MetricOutputForProjectByTimeAndThresholdBuilder outputBuilder =
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

        Map<MetricConstants, Duration> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricConstants.MEAN, Duration.ofHours( 1 ) );
        fakeOutputs.put( MetricConstants.MEDIAN, Duration.ofHours( 2 ) );
        fakeOutputs.put( MetricConstants.MAXIMUM, Duration.ofHours( 3 ) );

        // Fake output wrapper.
        MetricOutputMapByMetric<DurationScoreOutput> fakeOutputData =
                outputFactory.ofMap( Arrays.asList( outputFactory.ofDurationScoreOutput( fakeOutputs,
                                                                                         fakeMetadata ) ) );

        // wrap outputs in future
        Future<MetricOutputMapByMetric<DurationScoreOutput>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        // Fake lead time and threshold
        Pair<TimeWindow, Threshold> mapKeyByLeadThreshold =
                outputFactory.ofMapKeyByTimeThreshold( timeOne,
                                                       Double.NEGATIVE_INFINITY,
                                                       Threshold.Operator.GREATER );

        outputBuilder.addDurationScoreOutput( mapKeyByLeadThreshold,
                                              outputMapByMetricFuture );

        MetricOutputForProjectByTimeAndThreshold output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DurationScoreOutput> writer = CommaSeparatedScoreWriter.of( projectConfig ); 
        writer.accept( output.getDurationScoreOutput() ); 

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                     "DOLC2_SQIN_HEFS_TIME_TO_PEAK_ERROR_STATISTIC.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "ERROR" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,1,18,"
                                   + "PT1H,PT2H,PT3H" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
