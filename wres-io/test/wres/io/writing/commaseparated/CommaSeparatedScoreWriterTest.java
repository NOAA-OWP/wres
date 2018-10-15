package wres.io.writing.commaseparated;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of score outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedScoreWriterTest extends CommaSeparatedWriterTestHelper
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link DoubleScoreStatistic} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeDoubleScores()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "DRRC2";

        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne = TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 1 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..
        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadataA =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        StatisticMetadata fakeMetadataB =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ERROR,
                                      MetricConstants.MAIN );
        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_ABSOLUTE_ERROR,
                                      MetricConstants.MAIN );

        List<DoubleScoreStatistic> fakeOutputs = new ArrayList<>();
        fakeOutputs.add( DoubleScoreStatistic.of( 1.0, fakeMetadataA ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 2.0, fakeMetadataB ) );
        fakeOutputs.add( DoubleScoreStatistic.of( 3.0, fakeMetadataC ) );

        // Fake output wrapper.
        ListOfStatistics<DoubleScoreStatistic> fakeOutputData =
                ListOfStatistics.of( fakeOutputs );

        // Wrap outputs in future
        Future<ListOfStatistics<DoubleScoreStatistic>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addDoubleScoreOutput( outputMapByMetricFuture );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = this.getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreStatistic> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory );
        writer.accept( output.getDoubleScoreStatistics() );

        // Read the file, verify it has what we wanted:
        Path pathToFirstFile = Paths.get( this.outputDirectory.toString(),
                                          "DRRC2_SQIN_HEFS_MEAN_SQUARE_ERROR.csv" );
        List<String> firstResult = Files.readAllLines( pathToFirstFile );

        assertTrue( firstResult.get( 0 ).contains( "," ) );
        assertTrue( firstResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( firstResult.get( 1 )
                               .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                                        + "1.0" ) );
        Path pathToSecondFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                           "DRRC2_SQIN_HEFS_MEAN_ERROR.csv" );
        List<String> secondResult = Files.readAllLines( pathToSecondFile );

        assertTrue( secondResult.get( 0 ).contains( "," ) );
        assertTrue( secondResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( secondResult.get( 1 )
                                .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                                         + "2.0" ) );
        Path pathToThirdFile = Paths.get( System.getProperty( "java.io.tmpdir" ),
                                          "DRRC2_SQIN_HEFS_MEAN_ABSOLUTE_ERROR.csv" );
        List<String> thirdResult = Files.readAllLines( pathToThirdFile );

        assertTrue( thirdResult.get( 0 ).contains( "," ) );
        assertTrue( thirdResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( thirdResult.get( 1 )
                               .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                                        + "3.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFirstFile );
        Files.deleteIfExists( pathToSecondFile );
        Files.deleteIfExists( pathToThirdFile );
    }

    /**
     * Tests the writing of {@link DurationScoreStatistic} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeDurationScores()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "DOLC2";

        // Create fake outputs
        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               ReferenceTime.VALID_TIME,
                               Duration.ofHours( 1 ),
                               Duration.ofHours( 18 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      null );

        Map<MetricConstants, Duration> fakeOutputs = new HashMap<>();
        fakeOutputs.put( MetricConstants.MEAN, Duration.ofHours( 1 ) );
        fakeOutputs.put( MetricConstants.MEDIAN, Duration.ofHours( 2 ) );
        fakeOutputs.put( MetricConstants.MAXIMUM, Duration.ofHours( 3 ) );

        // Fake output wrapper.
        ListOfStatistics<DurationScoreStatistic> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( DurationScoreStatistic.of( fakeOutputs,
                                                                                           fakeMetadata ) ) );

        // wrap outputs in future
        Future<ListOfStatistics<DurationScoreStatistic>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addDurationScoreStatistics( outputMapByMetricFuture );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DurationScoreStatistic> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory );
        writer.accept( output.getDurationScoreStatistics() );

        // read the file, verify it has what we wanted:
        Path pathToFile = Paths.get( this.outputDirectory.toString(),
                                     "DOLC2_SQIN_HEFS_TIME_TO_PEAK_ERROR_STATISTIC.csv" );
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "ERROR" ) );
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,64800,"
                                   + "PT1H,PT2H,PT3H" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Tests the writing of {@link DoubleScoreStatistic} to file where the output is not square (i.e. contains missing
     * data).
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeDoubleScoresWithMissingData()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "FTSC1";

        // Create fake outputs
        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne = TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 1 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadataA =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         thresholdOne ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        List<DoubleScoreStatistic> fakeOutputs =
                Collections.singletonList( DoubleScoreStatistic.of( 1.0, fakeMetadataA ) );

        // Fake output wrapper.
        ListOfStatistics<DoubleScoreStatistic> fakeOutputData =
                ListOfStatistics.of( fakeOutputs );

        // wrap outputs in future
        Future<ListOfStatistics<DoubleScoreStatistic>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addDoubleScoreOutput( outputMapByMetricFuture );

        // Add the data for another threshold at the same time
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 23.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        StatisticMetadata fakeMetadataB =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         thresholdTwo ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        List<DoubleScoreStatistic> fakeOutputsB =
                Collections.singletonList( DoubleScoreStatistic.of( 1.0, fakeMetadataB ) );

        ListOfStatistics<DoubleScoreStatistic> fakeOutputDataB =
                ListOfStatistics.of( fakeOutputsB );

        Future<ListOfStatistics<DoubleScoreStatistic>> outputMapByMetricFutureB =
                CompletableFuture.completedFuture( fakeOutputDataB );

        outputBuilder.addDoubleScoreOutput( outputMapByMetricFutureB );

        // Add data for another time, and one threshold only
        TimeWindow timeTwo = TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 2 ) );

        StatisticMetadata fakeMetadataC =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeTwo,
                                                         thresholdOne ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.MEAN_SQUARE_ERROR,
                                      MetricConstants.MAIN );

        List<DoubleScoreStatistic> fakeOutputsC =
                Collections.singletonList( DoubleScoreStatistic.of( 1.0, fakeMetadataC ) );

        ListOfStatistics<DoubleScoreStatistic> fakeOutputDataC =
                ListOfStatistics.of( fakeOutputsC );

        Future<ListOfStatistics<DoubleScoreStatistic>> outputMapByMetricFutureC =
                CompletableFuture.completedFuture( fakeOutputDataC );

        outputBuilder.addDoubleScoreOutput( outputMapByMetricFutureC );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreStatistic> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory );
        writer.accept( output.getDoubleScoreStatistics() );

        // read the file, verify it has what we wanted:
        Path pathToFirstFile = Paths.get( this.outputDirectory.toString(),
                                          "FTSC1_SQIN_HEFS_MEAN_SQUARE_ERROR.csv" );

        List<String> firstResult = Files.readAllLines( pathToFirstFile );

        assertTrue( firstResult.get( 0 ).equals( "EARLIEST VALID TIME,LATEST VALID TIME,EARLIEST LEAD TIME IN SECONDS,"
                                                 + "LATEST LEAD TIME IN SECONDS,MEAN SQUARE ERROR All data,"
                                                 + "MEAN SQUARE ERROR > 23.0" ) );
        assertTrue( firstResult.get( 1 )
                               .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                        + "3600,3600,1.0,1.0" ) );
        assertTrue( firstResult.get( 2 )
                               .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,7200,"
                                        + "7200,1.0,NA" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFirstFile );
    }


}
