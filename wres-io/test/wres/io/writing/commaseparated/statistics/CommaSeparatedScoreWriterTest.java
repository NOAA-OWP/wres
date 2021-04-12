package wres.io.writing.commaseparated.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter.DurationScoreComponentOuter;
import wres.io.writing.WriterTestHelper;

/**
 * Tests the writing of score outputs to a file of Comma Separated Values (CSV).
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedScoreWriterTest
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link DoubleScoreStatisticOuter} to file.
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
        String LID = "DRRC2";

        // Construct a fake configuration file.
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreComponentOuter, DoubleScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory,
                                              next -> Double.toString( next.getData().getValue() ) );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getScoreStatisticsForOnePool() );

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 3 );

        Optional<Path> pathToFirstFile =
                pathsToFile.stream()
                           .filter( next -> next.endsWith( "DRRC2_DRRC2_HEFS_MEAN_ABSOLUTE_ERROR.csv" ) )
                           .findAny();

        // Check the expected path: #61841
        assertTrue( pathToFirstFile.isPresent() );

        List<String> firstResult = Files.readAllLines( pathToFirstFile.get() );

        assertTrue( firstResult.get( 0 ).contains( "," ) );
        assertTrue( firstResult.get( 0 ).contains( "ERROR" ) );
        assertEquals( "DRRC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                      + "3.0",
                      firstResult.get( 1 ) );

        Optional<Path> pathToSecondFile =
                pathsToFile.stream()
                           .filter( next -> next.endsWith( "DRRC2_DRRC2_HEFS_MEAN_ERROR.csv" ) )
                           .findAny();

        // Check the expected path: #61841
        assertTrue( pathToSecondFile.isPresent() );

        List<String> secondResult = Files.readAllLines( pathToSecondFile.get() );

        assertTrue( secondResult.get( 0 ).contains( "," ) );
        assertTrue( secondResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( secondResult.get( 1 )
                                .equals( "DRRC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                         + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                                         + "2.0" ) );

        Optional<Path> pathToThirdFile =
                pathsToFile.stream()
                           .filter( next -> next.endsWith( "DRRC2_DRRC2_HEFS_MEAN_SQUARE_ERROR.csv" ) )
                           .findAny();

        // Check the expected path: #61841
        assertTrue( pathToThirdFile.isPresent() );

        List<String> thirdResult = Files.readAllLines( pathToThirdFile.get() );

        assertTrue( thirdResult.get( 0 ).contains( "," ) );
        assertTrue( thirdResult.get( 0 ).contains( "ERROR" ) );
        assertTrue( thirdResult.get( 1 )
                               .equals( "DRRC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                                        + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                                        + "1.0" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToThirdFile.get() );
        Files.deleteIfExists( pathToSecondFile.get() );
        Files.deleteIfExists( pathToFirstFile.get() );
    }

    /**
     * Tests the writing of {@link DurationScoreStatisticOuter} to file.
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
        String LID = "DOLC2";

        // Construct a fake configuration file.
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DurationScoreComponentOuter, DurationScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory,
                                              next -> MessageFactory.parse( next.getData().getValue() ).toString() );
        
        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getDurationScoreStatisticsForOnePool() );

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertEquals( "DOLC2_DOLC2_HEFS_TIME_TO_PEAK_ERROR_STATISTIC.csv", pathToFile.toFile().getName() );
        
        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "ERROR" ) );
        assertEquals( "DOLC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,64800,"
                      + "PT1H,PT2H,PT3H",
                      result.get( 1 ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Tests the writing of {@link DoubleScoreStatisticOuter} to file where the output is not square (i.e. contains missing
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
        String LID = "FTSC1";

        // Construct a fake configuration file.
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreComponentOuter, DoubleScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( projectConfig,
                                              ChronoUnit.SECONDS,
                                              this.outputDirectory,
                                              next -> Double.toString( next.getData().getValue() ) );
        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getScoreStatisticsForThreePoolsWithMissings() );

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertEquals( "FTSC1_FTSC1_HEFS_MEAN_SQUARE_ERROR.csv", pathToFile.toFile().getName() );

        List<String> firstResult = Files.readAllLines( pathToFile );

        assertEquals( "FEATURE DESCRIPTION,EARLIEST ISSUE TIME,LATEST ISSUE TIME,"
                      + "EARLIEST VALID TIME,LATEST VALID TIME,EARLIEST LEAD TIME IN SECONDS,"
                      + "LATEST LEAD TIME IN SECONDS,MEAN SQUARE ERROR All data,"
                      + "MEAN SQUARE ERROR > 23.0",
                      firstResult.get( 0 ) );
        assertEquals( "FTSC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "3600,3600,1.0,1.0",
                      firstResult.get( 1 ) );
        assertEquals( "FTSC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,7200,"
                      + "7200,1.0,NA",
                      firstResult.get( 2 ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }


}
