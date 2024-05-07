package wres.writing.csv.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.Test;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter.DurationScoreComponentOuter;
import wres.writing.WriterTestHelper;
import wres.statistics.MessageFactory;

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
     * @throws IOException if the output could not be written
     */

    @Test
    public void writeDoubleScores() throws IOException
    {
        EvaluationDeclaration declaration =
                WriterTestHelper.getMockedDeclaration( new DecimalFormat( "0.000000" ) );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreComponentOuter, DoubleScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( declaration,
                                              this.outputDirectory,
                                              next -> Double.toString( next.getStatistic().getValue() ) );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getScoreStatisticsForOnePool() );

        // Check the expected number of paths: #61841
        assertEquals( 3, pathsToFile.size() );

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
        assertEquals( "DRRC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                      + "2.0", secondResult.get( 1 ) );

        Optional<Path> pathToThirdFile =
                pathsToFile.stream()
                           .filter( next -> next.endsWith( "DRRC2_DRRC2_HEFS_MEAN_SQUARE_ERROR.csv" ) )
                           .findAny();

        // Check the expected path: #61841
        assertTrue( pathToThirdFile.isPresent() );

        List<String> thirdResult = Files.readAllLines( pathToThirdFile.get() );

        assertTrue( thirdResult.get( 0 ).contains( "," ) );
        assertTrue( thirdResult.get( 0 ).contains( "ERROR" ) );
        assertEquals( "DRRC2,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,3600,"
                      + "1.0", thirdResult.get( 1 ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToThirdFile.get() );
        Files.deleteIfExists( pathToSecondFile.get() );
        Files.deleteIfExists( pathToFirstFile.get() );
    }

    /**
     * Tests the writing of {@link DurationScoreStatisticOuter} to file.
     *
     * @throws IOException if the output could not be written
     */

    @Test
    public void writeDurationScores() throws IOException
    {
        EvaluationDeclaration declaration =
                WriterTestHelper.getMockedDeclaration( new DecimalFormat( "0.000000" ) );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DurationScoreComponentOuter, DurationScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( declaration,
                                              this.outputDirectory,
                                              next -> MessageFactory.getDuration( next.getStatistic().getValue() ).toString() );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getDurationScoreStatisticsForOnePool() );

        // Check the expected number of paths: #61841
        assertEquals( 1, pathsToFile.size() );

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
     * @throws IOException if the output could not be written
     */

    @Test
    public void writeDoubleScoresWithMissingData() throws IOException
    {
        EvaluationDeclaration declaration =
                WriterTestHelper.getMockedDeclaration( new DecimalFormat( "0.000000" ) );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedScoreWriter<DoubleScoreComponentOuter, DoubleScoreStatisticOuter> writer =
                CommaSeparatedScoreWriter.of( declaration,
                                              this.outputDirectory,
                                              next -> Double.toString( next.getStatistic().getValue() ) );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getScoreStatisticsForThreePoolsWithMissings() );

        try
        {
            // Check the expected number of paths: #61841
            assertEquals( 1, pathsToFile.size() );

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
        }
        // Clean-up
        finally
        {
            // If all succeeded, remove the file, otherwise leave to help debugging.
            for ( Path path : pathsToFile )
            {
                Files.deleteIfExists( path );
            }
        }
    }

}
