package wres.io.writing.commaseparated.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;

import wres.datamodel.statistics.DurationDiagramStatisticOuter;

import wres.io.writing.WriterTestHelper;

/**
 * Tests the writing of paired outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedDurationDiagramWriterTest
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link DurationDiagramStatisticOuter} to file, where the left pair comprises an {@link Instant} and the
     * right pair comprises an (@link Duration).
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writePairedOutputForTimeSeriesMetrics()
            throws IOException, InterruptedException
    {

        // location id
        String LID = "FTSC1";

        // Construct a fake configuration file.
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedDurationDiagramWriter writer =
                CommaSeparatedDurationDiagramWriter.of( projectConfig,
                                                        ChronoUnit.SECONDS,
                                                        this.outputDirectory );

        writer.accept( WriterTestHelper.getTimeToPeakErrorsForOnePool() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertEquals( "FTSC1_SQIN_HEFS_TIME_TO_PEAK_ERROR.csv", pathToFile.toFile().getName() );

        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "ERROR" ) );

        assertEquals( "FTSC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,64800,"
                      + "1985-01-01T00:00:00Z,PT1H",
                      result.get( 1 ) );
        assertEquals( "FTSC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,64800,"
                      + "1985-01-02T00:00:00Z,PT2H",
                      result.get( 2 ) );
        assertEquals( "FTSC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,3600,64800,"
                      + "1985-01-03T00:00:00Z,PT3H",
                      result.get( 3 ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
