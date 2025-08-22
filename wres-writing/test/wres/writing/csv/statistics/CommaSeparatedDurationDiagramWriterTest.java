package wres.writing.csv.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import wres.config.components.EvaluationDeclaration;

import wres.writing.WriterTestHelper;

/**
 * Tests the writing of paired outputs to a file of Comma Separated Values (CSV).
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedDurationDiagramWriterTest
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @Test
    public void writePairedOutputForTimeSeriesMetrics() throws IOException
    {
        EvaluationDeclaration declaration =
                WriterTestHelper.getMockedDeclaration( new DecimalFormat( "0.000000" ) );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedDurationDiagramWriter writer = CommaSeparatedDurationDiagramWriter.of( declaration,
                                                                                             this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getTimeToPeakErrorsForOnePool() );

        try
        {
            // Check the expected number of paths: #61841
            assertEquals( 1, pathsToFile.size() );

            Path pathToFile = pathsToFile.iterator().next();

            // Check the expected path: #61841
            assertEquals( "FTSC1_FTSC1_HEFS_TIME_TO_PEAK_ERROR.csv", pathToFile.toFile().getName() );

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
        }
        // Clean-up
        finally
            {
                // If all succeeded, remove the file, otherwise leave to help debugging.
                for( Path path : pathsToFile )
                {
                    Files.deleteIfExists( path );
                }
            }
    }

}
