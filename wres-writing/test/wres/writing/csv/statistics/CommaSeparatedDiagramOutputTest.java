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
 * Tests the writing of diagram outputs to a file of Comma Separated Values (CSV).
 * @deprecated since v5.8. Use the {@link CsvStatisticsWriter} instead.
 */

@Deprecated( since = "5.8", forRemoval = true )
public class CommaSeparatedDiagramOutputTest
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @Test
    public void writeDiagramOutput() throws IOException
    {
        EvaluationDeclaration declaration =
                WriterTestHelper.getMockedDeclaration( new DecimalFormat( "0.00000" ) );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedDiagramWriter writer = CommaSeparatedDiagramWriter.of( declaration,
                                                                             this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getReliabilityDiagramForOnePool() );

        try
        {
            // Check the expected number of paths: #61841
            assertEquals( 1, pathsToFile.size() );

            Path pathToFile = pathsToFile.iterator()
                                         .next();

            // Check the expected path: #61841
            assertEquals( "CREC1_CREC1_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv", pathToFile.toFile().getName() );

            List<String> result = Files.readAllLines( pathToFile );

            assertTrue( result.get( 0 ).contains( "," ) );
            assertTrue( result.get( 0 ).contains( "FORECAST PROBABILITY" ) );
            assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                          + "0.08625,0.06294,5926.00000",
                          result.get( 1 ) );
            assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                          + "0.29550,0.29380,371.00000",
                          result.get( 2 ) );
            assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                          + "0.50723,0.50000,540.00000",
                          result.get( 3 ) );
            assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                          + "0.70648,0.73538,650.00000",
                          result.get( 4 ) );
            assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                          + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                          + "0.92682,0.93937,1501.00000",
                          result.get( 5 ) );
        }
        // Clean-up
        finally
        {
            for( Path path : pathsToFile )
            {
                Files.deleteIfExists( path );
            }
        }
    }

}
