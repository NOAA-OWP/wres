package wres.io.writing.commaseparated.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.statistics.DiagramStatisticOuter;

import wres.io.writing.WriterTestHelper;

/**
 * Tests the writing of diagram outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedDiagramOutputTest
{
    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link DiagramStatisticOuter} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeDiagramOutput()
            throws IOException, InterruptedException
    {

        // Construct a fake configuration file
        String LID = "CREC1";
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedDiagramWriter writer = CommaSeparatedDiagramWriter.of( projectConfig,
                                                                             ChronoUnit.SECONDS,
                                                                             this.outputDirectory );

        writer.accept( WriterTestHelper.getReliabilityDiagramForOnePool() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();
        
        // Check the expected path: #61841
        assertEquals( "CREC1_SQIN_HEFS_RELIABILITY_DIAGRAM_86400_SECONDS.csv", pathToFile.toFile().getName() );

        List<String> result = Files.readAllLines( pathToFile );
        
        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "FORECAST PROBABILITY" ) );
        assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "0.08625,0.06294,5926.0",
                      result.get( 1 ) );
        assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "0.2955,0.2938,371.0",
                      result.get( 2 ) );
        assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "0.50723,0.5,540.0",
                      result.get( 3 ) );
        assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "0.70648,0.73538,650.0",
                      result.get( 4 ) );
        assertEquals( "CREC1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "0.92682,0.93937,1501.0",
                      result.get( 5 ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
