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
import wres.datamodel.statistics.BoxplotStatisticOuter;

import wres.io.writing.WriterTestHelper;

/**
 * Tests the writing of box plot outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedBoxPlotWriterTest
{

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link BoxplotStatisticOuter} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeBoxPlotOutputPerPair()
            throws IOException, InterruptedException
    {

        // Construct a fake configuration file
        String LID = "JUNP1";
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedBoxPlotWriter writer = CommaSeparatedBoxPlotWriter.of( projectConfig,
                                                                             ChronoUnit.SECONDS,
                                                                             this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getBoxPlotPerPairForOnePool() );

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertEquals( "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.csv",
                      pathToFile.toFile().getName() );

        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "OBSERVED VALUE" ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );

        assertEquals( 4, result.size() );

        assertEquals( "JUNP1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "1.0,2.0,3.0,4.0,5.0,6.0",
                      result.get( 1 ) );
        assertEquals( "JUNP1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "3.0,7.0,9.0,11.0,13.0,15.0",
                      result.get( 2 ) );
        assertEquals( "JUNP1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "5.0,21.0,24.0,27.0,30.0,33.0",
                      result.get( 3 ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Tests the writing of {@link BoxplotStatisticOuter} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeBoxPlotOutputPerPool()
            throws IOException, InterruptedException
    {

        // Construct a fake configuration file
        String LID = "JUNP1";
        Feature feature = WriterTestHelper.getMockedFeature( LID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.NUMERIC );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedBoxPlotWriter writer = CommaSeparatedBoxPlotWriter.of( projectConfig,
                                                                             ChronoUnit.SECONDS,
                                                                             this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( WriterTestHelper.getBoxPlotPerPoolForTwoPools() );

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertEquals( "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS.csv",
                      pathToFile.toFile().getName() );

        List<String> result = Files.readAllLines( pathToFile );

        assertEquals( 3, result.size() );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );

        assertEquals( "JUNP1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                      + "1.0,3.0,5.0,7.0,9.0",
                      result.get( 1 ) );

        assertEquals( "JUNP1,-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,"
                      + "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,172800,172800,"
                      + "11.0,33.0,55.0,77.0,99.0",
                      result.get( 2 ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
