package wres.vis.writing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.DurationUnit;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link DiagramGraphicsWriter}.
 * 
 *  @author James Brown
 */

class DiagramGraphicsWriterTest
{
    /** Temp dir.*/
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );

    /**
     * Tests the writing of {@link DiagramStatisticOuter} to file.
     * 
     * @throws IOException if the charts could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    void testWriteCreatesThreeDiagramCharts() throws IOException, InterruptedException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setSvg( SvgFormat.newBuilder()
                                                   .setOptions( GraphicFormat.newBuilder()
                                                                             .setLeadUnit( DurationUnit.HOURS ) ) )
                                 .build();

        // Aims to use an in-memory file system, but this only works for java.nio and the JFreeChart dependencies 
        // use java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
        Path expectedOne = Paths.get( TEMP_DIR, "DRRC2_09165000_18384141_HEFS_RANK_HISTOGRAM_1_HOURS.svg" );
        Path expectedTwo = Paths.get( TEMP_DIR, "DRRC2_09165000_18384141_HEFS_RANK_HISTOGRAM_2_HOURS.svg" );
        Path expectedThree = Paths.get( TEMP_DIR, "DRRC2_09165000_18384141_HEFS_RANK_HISTOGRAM_3_HOURS.svg" );

        Files.deleteIfExists( expectedOne );
        Files.deleteIfExists( expectedTwo );
        Files.deleteIfExists( expectedThree );

//        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
//        {
//            Path directory = fileSystem.getPath( TEMP_DIR );
        Path directory = Paths.get( TEMP_DIR );
        //Files.createDirectory( directory );

        // Create the writer and write
        DiagramGraphicsWriter writer = DiagramGraphicsWriter.of( outputs,
                                                                 directory );

        List<DiagramStatisticOuter> statistics =
                TestDataGenerator.getDiagramStatisticsForTwoThresholdsAndThreeLeadDurations();

        Set<Path> actual = writer.apply( statistics );

        // Check the expected number of paths
        assertEquals( 3, actual.size() );

        // Check the expected path
        Set<Path> expected = Set.of( expectedOne, expectedTwo, expectedThree );

        assertEquals( expected, actual );

        // Clean up
        for ( Path next : actual )
        {
            Files.deleteIfExists( next );
        }
//        }
    }

}
