package wres.vis.writing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.GraphicFormat.DurationUnit;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link DurationDiagramGraphicsWriter}.
 * 
 *  @author James Brown
 */

class DurationDiagramGraphicsWriterTest
{
    /** Temp dir.*/
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );

    /**
     * Tests the writing of {@link DurationDiagramStatisticOuter} to file.
     * 
     * @throws IOException if the charts could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    void testWriteCreatesOneDurationDiagramChart() throws IOException, InterruptedException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setSvg( SvgFormat.newBuilder()
                                                   .setOptions( GraphicFormat.newBuilder()
                                                                             .setLeadUnit( DurationUnit.HOURS ) ) )
                                 .build();

        Path expected = Paths.get( TEMP_DIR, "DRRC2_09165000_18384141_HEFS_TIME_TO_PEAK_ERROR.svg" );
        Files.deleteIfExists( expected );

        // Aims to use an in-memory file system, but this only works for java.nio and some low-level writing code still
        // uses java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
//        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
//        {
//            Path directory = fileSystem.getPath( TEMP_DIR );
        Path directory = Paths.get( TEMP_DIR );
//            Files.createDirectory( directory );

        // Create the writer and write
        DurationDiagramGraphicsWriter writer = DurationDiagramGraphicsWriter.of( outputs,
                                                                                 directory );

        List<DurationDiagramStatisticOuter> statistics = TestDataGenerator.getTimeToPeakErrors();

        Set<Path> actualPaths = writer.apply( statistics );

        Path actual = actualPaths.iterator().next();

        // Check the expected number of paths
        assertEquals( 1, actualPaths.size() );

        assertEquals( expected, actual );

        // Clean up
        Files.deleteIfExists( actual );
//        }
    }

}
