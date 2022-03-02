package wres.vis.writing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.PngFormat;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link BoxplotGraphicsWriter}.
 * 
 * @author James Brown
 */

class BoxplotGraphicsWriterTest
{
    /** Temp dir.*/ 
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );    
    
    /**
     * Tests the writing of {@link BoxplotStatisticOuter} to file.
     * 
     * @throws IOException if the chart could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    void testWriteCreatesOneBoxplotChartByObservedValue() throws IOException, InterruptedException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setPng( PngFormat.getDefaultInstance() )
                                 .build();

        Path expected = Paths.get( TEMP_DIR, "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.png" );
        Files.deleteIfExists( expected );
        
        // Aims to use an in-memory file system, but this only works for java.nio and some low-level writing code still
        // uses java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( TEMP_DIR );
            Files.createDirectory( directory );

            // Create the writer and write
            BoxplotGraphicsWriter writer = BoxplotGraphicsWriter.of( outputs,
                                                                     directory );

            List<BoxplotStatisticOuter> statistics = TestDataGenerator.getBoxPlotPerPairForOnePool();

            Set<Path> paths = writer.apply( statistics );

            // Check the expected number of paths: #61841
            assertEquals( 1, paths.size() );

            Path actual = paths.iterator().next();

            // Check the expected path: #61841
            assertEquals( expected, actual );

            Files.deleteIfExists( actual );
        }
    }

    /**
     * Tests the writing of {@link BoxplotStatisticOuter} to file.
     * 
     * @throws IOException if the chart could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    void testWriteCreatesOneBoxplotChart() throws IOException, InterruptedException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setPng( PngFormat.getDefaultInstance() )
                                 .build();

        Path expected = Paths.get( TEMP_DIR, "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS.png" );
        Files.deleteIfExists( expected );

        // Aims to use an in-memory file system, but this only works for java.nio and some low-level writing code still
        // uses java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( TEMP_DIR );
            Files.createDirectory( directory );

            // Create the writer and write
            BoxplotGraphicsWriter writer = BoxplotGraphicsWriter.of( outputs,
                                                                     directory );

            List<BoxplotStatisticOuter> statistics = TestDataGenerator.getBoxPlotPerPoolForTwoPools();

            Set<Path> paths = writer.apply( statistics );

            // Check the expected number of paths: #61841
            assertEquals( 1, paths.size() );

            Path actual = paths.iterator().next();

            // Check the expected path: #61841
            assertEquals( expected, actual );

            Files.deleteIfExists( actual );
        }
    }

}
