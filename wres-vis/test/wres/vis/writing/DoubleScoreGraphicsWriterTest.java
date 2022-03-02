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

import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link DoubleScoreGraphicsWriter}.
 * 
 *  @author James Brown
 */

class DoubleScoreGraphicsWriterTest
{
    /** Temp dir.*/
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );

    /**
     * Tests the writing of {@link DoubleScoreStatisticOuter} to file.
     * 
     * @throws IOException if the chart could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    void testWriteCreatesOneScoreChart() throws IOException, InterruptedException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setSvg( SvgFormat.getDefaultInstance() )
                                 .build();

        Path expected = Paths.get( TEMP_DIR, "DRRC2_09165000_18384141_HEFS_BIAS_FRACTION.svg" );
        Files.deleteIfExists( expected );
        
        // Aims to use an in-memory file system, but this only works for java.nio and some low-level writing code still
        // uses java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path directory = fileSystem.getPath( TEMP_DIR );
            Files.createDirectory( directory );

            // Create the writer and write
            DoubleScoreGraphicsWriter writer = DoubleScoreGraphicsWriter.of( outputs,
                                                                             directory );

            List<DoubleScoreStatisticOuter> statistics = TestDataGenerator.getScoresForIssuedTimePools();

            Set<Path> paths = writer.apply( statistics );

            // Check the expected number of paths
            assertEquals( 1, paths.size() );

            Path actual = paths.iterator().next();

            // Check the expected path
            assertEquals( expected, actual );

            Files.deleteIfExists( actual );
        }
    }

}
