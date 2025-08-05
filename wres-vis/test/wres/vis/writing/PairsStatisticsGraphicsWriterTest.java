package wres.vis.writing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.datamodel.statistics.PairsStatisticOuter;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.DurationUnit;
import wres.statistics.generated.Outputs.GraphicFormat;
import wres.statistics.generated.Outputs.SvgFormat;
import wres.vis.TestDataGenerator;

/**
 * Tests the {@link PairsStatisticsGraphicsWriter}.
 *
 *  @author James Brown
 */

class PairsStatisticsGraphicsWriterTest
{
    /** Temp dir.*/
    private static final String TEMP_DIR = System.getProperty( "java.io.tmpdir" );

    /**
     * Tests the writing of {@link PairsStatisticOuter} to file.
     *
     * @throws IOException if the charts could not be written
     */

    @Test
    void testWriteCreatesOnePairsChart() throws IOException
    {
        Outputs outputs = Outputs.newBuilder()
                                 .setSvg( SvgFormat.newBuilder()
                                                   .setOptions( GraphicFormat.newBuilder()
                                                                             .setLeadUnit( DurationUnit.HOURS ) ) )
                                 .build();

        // Aims to use an in-memory file system, but this only works for java.nio and the JFreeChart dependencies 
        // use java.io, so this will write to disk in the mean time, hence deleting any existing file before/after
        Path expectedOne = Paths.get( TEMP_DIR, "DRRC2_DRRC2_DRRC2_HEFS_TIME_SERIES_PLOT_7_HOURS.svg" );

        Files.deleteIfExists( expectedOne );

        //        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        //        {
        //            Path directory = fileSystem.getPath( TEMP_DIR );
        Path directory = Paths.get( TEMP_DIR );
        //Files.createDirectory( directory );

        // Create the writer and write
        PairsStatisticsGraphicsWriter writer = PairsStatisticsGraphicsWriter.of( outputs,
                                                                                 directory );

        PairsStatisticOuter statistics =
                TestDataGenerator.getPairsStatisticsForOnePoolWithTwoTimeSeries();

        Set<Path> actual = writer.apply( List.of( statistics ) );

        // Check the expected path
        Set<Path> expected = Set.of( expectedOne );

        assertEquals( expected, actual );

        // Clean up
        for ( Path next : actual )
        {
            Files.deleteIfExists( next );
        }
        //        }
    }

}
