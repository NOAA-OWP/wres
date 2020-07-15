package wres.io.writing.png;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DatasetIdentifier;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.io.writing.WriterTestHelper;
import wres.statistics.generated.BoxplotStatistic;
import wres.system.SystemSettings;

/**
 * Tests the {@link PNGBoxPlotWriter}. There are not checks on the content of the PNG outputs,
 * only that outputs were written.
 * 
 * TODO: add the templates from dist/lib/conf in wres-vis to the test path for these 
 * tests to pass in gradle, then unignore
 */
@Ignore
public class PNGBoxPlotWriterTest
{

    /**
     * Output directory.
     */

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Fake location for testing.
     */

    private static final String LOCATION_ID = "JUNP1";

    @Mock
    private SystemSettings mockSystemSettings;

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
        Feature feature = WriterTestHelper.getMockedFeature( PNGBoxPlotWriterTest.LOCATION_ID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.GRAPHIC );
        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() ).thenReturn( projectConfig );

        // Begin the actual test now that we have constructed dependencies.
        PNGBoxPlotWriter writer = PNGBoxPlotWriter.of( this.mockSystemSettings,
                                                       projectConfigPlus,
                                                       ChronoUnit.SECONDS,
                                                       this.outputDirectory );

        writer.accept( WriterTestHelper.getBoxPlotPerPairForOnePool() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.png" ) );

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

        // Construct a fake configuration file.
        Feature feature = WriterTestHelper.getMockedFeature( PNGBoxPlotWriterTest.LOCATION_ID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.GRAPHIC );
        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() ).thenReturn( projectConfig );

        // Begin the actual test now that we have constructed dependencies.
        PNGBoxPlotWriter writer = PNGBoxPlotWriter.of( this.mockSystemSettings,
                                                       projectConfigPlus,
                                                       ChronoUnit.SECONDS,
                                                       this.outputDirectory );

        writer.accept( WriterTestHelper.getBoxPlotPerPoolForTwoPools() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS.png" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * <p>Tests for the writing of no box plot statistics, without exception, when no box 
     * plot statistics are available.
     * 
     * <p>See #62863.
     * @throws IOException if the path containing output cannot be deleted
     */

    @Test
    public void writeWithoutExceptionWhenNothingIsAvailable() throws IOException
    {

        // Construct the fake metadata for the nothing output
        TimeWindowOuter timeOne = TimeWindowOuter.of();
        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( PNGBoxPlotWriterTest.LOCATION_ID ), "SQIN" );

        MeasurementUnit measurementUnit = MeasurementUnit.of( "CMS" );
        SampleMetadata fakeMetadata = SampleMetadata.of( measurementUnit,
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold );

        // Construct the fake declaration
        Feature feature = WriterTestHelper.getMockedFeature( PNGBoxPlotWriterTest.LOCATION_ID );
        ProjectConfig projectConfig = WriterTestHelper.getMockedProjectConfig( feature, DestinationType.GRAPHIC );

        // Mock the return of the fake declaration when required
        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() ).thenReturn( projectConfig );

        // Construct the nothing data
        BoxplotStatisticOuter emptyFakeStatistics = BoxplotStatisticOuter.of( BoxplotStatistic.getDefaultInstance(),
                                                                              fakeMetadata );
        List<BoxplotStatisticOuter> fakeStatistics = Collections.singletonList( emptyFakeStatistics );

        // Begin the actual test now that we have the dependencies.
        PNGBoxPlotWriter writer = PNGBoxPlotWriter.of( this.mockSystemSettings,
                                                       projectConfigPlus,
                                                       ChronoUnit.SECONDS,
                                                       this.outputDirectory );

        // Attempt to write empty output
        writer.accept( fakeStatistics );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_BOX_PLOT_OF_ERRORS.png" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
