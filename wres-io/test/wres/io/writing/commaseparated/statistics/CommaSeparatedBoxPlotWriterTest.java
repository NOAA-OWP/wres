package wres.io.writing.commaseparated.statistics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;

/**
 * Tests the writing of box plot outputs to a file of Comma Separated Values (CSV).
 */

public class CommaSeparatedBoxPlotWriterTest extends CommaSeparatedWriterTestHelper
{

    private final Path outputDirectory = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    /**
     * Tests the writing of {@link BoxPlotStatistics} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeBoxPlotOutputPerPair()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "JUNP1";

        // Create fake outputs

        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadata =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                      null );

        List<BoxPlotStatistic> fakeOutputs = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 2, 3, 4, 5, 6 ),
                                              fakeMetadata,
                                              1,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 7, 9, 11, 13, 15 ),
                                              fakeMetadata,
                                              3,
                                              MetricDimension.OBSERVED_VALUE ) );
        fakeOutputs.add( BoxPlotStatistic.of( probs,
                                              VectorOfDoubles.of( 21, 24, 27, 30, 33 ),
                                              fakeMetadata,
                                              5,
                                              MetricDimension.OBSERVED_VALUE ) );

        // Fake output wrapper.
        ListOfStatistics<BoxPlotStatistics> fakeOutputData =
                ListOfStatistics.of( Collections.singletonList( BoxPlotStatistics.of( fakeOutputs,
                                                                                      fakeMetadata ) ) );
        // wrap outputs in future
        Future<ListOfStatistics<BoxPlotStatistics>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addBoxPlotStatisticsPerPair( outputMapByMetricFuture );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedBoxPlotWriter writer = CommaSeparatedBoxPlotWriter.of( projectConfig,
                                                                             ChronoUnit.SECONDS,
                                                                             this.outputDirectory );

        writer.accept( output.getBoxPlotStatisticsPerPair() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.csv" ) );

        List<String> result = Files.readAllLines( pathToFile );

        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "OBSERVED VALUE" ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );

        assertEquals( 4, result.size() );
        
        assertTrue( result.get( 1 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                                   + "1.0,2.0,3.0,4.0,5.0,6.0" ) );
        assertTrue( result.get( 2 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                                   + "3.0,7.0,9.0,11.0,13.0,15.0" ) );
        assertTrue( result.get( 3 )
                          .equals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                                   + "5.0,21.0,24.0,27.0,30.0,33.0" ) );
        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Tests the writing of {@link BoxPlotStatistics} to file.
     * 
     * @throws ProjectConfigException if the project configuration is incorrect
     * @throws IOException if the output could not be written
     * @throws InterruptedException if the process is interrupted
     */

    @Test
    public void writeBoxPlotOutputPerPool()
            throws IOException, InterruptedException
    {

        // location id
        final String LID = "JUNP1";

        // Create fake outputs

        StatisticsForProject.StatisticsForProjectBuilder outputBuilder =
                DataFactory.ofMetricOutputForProjectByTimeAndThreshold();

        TimeWindow timeOne =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 24 ),
                               Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        // Output requires a future... which requires a metadata...
        // which requires a datasetidentifier..

        DatasetIdentifier datasetIdentifier =
                DatasetIdentifier.of( Location.of( LID ), "SQIN", "HEFS", "ESP" );

        StatisticMetadata fakeMetadataOne =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeOne,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS,
                                      null );

        List<BoxPlotStatistic> fakeOutputsOne = new ArrayList<>();
        VectorOfDoubles probs = VectorOfDoubles.of( 0, 0.25, 0.5, 0.75, 1.0 );

        fakeOutputsOne.add( BoxPlotStatistic.of( probs,
                                                 VectorOfDoubles.of( 1, 3, 5, 7, 9 ),
                                                 fakeMetadataOne ) );
        
        TimeWindow timeTwo =
                TimeWindow.of( Instant.MIN,
                               Instant.MAX,
                               Duration.ofHours( 48 ),
                               Duration.ofHours( 48 ) );
        
        StatisticMetadata fakeMetadataTwo =
                StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                         datasetIdentifier,
                                                         timeTwo,
                                                         threshold ),
                                      1000,
                                      MeasurementUnit.of(),
                                      MetricConstants.BOX_PLOT_OF_ERRORS,
                                      null );
        
        List<BoxPlotStatistic> fakeOutputsTwo = Collections.singletonList( BoxPlotStatistic.of( probs,
                                                 VectorOfDoubles.of( 11, 33, 55, 77, 99 ),
                                                 fakeMetadataTwo ) );
        
        // Fake output wrapper.
        ListOfStatistics<BoxPlotStatistics> fakeOutputData =
                ListOfStatistics.of( Arrays.asList( BoxPlotStatistics.of( fakeOutputsOne,
                                                                          fakeMetadataOne ),
                                                    BoxPlotStatistics.of( fakeOutputsTwo,
                                                                          fakeMetadataTwo ) ) );
        // wrap outputs in future
        Future<ListOfStatistics<BoxPlotStatistics>> outputMapByMetricFuture =
                CompletableFuture.completedFuture( fakeOutputData );

        outputBuilder.addBoxPlotStatisticsPerPool( outputMapByMetricFuture );

        StatisticsForProject output = outputBuilder.build();

        // Construct a fake configuration file.
        Feature feature = getMockedFeature( LID );
        ProjectConfig projectConfig = getMockedProjectConfig( feature );

        // Begin the actual test now that we have constructed dependencies.
        CommaSeparatedBoxPlotWriter writer = CommaSeparatedBoxPlotWriter.of( projectConfig,
                                                                             ChronoUnit.SECONDS,
                                                                             this.outputDirectory );

        writer.accept( output.getBoxPlotStatisticsPerPool() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertTrue( pathsToFile.size() == 1 );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS.csv" ) );

        List<String> result = Files.readAllLines( pathToFile );

        assertEquals( 3, result.size() );
        
        assertTrue( result.get( 0 ).contains( "," ) );
        assertTrue( result.get( 0 ).contains( "FORECAST ERROR" ) );

        assertEquals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,86400,86400,"
                + "1.0,3.0,5.0,7.0,9.0", result.get( 1 ) );
        
        assertEquals( "-1000000000-01-01T00:00:00Z,+1000000000-12-31T23:59:59.999999999Z,172800,172800,"
                + "11.0,33.0,55.0,77.0,99.0", result.get( 2 ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

}
