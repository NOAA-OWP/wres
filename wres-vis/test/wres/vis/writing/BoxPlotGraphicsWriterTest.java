package wres.vis.writing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import wres.config.ProjectConfigException;
import wres.datamodel.FeatureKey;
import wres.datamodel.FeatureTuple;
import wres.datamodel.OneOrTwoDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Outputs.PngFormat;
import wres.statistics.generated.Pool;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotGraphicsWriter}. There are not checks on the content of the PNG outputs,
 * only that outputs were written.
 */

public class BoxPlotGraphicsWriterTest
{

    /**
     * Output directory.
     */

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
        Outputs outputs = Outputs.newBuilder()
                                 .setPng( PngFormat.getDefaultInstance() )
                                 .build();

        // Begin the actual test now that we have constructed dependencies.
        BoxPlotGraphicsWriter writer = BoxPlotGraphicsWriter.of( outputs,
                                                                 this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( BoxPlotGraphicsWriterTest.getBoxPlotPerPairForOnePool() );

        // Check the expected number of paths: #61841
        assertEquals( 1, pathsToFile.size() );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE_86400_SECONDS.png" ) );

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
        Outputs outputs = Outputs.newBuilder()
                .setPng( PngFormat.getDefaultInstance() )
                .build();
        
        // Begin the actual test now that we have constructed dependencies.
        BoxPlotGraphicsWriter writer = BoxPlotGraphicsWriter.of( outputs,
                                                                 this.outputDirectory );

        Set<Path> pathsToFile = writer.apply( BoxPlotGraphicsWriterTest.getBoxPlotPerPoolForTwoPools() );

        // Check the expected number of paths: #61841
        assertEquals( 1, pathsToFile.size() );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_JUNP1_HEFS_BOX_PLOT_OF_ERRORS.png" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for two pools of data.
     * 
     * @return a box plot per pool for two pools
     */

    private static List<BoxplotStatisticOuter> getBoxPlotPerPoolForTwoPools()
    {
        // location id
        FeatureKey feature = FeatureKey.of( "JUNP1" );

        // Create fake outputs
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 24 ),
                                    Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadataOne = SampleMetadata.of( evaluation, pool );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( 1.0, 3.0, 5.0, 7.0, 9.0 ) )
                     .build();

        BoxplotStatistic boxOne = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( box )
                                                  .build();

        BoxplotStatisticOuter fakeOutputsOne = BoxplotStatisticOuter.of( boxOne, fakeMetadataOne );

        TimeWindowOuter timeTwo =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 48 ),
                                    Duration.ofHours( 48 ) );

        Pool poolTwo = MessageFactory.parse( new FeatureTuple( feature,
                                                               feature,
                                                               null ),
                                             timeTwo,
                                             null,
                                             threshold,
                                             false );

        SampleMetadata fakeMetadataTwo = SampleMetadata.of( evaluation, poolTwo );

        Box anotherBox = Box.newBuilder()
                            .addAllQuantiles( List.of( 11.0, 33.0, 55.0, 77.0, 99.0 ) )
                            .build();

        BoxplotStatistic boxTwo = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( anotherBox )
                                                  .build();

        BoxplotStatisticOuter fakeOutputsTwo = BoxplotStatisticOuter.of( boxTwo, fakeMetadataTwo );

        return List.of( fakeOutputsOne, fakeOutputsTwo );
    }

    /**
     * Returns a {@link List} containing {@link BoxplotStatisticOuter} for several pairs.
     * 
     * @return a box plot per pair
     */

    private static List<BoxplotStatisticOuter> getBoxPlotPerPairForOnePool()
    {
        // location id
        FeatureKey feature = FeatureKey.of( "JUNP1" );

        // Create fake outputs
        TimeWindowOuter timeOne =
                TimeWindowOuter.of( Instant.MIN,
                                    Instant.MAX,
                                    Duration.ofHours( 24 ),
                                    Duration.ofHours( 24 ) );

        OneOrTwoThresholds threshold =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( Double.NEGATIVE_INFINITY ),
                                                          Operator.GREATER,
                                                          ThresholdDataType.LEFT ) );

        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightDataName( "HEFS" )
                                          .setBaselineDataName( "ESP" )
                                          .setRightVariableName( "SQIN" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( feature,
                                                            feature,
                                                            null ),
                                          timeOne,
                                          null,
                                          threshold,
                                          false );

        SampleMetadata fakeMetadata = SampleMetadata.of( evaluation, pool );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE )
                                            .setLinkedValueType( LinkedValueType.OBSERVED_VALUE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box first = Box.newBuilder()
                       .addAllQuantiles( List.of( 2.0, 3.0, 4.0, 5.0, 6.0 ) )
                       .setLinkedValue( 1.0 )
                       .build();

        Box second = Box.newBuilder()
                        .addAllQuantiles( List.of( 7.0, 9.0, 11.0, 13.0, 15.0 ) )
                        .setLinkedValue( 3.0 )
                        .build();

        Box third = Box.newBuilder()
                       .addAllQuantiles( List.of( 21.0, 24.0, 27.0, 30.0, 33.0 ) )
                       .setLinkedValue( 5.0 )
                       .build();

        BoxplotStatistic boxOne = BoxplotStatistic.newBuilder()
                                                  .setMetric( metric )
                                                  .addStatistics( first )
                                                  .addStatistics( second )
                                                  .addStatistics( third )
                                                  .build();


        return List.of( BoxplotStatisticOuter.of( boxOne, fakeMetadata ) );
    }

}
