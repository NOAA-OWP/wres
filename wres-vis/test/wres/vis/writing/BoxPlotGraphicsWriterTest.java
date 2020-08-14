package wres.vis.writing;

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
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.mockito.Mockito;

import wres.config.ProjectConfigException;
import wres.config.ProjectConfigPlus;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.Feature;
import wres.config.generated.GraphicalType;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
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
     * Fake location for testing.
     */

    private static final String LOCATION_ID = "JUNP1";

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
        Feature feature = BoxPlotGraphicsWriterTest.getMockedFeature( BoxPlotGraphicsWriterTest.LOCATION_ID );
        ProjectConfig projectConfig =
                BoxPlotGraphicsWriterTest.getMockedProjectConfig( feature, DestinationType.GRAPHIC );
        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() ).thenReturn( projectConfig );

        // Begin the actual test now that we have constructed dependencies.
        BoxPlotGraphicsWriter writer = BoxPlotGraphicsWriter.of( projectConfigPlus,
                                                                 ChronoUnit.SECONDS,
                                                                 this.outputDirectory );

        writer.accept( BoxPlotGraphicsWriterTest.getBoxPlotPerPairForOnePool() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertEquals( 1, pathsToFile.size() );

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
        Feature feature = BoxPlotGraphicsWriterTest.getMockedFeature( BoxPlotGraphicsWriterTest.LOCATION_ID );
        ProjectConfig projectConfig =
                BoxPlotGraphicsWriterTest.getMockedProjectConfig( feature, DestinationType.GRAPHIC );
        ProjectConfigPlus projectConfigPlus = Mockito.mock( ProjectConfigPlus.class );
        Mockito.when( projectConfigPlus.getProjectConfig() ).thenReturn( projectConfig );

        // Begin the actual test now that we have constructed dependencies.
        BoxPlotGraphicsWriter writer = BoxPlotGraphicsWriter.of( projectConfigPlus,
                                                                 ChronoUnit.SECONDS,
                                                                 this.outputDirectory );

        writer.accept( BoxPlotGraphicsWriterTest.getBoxPlotPerPoolForTwoPools() );

        // Determine the paths written
        Set<Path> pathsToFile = writer.get();

        // Check the expected number of paths: #61841
        assertEquals( 1, pathsToFile.size() );

        Path pathToFile = pathsToFile.iterator().next();

        // Check the expected path: #61841
        assertTrue( pathToFile.endsWith( "JUNP1_SQIN_HEFS_BOX_PLOT_OF_ERRORS.png" ) );

        // If all succeeded, remove the file, otherwise leave to help debugging.
        Files.deleteIfExists( pathToFile );
    }

    /**
     * Returns a fake project configuration for a specified feature.
     * 
     * @param feature the feature
     * @param destinationType the destination type
     * @return fake project configuration
     */

    private static ProjectConfig getMockedProjectConfig( Feature feature, DestinationType destinationType )
    {
        // Use the system temp directory so that checks for writeability pass.
        GraphicalType graphics = new GraphicalType( null, null, 800, 600, null );
        DestinationConfig destinationConfig =
                new DestinationConfig( null,
                                       graphics,
                                       null,
                                       destinationType,
                                       null );

        List<DestinationConfig> destinations = new ArrayList<>();
        destinations.add( destinationConfig );

        ProjectConfig.Outputs outputsConfig =
                new ProjectConfig.Outputs( destinations, null );

        List<Feature> features = new ArrayList<>();
        features.add( feature );

        PairConfig pairConfig = new PairConfig( null,
                                                null,
                                                features,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig projectConfig = new ProjectConfig( null,
                                                         pairConfig,
                                                         null,
                                                         outputsConfig,
                                                         null,
                                                         "test" );
        return projectConfig;
    }

    /**
     * Returns a fake feature for a specified location identifier.
     * 
     * @param featureName the location identifier
     */

    private static Feature getMockedFeature( String featureName )
    {
        return new Feature( featureName,
                            featureName,
                            null );
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
