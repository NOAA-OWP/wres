package wres.io.writing.netcdf;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DurationUnit;
import wres.config.generated.IntBoundsType;
import wres.config.generated.InterfaceShortHand;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.yaml.components.ThresholdType;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeWindow;
import wres.system.SystemSettings;

/**
 * Tests the {@link NetcdfOutputWriter}.
 *
 * @author James Brown
 */
class NetcdfOutputWriterTest
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( NetcdfOutputWriterTest.class );

    /**  Temp dir to write. Cannot use an in-memory file system because the low-level writer does not use java.nio. */
    private static final Path TEMP_DIR = Paths.get( System.getProperty( "java.io.tmpdir" ) );

    @BeforeEach
    void runBeforeEachTest() throws IOException
    {
        Path output = TEMP_DIR.resolve( "testpath" );
        if ( !Files.exists( output ) )
        {
            Files.createDirectory( output );
        }
    }

    @AfterEach
    void runAfterEachTest() throws IOException
    {
        Path output = TEMP_DIR.resolve( "testpath" );
        try ( Stream<Path> paths = Files.walk( output )
                                        .sorted( Collections.reverseOrder() ) )
        {
            paths.forEach( path -> {
                try
                {
                    if ( !Files.isDirectory( path ) )
                    {
                        Files.deleteIfExists( path );
                    }
                }
                catch ( IOException e )
                {
                    LOGGER.warn( "Failed to delete path.", e );
                }
            } );
        }
    }

    @Test
    void testNetcdf2OutputWritingWithThresholdsThatVaryByFeatureDoesNotProduceAnException() throws IOException
    {
        Path output = TEMP_DIR.resolve( "testpath" );

        // Mock sufficient aspects of the classes required to create a writer
        SystemSettings systemSettings = Mockito.mock( SystemSettings.class );
        ProjectConfig projectConfig = Mockito.mock( ProjectConfig.class );

        PoolingWindowConfig pools = new PoolingWindowConfig( 3, 3, DurationUnit.HOURS );
        PairConfig pairConfig = Mockito.mock( PairConfig.class );
        IntBoundsType leadHours = new IntBoundsType( 0, 6 );
        Mockito.when( pairConfig.getLeadTimesPoolingWindow() )
               .thenReturn( pools );
        Mockito.when( pairConfig.getLeadHours() )
               .thenReturn( leadHours );
        Mockito.when( projectConfig.getPair() )
               .thenReturn( pairConfig );

        ProjectConfig.Inputs inputs = Mockito.mock( ProjectConfig.Inputs.class );

        DataSourceConfig leftSources = Mockito.mock( DataSourceConfig.class );
        DataSourceConfig.Source leftSource = Mockito.mock( DataSourceConfig.Source.class );
        Mockito.when( leftSource.getInterface() )
               .thenReturn( InterfaceShortHand.WRDS_NWM );
        Mockito.when( leftSources.getSource() )
               .thenReturn( List.of( leftSource ) );

        DataSourceConfig rightSources = Mockito.mock( DataSourceConfig.class );
        DataSourceConfig.Source rightSource = Mockito.mock( DataSourceConfig.Source.class );
        Mockito.when( rightSource.getInterface() )
               .thenReturn( InterfaceShortHand.USGS_NWIS );
        Mockito.when( rightSources.getSource() )
               .thenReturn( List.of( rightSource ) );
        Mockito.when( rightSources.getType() )
               .thenReturn( DatasourceType.ENSEMBLE_FORECASTS );

        Mockito.when( inputs.getLeft() )
               .thenReturn( leftSources );
        Mockito.when( inputs.getRight() )
               .thenReturn( rightSources );
        Mockito.when( projectConfig.getInputs() )
               .thenReturn( inputs );

        DestinationConfig destinationConfig = Mockito.mock( DestinationConfig.class );
        Mockito.when( destinationConfig.getType() )
               .thenReturn( DestinationType.NETCDF_2 );

        ChronoUnit durationUnits = ChronoUnit.HOURS;

        try ( NetcdfOutputWriter writer = NetcdfOutputWriter.of( systemSettings,
                                                                 projectConfig,
                                                                 destinationConfig,
                                                                 durationUnits,
                                                                 output,
                                                                 false ) )
        {
            // Create the features
            Geometry oneLeft = Geometry.newBuilder()
                                       .setName( "01013500" )
                                       .setWkt( "POINT( -68.58277778, 47.2375 )" )
                                       .build();
            Geometry twoLeft = Geometry.newBuilder()
                                       .setName( "01021470" )
                                       .setWkt( "POINT( -67.725, 44.8008333 )" )
                                       .build();

            Geometry oneRight = Geometry.newBuilder()
                                        .setName( "724696" )
                                        .build();
            Geometry twoRight = Geometry.newBuilder()
                                        .setName( "2677654" )
                                        .build();

            GeometryTuple geoTupleOne = GeometryTuple.newBuilder()
                                                     .setLeft( oneLeft )
                                                     .setRight( oneRight )
                                                     .build();
            GeometryTuple geoTupleTwo = GeometryTuple.newBuilder()
                                                     .setLeft( twoLeft )
                                                     .setRight( twoRight )
                                                     .build();

            GeometryGroup geoGroupOne = GeometryGroup.newBuilder()
                                                     .addGeometryTuples( geoTupleOne )
                                                     .build();
            GeometryGroup geoGroupTwo = GeometryGroup.newBuilder()
                                                     .addGeometryTuples( geoTupleTwo )
                                                     .build();

            FeatureGroup groupOne = FeatureGroup.of( geoGroupOne );
            FeatureGroup groupTwo = FeatureGroup.of( geoGroupTwo );

            Set<FeatureGroup> featureGroups = Set.of( groupOne, groupTwo );

            // Create the metrics
            Set<MetricConstants> metrics = Set.of( MetricConstants.PROBABILITY_OF_FALSE_DETECTION );

            // Create the thresholds
            Threshold thresholdOne = Threshold.newBuilder()
                                              .setLeftThresholdValue( DoubleValue.of( 6440.34 ) )
                                              .setThresholdValueUnits( "CFS" )
                                              .setOperator( Threshold.ThresholdOperator.GREATER )
                                              .setName( "1_5_year_recurrence_flow" )
                                              .build();

            Threshold thresholdTwo = Threshold.newBuilder()
                                              .setLeftThresholdValue( DoubleValue.of( 295.94 ) )
                                              .setThresholdValueUnits( "CFS" )
                                              .setOperator( Threshold.ThresholdOperator.GREATER )
                                              .setName( "1_5_year_recurrence_flow" )
                                              .build();

            Threshold thresholdThree = Threshold.newBuilder()
                                                .setLeftThresholdValue( DoubleValue.of( 0.5 ) )
                                                .setOperator( Threshold.ThresholdOperator.GREATER )
                                                .build();

            // Create the thresholds-by-feature
            ThresholdOuter thresholdOneWrapped = ThresholdOuter.of( thresholdOne, ThresholdType.VALUE );
            ThresholdOuter thresholdTwoWrapped = ThresholdOuter.of( thresholdTwo, ThresholdType.VALUE );
            ThresholdOuter thresholdThreeWrapped = ThresholdOuter.of( thresholdThree,
                                                                      ThresholdType.PROBABILITY_CLASSIFIER );

            FeatureTuple featureOne = FeatureTuple.of( geoTupleOne );
            FeatureTuple featureTwo = FeatureTuple.of( geoTupleTwo );
            Map<FeatureTuple, Set<ThresholdOuter>> thresholdsByFeature = Map.of( featureOne,
                                                                                 Set.of( thresholdOneWrapped,
                                                                                         thresholdThreeWrapped ),
                                                                                 featureTwo,
                                                                                 Set.of( thresholdTwoWrapped,
                                                                                         thresholdThreeWrapped ) );

            MetricsAndThresholds metricsAndThresholds = new MetricsAndThresholds( metrics,
                                                                                  thresholdsByFeature,
                                                                                  0,
                                                                                  Pool.EnsembleAverageType.MEAN );

            List<MetricsAndThresholds> metricsAndThresholdsList = List.of( metricsAndThresholds );

            // Create the blobs to write
            writer.createBlobsForWriting( featureGroups, metricsAndThresholdsList );

            // Create the statistics to write
            DoubleScoreMetric.DoubleScoreMetricComponent
                    metricComponent = DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                                                  .setName( DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName.MAIN )
                                                                                  .build();
            DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                        .addComponents( metricComponent )
                                                        .setName( MetricName.PROBABILITY_OF_FALSE_DETECTION )
                                                        .build();

            DoubleScoreStatistic.DoubleScoreStatisticComponent
                    component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                  .setMetric( metricComponent )
                                                                                  .setValue( 0.5 )
                                                                                  .build();
            DoubleScoreStatistic statistic = DoubleScoreStatistic.newBuilder()
                                                                 .setMetric( metric )
                                                                 .addStatistics( component )
                                                                 .build();

            Evaluation evaluation = Evaluation.newBuilder()
                                              .setMeasurementUnit( "CFS" )
                                              .build();
            TimeWindow timeWindowOne =
                    TimeWindow.newBuilder()
                              .setEarliestReferenceTime( Timestamp.newBuilder()
                                                                  .setSeconds( -31557014167219200L ) )
                              .setLatestReferenceTime( Timestamp.newBuilder()
                                                                .setSeconds( 31556889864403199L )
                                                                .setNanos( 999999999 ) )
                              .setEarliestValidTime( Timestamp.newBuilder()
                                                              .setSeconds( -31557014167219200L ) )
                              .setLatestValidTime( Timestamp.newBuilder()
                                                            .setSeconds( 31556889864403199L )
                                                            .setNanos( 999999999 ) )
                              .setEarliestLeadDuration( Duration.newBuilder()
                                                                .setSeconds( 0 ) )
                              .setLatestLeadDuration( Duration.newBuilder()
                                                              .setSeconds( 10_800 ) )
                              .build();
            Pool poolOne = Pool.newBuilder()
                               .setGeometryGroup( geoGroupOne )
                               .setTimeWindow( timeWindowOne )
                               .setEventThreshold( thresholdOne )
                               .setDecisionThreshold( thresholdThree )
                               .build();
            PoolMetadata metadataOne = PoolMetadata.of( evaluation, poolOne );

            Pool poolTwo = Pool.newBuilder()
                               .setGeometryGroup( geoGroupTwo )
                               .setTimeWindow( timeWindowOne.toBuilder()
                                                            .setEarliestLeadDuration( Duration.newBuilder()
                                                                                              .setSeconds( 10_800 ) )
                                                            .setLatestLeadDuration( Duration.newBuilder()
                                                                                            .setSeconds( 21_600 ) ) )
                               .setEventThreshold( thresholdTwo )
                               .setDecisionThreshold( thresholdThree )
                               .build();
            PoolMetadata metadataTwo = PoolMetadata.of( evaluation, poolTwo );

            DoubleScoreStatisticOuter outerOne = DoubleScoreStatisticOuter.of( statistic, metadataOne );
            DoubleScoreStatisticOuter outerTwo = DoubleScoreStatisticOuter.of( statistic, metadataTwo );
            List<DoubleScoreStatisticOuter> statistics = List.of( outerOne, outerTwo );

            // Attempt to write statistics to the blobs, which should not throw an exception
            Assertions.assertDoesNotThrow( () -> writer.apply( statistics ) );
        }
    }
}
