package wres.writing.netcdf;

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
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
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
        EvaluationDeclaration declaration = Mockito.mock( EvaluationDeclaration.class );

        Set<TimePools> pools = Collections.singleton( TimePoolsBuilder.builder()
                                                                      .period( java.time.Duration.ofHours( 3 ) )
                                                                      .frequency( java.time.Duration.ofHours( 3 ) )
                                                                      .build() );
        LeadTimeInterval leadHours = new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                           java.time.Duration.ofHours( 6 ) );
        Mockito.when( declaration.leadTimePools() )
               .thenReturn( pools );
        Mockito.when( declaration.leadTimes() )
               .thenReturn( leadHours );

        Dataset leftSources = Mockito.mock( Dataset.class );
        Source leftSource = Mockito.mock( Source.class );
        Mockito.when( leftSource.sourceInterface() )
               .thenReturn( SourceInterface.WRDS_NWM );
        Mockito.when( leftSources.sources() )
               .thenReturn( List.of( leftSource ) );

        Dataset rightSources = Mockito.mock( Dataset.class );
        Source rightSource = Mockito.mock( Source.class );
        Mockito.when( rightSource.sourceInterface() )
               .thenReturn( SourceInterface.USGS_NWIS );
        Mockito.when( rightSources.sources() )
               .thenReturn( List.of( rightSource ) );
        Mockito.when( rightSources.type() )
               .thenReturn( DataType.ENSEMBLE_FORECASTS );

        Mockito.when( declaration.left() )
               .thenReturn( leftSources );
        Mockito.when( declaration.right() )
               .thenReturn( rightSources );

        Formats formats = Mockito.mock( Formats.class );
        Mockito.when( declaration.formats() )
               .thenReturn( formats );
        Mockito.when( formats.outputs() )
               .thenReturn( Outputs.newBuilder()
                                   .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
                                   .build() );

        ChronoUnit durationUnits = ChronoUnit.HOURS;

        try ( NetcdfOutputWriter writer = NetcdfOutputWriter.of( systemSettings,
                                                                 declaration,
                                                                 durationUnits,
                                                                 output ) )
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
                                              .setLeftThresholdValue( 6440.34 )
                                              .setThresholdValueUnits( "CFS" )
                                              .setOperator( Threshold.ThresholdOperator.GREATER )
                                              .setName( "1_5_year_recurrence_flow" )
                                              .build();

            Threshold thresholdTwo = Threshold.newBuilder()
                                              .setLeftThresholdValue( 295.94 )
                                              .setThresholdValueUnits( "CFS" )
                                              .setOperator( Threshold.ThresholdOperator.GREATER )
                                              .setName( "1_5_year_recurrence_flow" )
                                              .build();

            Threshold thresholdThree = Threshold.newBuilder()
                                                .setLeftThresholdValue( 0.5 )
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

            Set<MetricsAndThresholds> metricsAndThresholdsList = Set.of( metricsAndThresholds );

            // Create the blobs to write
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

            TimeWindow timeWindowTwo = timeWindowOne.toBuilder()
                                                    .setEarliestLeadDuration( Duration.newBuilder()
                                                                                      .setSeconds( 10_800 ) )
                                                    .setLatestLeadDuration( Duration.newBuilder()
                                                                                    .setSeconds( 21_600 ) )
                                                    .build();

            writer.createBlobsForWriting( featureGroups,
                                          metricsAndThresholdsList,
                                          Set.of( TimeWindowOuter.of( timeWindowOne ),
                                                  TimeWindowOuter.of( timeWindowTwo ) ) );

            // Create the statistics to write
            DoubleScoreMetric.DoubleScoreMetricComponent
                    metricComponent = DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                                                  .setName( MetricName.MAIN )
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

            Pool poolOne = Pool.newBuilder()
                               .setGeometryGroup( geoGroupOne )
                               .setTimeWindow( timeWindowOne )
                               .setEventThreshold( thresholdOne )
                               .setDecisionThreshold( thresholdThree )
                               .build();
            PoolMetadata metadataOne = PoolMetadata.of( evaluation, poolOne );

            Pool poolTwo = Pool.newBuilder()
                               .setGeometryGroup( geoGroupTwo )
                               .setTimeWindow( timeWindowTwo )
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
