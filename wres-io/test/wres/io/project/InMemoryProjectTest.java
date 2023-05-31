package wres.io.project;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DoubleValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.AnalysisTimesBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.TimeScaleBuilder;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.VariableBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.Feature;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesStore;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link InMemoryProject}.
 * @author James Brown
 */
class InMemoryProjectTest
{
    /** Geographic feature name. */
    private static final String FEATURE_NAME = "09165000";

    /** Instance to test. */
    private Project testProject;

    /** Declaration to use. */
    private EvaluationDeclaration declaration;

    /** Left-ish dataset. */
    private Dataset leftDataset;

    /** Right-ish dataset. */
    private Dataset rightDataset;

    /** Baseline-ish dataset. */
    private Dataset baselineDataset;

    @BeforeEach
    void runBeforeEachTest()
    {
        Map<ReferenceTime.ReferenceTimeType, Instant> refTime = Map.of( ReferenceTime.ReferenceTimeType.T0,
                                                                        Instant.parse( "2037-12-23T00:00:00Z" ) );
        Feature feature = Feature.of( Geometry.newBuilder()
                                              .setName( FEATURE_NAME )
                                              .build() );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( refTime,
                                                             TimeScaleOuter.of(),
                                                             "bat",
                                                             feature,
                                                             "unit" );
        TimeSeries<Double> timeSeries
                = new TimeSeries.Builder<Double>().setMetadata( metadata )
                                                  .addEvent( Event.of( Instant.parse( "2037-12-23T06:00:00Z" ), 1.0 ) )
                                                  .addEvent( Event.of( Instant.parse( "2037-12-23T12:00:00Z" ), 2.0 ) )
                                                  .addEvent( Event.of( Instant.parse( "2037-12-23T18:00:00Z" ), 3.0 ) )
                                                  .build();
        TimeSeriesStore store = new TimeSeriesStore.Builder().addSingleValuedSeries( timeSeries,
                                                                                     DatasetOrientation.LEFT )
                                                             .addSingleValuedSeries( timeSeries,
                                                                                     DatasetOrientation.RIGHT )
                                                             .addSingleValuedSeries( timeSeries,
                                                                                     DatasetOrientation.BASELINE )
                                                             .build();

        URI fakeLeftUri = URI.create( "https://foo.bar" );
        Source fakeLeftSource = SourceBuilder.builder()
                                             .uri( fakeLeftUri )
                                             .build();
        this.leftDataset = DatasetBuilder.builder()
                                         .sources( List.of( fakeLeftSource ) )
                                         .variable( VariableBuilder.builder()
                                                                   .name( "bat" )
                                                                   .build() )
                                         .build();
        URI fakeRightUri = URI.create( "https://bar.baz" );
        Source fakeRightSource = SourceBuilder.builder()
                                              .uri( fakeRightUri )
                                              .build();
        this.rightDataset = DatasetBuilder.builder()
                                          .sources( List.of( fakeRightSource ) )
                                          .variable( VariableBuilder.builder()
                                                                    .name( "bat" )
                                                                    .build() )
                                          .build();
        URI fakeBaselineUri = URI.create( "https://baz.qux" );
        Source fakeBaselineSource = SourceBuilder.builder()
                                                 .uri( fakeBaselineUri )
                                                 .build();
        this.baselineDataset = DatasetBuilder.builder()
                                             .sources( List.of( fakeBaselineSource ) )
                                             .variable( VariableBuilder.builder()
                                                                       .name( "bat" )
                                                                       .build() )
                                             .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .persistence( 1 )
                                                         .build();

        LeadTimeInterval leadTimes = LeadTimeIntervalBuilder.builder()
                                                            .minimum( Duration.ofHours( 0 ) )
                                                            .maximum( Duration.ofHours( 18 ) )
                                                            .build();
        TimeInterval validDates = TimeIntervalBuilder.builder()
                                                     .minimum( Instant.parse( "2000-01-01T00:00:00Z" ) )
                                                     .maximum( Instant.parse( "2040-01-01T00:00:00Z" ) )
                                                     .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( Duration.ofHours( 13 ) )
                                                       .frequency( Duration.ofHours( 7 ) )
                                                       .build();
        Set<GeometryTuple> geometries = Set.of( GeometryTuple.newBuilder()
                                                             .setLeft( Geometry.newBuilder().setName( FEATURE_NAME ) )
                                                             .setRight( Geometry.newBuilder().setName( FEATURE_NAME ) )
                                                             .setBaseline( Geometry.newBuilder()
                                                                                   .setName( FEATURE_NAME ) )
                                                             .build() );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();

        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                               .setSeconds( 1200 )
                                                                               .build() )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();

        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .minimum( Duration.ofHours( 1 ) )
                                                          .maximum( Duration.ofHours( 3 ) )
                                                          .build();
        Season season = SeasonBuilder.builder()
                                     .minimum( MonthDay.of( 12, 2 ) )
                                     .maximum( MonthDay.of( 2, 4 ) )
                                     .build();
        wres.statistics.generated.Threshold probThreshold =
                wres.statistics.generated.Threshold.newBuilder()
                                                   .setLeftThresholdValue( DoubleValue.of( 0.4 ) )
                                                   .build();
        Set<Threshold> thresholds =
                Set.of( ThresholdBuilder.builder()
                                        .threshold( probThreshold )
                                        .build() );

        this.declaration = EvaluationDeclarationBuilder.builder()
                                                       .unit( "moon" )
                                                       .left( leftDataset )
                                                       .right( rightDataset )
                                                       .baseline( baseline )
                                                       .leadTimes( leadTimes )
                                                       .validDates( validDates )
                                                       .referenceDatePools( referenceTimePools )
                                                       .features( features )
                                                       .probabilityThresholds( thresholds )
                                                       .rescaleLenience( TimeScaleLenience.RIGHT )
                                                       .analysisTimes( analysisTimes )
                                                       .timeScale( TimeScaleBuilder.builder()
                                                                                   .timeScale( timeScale )
                                                                                   .build() )
                                                       .season( season )
                                                       .build();

        this.testProject = new InMemoryProject( declaration, store, List.of() );
    }

    @Test
    void testGetDeclaration()
    {
        assertEquals( this.declaration, this.testProject.getDeclaration() );
    }

    @Test
    void testGetMeasurementUnit()
    {
        assertEquals( "moon", this.testProject.getMeasurementUnit() );
    }

    @Test
    void testGetDesiredTimeScale()
    {
        assertEquals( "moon", this.testProject.getMeasurementUnit() );
    }

    @Test
    void testGetTimeScale()
    {
        TimeScale expectedInner = TimeScale.newBuilder()
                                           .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                                   .setSeconds( 1200 )
                                                                                   .build() )
                                           .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                           .build();
        TimeScaleOuter expected = TimeScaleOuter.of( expectedInner );
        assertEquals( expected, this.testProject.getDesiredTimeScale() );
    }

    @Test
    void testGetFeatures()
    {
        GeometryTuple expectedInner = GeometryTuple.newBuilder()
                                                   .setLeft( Geometry.newBuilder().setName( FEATURE_NAME ) )
                                                   .setRight( Geometry.newBuilder().setName( FEATURE_NAME ) )
                                                   .setBaseline( Geometry.newBuilder().setName( FEATURE_NAME ) )
                                                   .build();
        Set<FeatureTuple> expected = Set.of( FeatureTuple.of( expectedInner ) );
        assertEquals( expected, this.testProject.getFeatures() );
    }

    @Test
    void testGetDeclaredDataset()
    {
        assertAll( () -> assertEquals( this.leftDataset,
                                       this.testProject.getDeclaredDataset( DatasetOrientation.LEFT ) ),
                   () -> assertEquals( this.rightDataset,
                                       this.testProject.getDeclaredDataset( DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( this.baselineDataset,
                                       this.testProject.getDeclaredDataset( DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testIsUpscalingLenient()
    {
        assertAll( () -> assertTrue( this.testProject.isUpscalingLenient( DatasetOrientation.RIGHT ) ),
                   () -> assertFalse( this.testProject.isUpscalingLenient( DatasetOrientation.LEFT ) ),
                   () -> assertFalse( this.testProject.isUpscalingLenient( DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testGetVariableName()
    {
        assertAll( () -> assertEquals( "bat",
                                       this.testProject.getVariableName( DatasetOrientation.LEFT ) ),
                   () -> assertEquals( "bat",
                                       this.testProject.getVariableName( DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( "bat",
                                       this.testProject.getVariableName( DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testGetEarliestAnalysisDuration()
    {
        assertEquals( Duration.ofHours( 1 ), this.testProject.getEarliestAnalysisDuration() );
    }

    @Test
    void testGetLatestAnalysisDuration()
    {
        assertEquals( Duration.ofHours( 3 ), this.testProject.getLatestAnalysisDuration() );
    }

    @Test
    void testGetStartOfSeason()
    {
        assertEquals( MonthDay.of( 12, 2 ), this.testProject.getStartOfSeason() );
    }

    @Test
    void testGetEndOfSeason()
    {
        assertEquals( MonthDay.of( 2, 4 ), this.testProject.getEndOfSeason() );
    }

    @Test
    void testGetUsesGriddedData()
    {
        assertAll( () -> assertFalse( this.testProject.usesGriddedData( DatasetOrientation.LEFT ) ),
                   () -> assertFalse( this.testProject.usesGriddedData( DatasetOrientation.RIGHT ) ),
                   () -> assertFalse( this.testProject.usesGriddedData( DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testHasBaseline()
    {
        assertTrue( this.testProject.hasBaseline() );
    }

    @Test
    void testHasGeneratedBaseline()
    {
        assertTrue( this.testProject.hasGeneratedBaseline() );
    }

    @Test
    void testHasProbabilityThresholds()
    {
        assertTrue( this.testProject.hasProbabilityThresholds() );
    }

    @Test
    void testSave()
    {
        assertTrue( this.testProject.save() );
    }

    @Test
    void testGetId()
    {
        assertTrue( this.testProject.getId() >= 0 );
    }
}
