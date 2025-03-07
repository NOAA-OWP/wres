package wres.config.yaml;

import java.math.BigInteger;
import java.net.URI;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.MonthDay;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.xml.generated.DataSourceBaselineConfig;
import wres.config.xml.generated.DataSourceConfig;
import wres.config.xml.generated.DateCondition;
import wres.config.xml.generated.DesiredTimeScaleConfig;
import wres.config.xml.generated.DestinationConfig;
import wres.config.xml.generated.DestinationType;
import wres.config.xml.generated.DoubleBoundsType;
import wres.config.xml.generated.DurationBoundsType;
import wres.config.xml.generated.DurationUnit;
import wres.config.xml.generated.EnsembleAverageType;
import wres.config.xml.generated.EnsembleCondition;
import wres.config.xml.generated.FeatureDimension;
import wres.config.xml.generated.FeatureGroup;
import wres.config.xml.generated.FeaturePool;
import wres.config.xml.generated.GraphicalType;
import wres.config.xml.generated.IntBoundsType;
import wres.config.xml.generated.LeftOrRightOrBaseline;
import wres.config.xml.generated.LenienceType;
import wres.config.xml.generated.MetricConfig;
import wres.config.xml.generated.MetricConfigName;
import wres.config.xml.generated.MetricsConfig;
import wres.config.xml.generated.NamedFeature;
import wres.config.xml.generated.NetcdfType;
import wres.config.xml.generated.OutputTypeSelection;
import wres.config.xml.generated.PairConfig;
import wres.config.xml.generated.Polygon;
import wres.config.xml.generated.PoolingWindowConfig;
import wres.config.xml.generated.ProjectConfig;
import wres.config.xml.generated.ThresholdDataType;
import wres.config.xml.generated.ThresholdFormat;
import wres.config.xml.generated.ThresholdOperator;
import wres.config.xml.generated.ThresholdsConfig;
import wres.config.xml.generated.TimeScaleConfig;
import wres.config.xml.generated.TimeScaleFunction;
import wres.config.xml.generated.UnnamedFeature;
import wres.config.xml.generated.UrlParameter;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.AnalysisTimesBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.CrossPairMethod;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EnsembleFilterBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroupBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeIntervalBuilder;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.config.yaml.components.TimeScaleBuilder;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.UnitAliasBuilder;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.ValuesBuilder;
import wres.config.yaml.components.VariableBuilder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link DeclarationMigrator}.
 * @author James Brown
 */
class DeclarationMigratorTest
{
    /** A predicted source for re-use. */
    private Source predictedSource;
    /** An observed dataset for re-use. */
    private Dataset observedDataset;
    /** A predicted dataset for re-use. */
    private Dataset predictedDataset;
    /** Re-used feature string. */
    private static final String DRRC2 = "DRRC2";
    /** Re-used feature string. */
    private static final String DOLC2 = "DOLC2";
    /** Default list of observed sources in the old-style declaration. */
    List<DataSourceConfig.Source> observedSources;
    /** Default list of predicted sources in the old-style declaration. */
    List<DataSourceConfig.Source> predictedSources;
    /** Default list of baseline sources in the old-style declaration. */
    List<DataSourceConfig.Source> baselineSources;
    /** A default set of datasets in the old-style declaration. */
    private ProjectConfig.Inputs inputs;
    /** A default set of metrics in the old-style declaration. */
    private List<MetricsConfig> metrics;
    /** A default set of outputs in the old-style declaration. */
    private ProjectConfig.Outputs outputs;
    /** Default pairs in the old-style declaration. */
    private PairConfig pairs;

    @BeforeEach
    void runBeforeEach()
    {
        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "another_file.csv" );
        this.predictedSource = SourceBuilder.builder()
                                            .uri( predictedUri )
                                            .build();

        List<Source> observedSources = List.of( observedSource );
        this.observedDataset = DatasetBuilder.builder()
                                             .sources( observedSources )
                                             .build();

        List<Source> predictedSources = List.of( predictedSource );
        this.predictedDataset = DatasetBuilder.builder()
                                              .sources( predictedSources )
                                              .build();

        DataSourceConfig.Source observedDataSource = new DataSourceConfig.Source( observedUri,
                                                                                  null,
                                                                                  null,
                                                                                  null,
                                                                                  null );

        DataSourceConfig.Source predictedDataSource = new DataSourceConfig.Source( predictedUri,
                                                                                   null,
                                                                                   null,
                                                                                   null,
                                                                                   null );

        this.observedSources = List.of( observedDataSource );
        this.predictedSources = List.of( predictedDataSource );
        this.baselineSources = List.of( predictedDataSource );

        this.inputs = new ProjectConfig.Inputs( new DataSourceConfig( null,
                                                                      List.of( observedDataSource ),
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null ),
                                                new DataSourceConfig( null,
                                                                      List.of( predictedDataSource ),
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      null ),
                                                null );

        this.pairs = new PairConfig( null,
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
                                     null,
                                     null,
                                     null,
                                     null );

        MetricsConfig defaultMetrics = new MetricsConfig( null, null, null, null, null );
        this.metrics = List.of( defaultMetrics );

        this.outputs = new ProjectConfig.Outputs( null, null );
    }

    @Test
    void testMigrateProjectWithShortSources()
    {
        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithLongSources()
    {
        EnsembleCondition ensembleCondition = new EnsembleCondition( null, "baz", false );
        DataSourceConfig left = new DataSourceConfig( null,
                                                      this.observedSources,
                                                      new DataSourceConfig.Variable( "foo", "fooest" ),
                                                      null,
                                                      null,
                                                      List.of( ensembleCondition ),
                                                      null,
                                                      null,
                                                      null,
                                                      null,
                                                      null );

        DataSourceConfig right = new DataSourceConfig( null,
                                                       this.predictedSources,
                                                       null,
                                                       null,
                                                       null,
                                                       null,
                                                       new DataSourceConfig.TimeShift( 2, DurationUnit.HOURS ),
                                                       new TimeScaleConfig( TimeScaleFunction.MEAN,
                                                                            3,
                                                                            DurationUnit.HOURS,
                                                                            null ),
                                                       List.of( new UrlParameter( "moon", "bat" ) ),
                                                       null,
                                                       FeatureDimension.NWS_LID );

        DataSourceBaselineConfig baseline = new DataSourceBaselineConfig( null,
                                                                          this.predictedSources,
                                                                          null,
                                                                          null,
                                                                          2,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          false );

        ProjectConfig.Inputs longSources = new ProjectConfig.Inputs( left, right, baseline );

        ProjectConfig project = new ProjectConfig( longSources,
                                                   this.pairs,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );


        Dataset observedLong = DatasetBuilder.builder( this.observedDataset )
                                             .variable( VariableBuilder.builder()
                                                                       .label( "fooest" )
                                                                       .name( "foo" )
                                                                       .build() )
                                             .ensembleFilter( EnsembleFilterBuilder.builder()
                                                                                   .members( Set.of( "baz" ) )
                                                                                   .exclude( false )
                                                                                   .build() )
                                             .build();

        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder()
                                                           .setSeconds( 10_800 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();

        Source predictedSourceLong = SourceBuilder.builder( this.predictedSource )
                                                  .parameters( Map.of( "moon", "bat" ) )
                                                  .build();

        Dataset predictedLong = DatasetBuilder.builder()
                                              .sources( List.of( predictedSourceLong ) )
                                              .timeShift( java.time.Duration.ofHours( 2 ) )
                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                              .timeScale( TimeScaleBuilder.builder()
                                                                          .timeScale( timeScale )
                                                                          .build() )
                                              .build();

        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .order( 2 )
                                                                .build();
        BaselineDataset baselineLong = BaselineDatasetBuilder.builder()
                                                             .dataset( this.predictedDataset )
                                                             .generatedBaseline( persistence )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( observedLong )
                                                                     .right( predictedLong )
                                                                     .baseline( baselineLong )
                                                                     .build();

        assertEquals( expected, actual );
    }

    /**
     * Redmine issue #116312.
     */

    @Test
    void testMigrateProjectWithSeparateMetricsForBaseline()
    {
        DataSourceBaselineConfig baseline = new DataSourceBaselineConfig( null,
                                                                          this.predictedSources,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          null,
                                                                          true );

        ProjectConfig.Inputs sources = new ProjectConfig.Inputs( null, null, baseline );

        ProjectConfig project = new ProjectConfig( sources,
                                                   this.pairs,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        assertTrue( actual.baseline()
                          .separateMetrics() );
    }

    @Test
    void testMigrateProjectWithPairOptions() throws ParseException
    {
        wres.config.xml.generated.FeatureService featureService =
                new wres.config.xml.generated.FeatureService( URI.create( "https://moo" ),
                                                              List.of( new FeatureGroup( "small", "ball", false ) ) );
        List<wres.config.xml.generated.UnitAlias> aliases =
                List.of( new wres.config.xml.generated.UnitAlias( "banana", "pear" ) );
        List<NamedFeature> features = List.of( new NamedFeature( "red", "blue", "green" ) );
        List<FeaturePool> featureGroup = List.of( new FeaturePool( features, "groupish" ) );
        Polygon polygon = new Polygon( List.of( new Polygon.Point( 2.0F, 1.0F ),
                                                new Polygon.Point( 4.0F, 3.0F ),
                                                new Polygon.Point( 6.0F, 5.0F ) ),
                                       BigInteger.valueOf( 4326 ) );
        List<UnnamedFeature> grid = List.of( new UnnamedFeature( null, polygon, null ) );
        DesiredTimeScaleConfig timeScale = new DesiredTimeScaleConfig( TimeScaleFunction.MEAN,
                                                                       3,
                                                                       DurationUnit.HOURS,
                                                                       null,
                                                                       7,
                                                                       null,
                                                                       null,
                                                                       null,
                                                                       null,
                                                                       LenienceType.RIGHT );

        PoolingWindowConfig leadTimesPoolingWindowConfig =
                new PoolingWindowConfig( 24, null, DurationUnit.HOURS );
        PoolingWindowConfig issuedDatesPoolingWindowConfig =
                new PoolingWindowConfig( 13, 7, DurationUnit.HOURS );
        PoolingWindowConfig validDatesPoolingWindowConfig =
                new PoolingWindowConfig( 17, 4, DurationUnit.HOURS );
        PairConfig pairOptions = new PairConfig( "dogfish",
                                                 aliases,
                                                 featureService,
                                                 features,
                                                 featureGroup,
                                                 grid,
                                                 new IntBoundsType( 1, 3 ),
                                                 new DurationBoundsType( 3, 5, DurationUnit.HOURS ),
                                                 new DateCondition( "2096-12-01T00:00:00Z", "2097-12-01T00:00:00Z" ),
                                                 new DateCondition( "2093-12-01T00:00:00Z", "2094-12-01T00:00:00Z" ),
                                                 new PairConfig.Season( ( short ) 1,
                                                                        ( short ) 1,
                                                                        ( short ) 2,
                                                                        ( short ) 2 ),
                                                 new DoubleBoundsType( -1.0, 3.0, 7.0, 9.0 ),
                                                 timeScale,
                                                 issuedDatesPoolingWindowConfig,
                                                 validDatesPoolingWindowConfig,
                                                 leadTimesPoolingWindowConfig,
                                                 new wres.config.xml.generated.CrossPair( false ),
                                                 null );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   pairOptions,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        // Create the expected outcome
        FeatureService featureServiceEx
                = FeatureServiceBuilder.builder()
                                       .uri( URI.create( "https://moo" ) )
                                       .featureGroups( Set.of( FeatureServiceGroupBuilder.builder()
                                                                                         .group( "small" )
                                                                                         .value( "ball" )
                                                                                         .build() ) )
                                       .build();
        Set<UnitAlias> unitAliases = Set.of( UnitAliasBuilder.builder()
                                                             .unit( "pear" )
                                                             .alias( "banana" )
                                                             .build() );
        GeometryTuple geometry = GeometryTuple.newBuilder()
                                              .setLeft( Geometry.newBuilder()
                                                                .setName( "red" ) )
                                              .setRight( Geometry.newBuilder()
                                                                 .setName( "blue" ) )
                                              .setBaseline( Geometry.newBuilder()
                                                                    .setName( "green" ) )
                                              .build();
        Set<GeometryTuple> geometries = Set.of( geometry );
        Features featuresEx = FeaturesBuilder.builder()
                                             .geometries( geometries )
                                             .build();
        GeometryGroup geoGroup = GeometryGroup.newBuilder()
                                              .addAllGeometryTuples( geometries )
                                              .setRegionName( "groupish" )
                                              .build();
        Set<GeometryGroup> groups = Set.of( geoGroup );
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( groups )
                                                          .build();

        String wkt = "POLYGON ((1 2, 3 4, 5 6, 1 2))";
        WKTReader reader = new WKTReader();
        org.locationtech.jts.geom.Geometry mask = reader.read( wkt );
        mask.setSRID( 4326 );

        SpatialMask spatialMask = SpatialMaskBuilder.builder()
                                                    .geometry( mask )
                                                    .build();
        TimeScale innerTimeScale = TimeScale.newBuilder()
                                            .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                            .setPeriod( Duration.newBuilder()
                                                                .setSeconds( 10800 )
                                                                .build() )
                                            .build();
        wres.config.yaml.components.TimeScale timeScaleEx = TimeScaleBuilder.builder()
                                                                            .timeScale( innerTimeScale )
                                                                            .build();
        LeadTimeInterval leadTimeInterval = LeadTimeIntervalBuilder.builder()
                                                                   .minimum( java.time.Duration.ofHours( 1 ) )
                                                                   .maximum( java.time.Duration.ofHours( 3 ) )
                                                                   .build();
        TimeInterval validTimeInterval = TimeIntervalBuilder.builder()
                                                            .minimum( Instant.parse( "2096-12-01T00:00:00Z" ) )
                                                            .maximum( Instant.parse( "2097-12-01T00:00:00Z" ) )
                                                            .build();
        TimeInterval referenceTimeInterval = TimeIntervalBuilder.builder()
                                                                .minimum( Instant.parse( "2093-12-01T00:00:00Z" ) )
                                                                .maximum( Instant.parse( "2094-12-01T00:00:00Z" ) )
                                                                .build();
        AnalysisTimes analysisTimes =
                AnalysisTimesBuilder.builder()
                                    .minimum( java.time.Duration.ofHours( 4 ) )
                                    .maximum( java.time.Duration.ofHours( 5 ) )
                                    .build();
        TimePools validTimePools = TimePoolsBuilder.builder()
                                                   .period( java.time.Duration.ofHours( 17 ) )
                                                   .frequency( java.time.Duration.ofHours( 4 ) )
                                                   .build();
        TimePools referenceTimePools = TimePoolsBuilder.builder()
                                                       .period( java.time.Duration.ofHours( 13 ) )
                                                       .frequency( java.time.Duration.ofHours( 7 ) )
                                                       .build();
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( java.time.Duration.ofHours( 24 ) )
                                                  .build();
        Season season = SeasonBuilder.builder()
                                     .minimum( MonthDay.of( 1, 1 ) )
                                     .maximum( MonthDay.of( 2, 2 ) )
                                     .build();

        Values values = ValuesBuilder.builder()
                                     .minimum( -1.0 )
                                     .maximum( 3.0 )
                                     .belowMinimum( 7.0 )
                                     .aboveMaximum( 9.0 )
                                     .build();

        EvaluationDeclaration expected =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .featureService( featureServiceEx )
                                            .features( featuresEx )
                                            .featureGroups( featureGroups )
                                            .unitAliases( unitAliases )
                                            .spatialMask( spatialMask )
                                            .timeScale( timeScaleEx )
                                            .pairFrequency( java.time.Duration.ofHours( 7 ) )
                                            .rescaleLenience( TimeScaleLenience.RIGHT )
                                            .leadTimes( leadTimeInterval )
                                            .validDates( validTimeInterval )
                                            .analysisTimes( analysisTimes )
                                            .referenceDates( referenceTimeInterval )
                                            .validDatePools( Collections.singleton( validTimePools ) )
                                            .referenceDatePools( Collections.singleton( referenceTimePools ) )
                                            .leadTimePools( Collections.singleton( leadTimePools ) )
                                            .season( season )
                                            .values( values )
                                            .crossPair( new CrossPair( CrossPairMethod.FUZZY,
                                                                       null ) )
                                            .unit( "dogfish" )
                                            .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithTwoGroupsOfMetricsAndThresholds()
    {
        // First group of metrics with thresholds
        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ),
                                                  new MetricConfig( null, MetricConfigName.SAMPLE_SIZE ) );
        ThresholdsConfig someThresholds = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.PROBABILITY,
                                                                ThresholdDataType.LEFT, "0.1",
                                                                ThresholdOperator.GREATER_THAN );
        List<ThresholdsConfig> thresholdSets = List.of( someThresholds );
        MetricsConfig someMetricsWrapped = new MetricsConfig( thresholdSets, null, someMetrics, null, null );

        // Second group of metrics with thresholds
        List<MetricConfig> moreMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        ThresholdsConfig moreThresholds = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.PROBABILITY,
                                                                ThresholdDataType.LEFT, "0.2",
                                                                ThresholdOperator.GREATER_THAN );
        List<ThresholdsConfig> moreThresholdsSets = List.of( moreThresholds );
        MetricsConfig moreMetricsWrapped = new MetricsConfig( moreThresholdsSets, null, moreMetrics, null, null );

        // Add the two groups together
        List<MetricsConfig> innerMetrics = List.of( someMetricsWrapped, moreMetricsWrapped );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( 0.1 )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> someExpectedThresholds = Set.of( pOneWrapped );

        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                        .build();

        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.SAMPLE_SIZE )
                                        .build();

        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( 0.2 )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();

        wres.config.yaml.components.Threshold pTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pTwo )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> someMoreExpectedThresholds = Set.of( pTwoWrapped );
        MetricParameters someMoreParameters = MetricParametersBuilder.builder()
                                                                     .probabilityThresholds( someMoreExpectedThresholds )
                                                                     .build();

        Metric metricThree = MetricBuilder.builder()
                                          .name( MetricConstants.MEAN_ERROR )
                                          .parameters( someMoreParameters )
                                          .build();

        Set<Metric> expectedMetrics = Set.of( metricOne, metricTwo, metricThree );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( expectedMetrics )
                                                                     .probabilityThresholds( someExpectedThresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithFormats()
    {
        String decimalFormat = "0.00";
        GraphicalType graphicalType = new GraphicalType( null, 500, 300 );
        DestinationConfig one = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                       graphicalType,
                                                       null,
                                                       DestinationType.PNG,
                                                       null );
        DestinationConfig two = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                       null,
                                                       null,
                                                       DestinationType.CSV,
                                                       decimalFormat );
        DestinationConfig three = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                         null,
                                                         null,
                                                         DestinationType.CSV2,
                                                         decimalFormat );
        DestinationConfig four = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                        null,
                                                        null,
                                                        DestinationType.NETCDF,
                                                        null );
        DestinationConfig five = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                        null,
                                                        null,
                                                        DestinationType.NETCDF_2,
                                                        null );
        DestinationConfig six = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                       null,
                                                       null,
                                                       DestinationType.PROTOBUF,
                                                       null );
        DestinationConfig seven = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                         null,
                                                         null,
                                                         DestinationType.PAIRS,
                                                         decimalFormat );
        DestinationConfig eight = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                         graphicalType,
                                                         null,
                                                         DestinationType.SVG,
                                                         null );
        List<DestinationConfig> destinations = List.of( one, two, three, four, five, six, seven, eight );
        ProjectConfig.Outputs innerOutputs = new ProjectConfig.Outputs( destinations, DurationUnit.HOURS );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   this.metrics,
                                                   innerOutputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        Outputs.NumericFormat numericFormat = Outputs.NumericFormat.newBuilder()
                                                                   .setDecimalFormat( "#0.00" )
                                                                   .build();
        Outputs.GraphicFormat graphicFormat = Outputs.GraphicFormat.newBuilder()
                                                                   .setWidth( 500 )
                                                                   .setHeight( 300 )
                                                                   .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD )
                                                                   .build();
        Outputs formats = Outputs.newBuilder()
                                 .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
                                 .setNetcdf( Formats.NETCDF_FORMAT )
                                 .setPairs( Outputs.PairFormat.newBuilder()
                                                              .setOptions( numericFormat )
                                                              .build() )
                                 .setCsv2( Outputs.Csv2Format.newBuilder()
                                                             .setOptions( numericFormat )
                                                             .build() )
                                 .setCsv( Outputs.CsvFormat.newBuilder()
                                                           .setOptions( numericFormat )
                                                           .build() )
                                 .setPng( Outputs.PngFormat.newBuilder()
                                                           .setOptions( graphicFormat )
                                                           .build() )
                                 .setSvg( Outputs.SvgFormat.newBuilder()
                                                           .setOptions( graphicFormat )
                                                           .build() )
                                 .setProtobuf( Outputs.ProtobufFormat.getDefaultInstance() )
                                 .build();

        DecimalFormat formatter = new DecimalFormatPretty( "#0.00" );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .durationFormat( ChronoUnit.HOURS )
                                                                     .formats( new Formats( formats ) )
                                                                     .decimalFormat( formatter )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithFormatsAndMetricsToExclude()
    {
        List<MetricConfigName> metricsToExcludeGraphics = List.of( MetricConfigName.MEAN_ABSOLUTE_ERROR,
                                                                   MetricConfigName.PEARSON_CORRELATION_COEFFICIENT );
        GraphicalType graphicalType = new GraphicalType( metricsToExcludeGraphics, 500, 300 );
        DestinationConfig one = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                       graphicalType,
                                                       null,
                                                       DestinationType.PNG,
                                                       null );
        List<DestinationConfig> destinations = List.of( one );
        ProjectConfig.Outputs innerOutputs = new ProjectConfig.Outputs( destinations, DurationUnit.HOURS );

        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ),
                                                  new MetricConfig( null,
                                                                    MetricConfigName.PEARSON_CORRELATION_COEFFICIENT ),
                                                  new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        MetricsConfig someMetricsWrapped = new MetricsConfig( null, null, someMetrics, null, null );

        List<MetricsConfig> innerMetrics = List.of( someMetricsWrapped );
        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   innerOutputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        Outputs.GraphicFormat graphicFormat = Outputs.GraphicFormat.newBuilder()
                                                                   .setWidth( 500 )
                                                                   .setHeight( 300 )
                                                                   .setShape( Outputs.GraphicFormat.GraphicShape.LEAD_THRESHOLD )
                                                                   .build();
        Outputs expectedFormats = Outputs.newBuilder()
                                         .setPng( Outputs.PngFormat.newBuilder()
                                                                   .setOptions( graphicFormat )
                                                                   .build() )
                                         .build();

        MetricParameters omitParameter = MetricParametersBuilder.builder()
                                                                .png( false )
                                                                .build();
        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                        .parameters( omitParameter )
                                        .build();
        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                                        .parameters( omitParameter )
                                        .build();
        Metric metricThree = MetricBuilder.builder()
                                          .name( MetricConstants.MEAN_ERROR )
                                          .build();
        Set<Metric> expectedMetrics = Set.of( metricOne, metricTwo, metricThree );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( expectedMetrics )
                                                                     .formats( new Formats( expectedFormats ) )
                                                                     .durationFormat( ChronoUnit.HOURS )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithSampleSize()
    {
        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        MetricsConfig someMetricsWrapped = new MetricsConfig( null, 14, someMetrics, null, null );
        MetricsConfig moreMetricsWrapped = new MetricsConfig( null, 11, someMetrics, null, null );

        List<MetricsConfig> innerMetrics = List.of( someMetricsWrapped, moreMetricsWrapped );
        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        assertEquals( 14, actual.minimumSampleSize() );
    }

    @Test
    void testMigrateProjectWithFeaturesAndPredictedNameOnly()
    {
        List<NamedFeature> features = List.of( new NamedFeature( null, DRRC2, null ),
                                               new NamedFeature( null, DOLC2, null ) );
        PairConfig pairOptions = new PairConfig( null,
                                                 null,
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

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   pairOptions,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setRight( Geometry.newBuilder().setName( DOLC2 ) )
                                            .build();

        Set<GeometryTuple> geometries = Set.of( first, second );
        Features expectedFeatures = FeaturesBuilder.builder()
                                                   .geometries( geometries )
                                                   .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .features( expectedFeatures )
                                                                     .build();

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithExternalThresholds()
    {
        // First group of metrics with thresholds
        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        URI uriOne = URI.create( "foo/bar.csv" );
        ThresholdsConfig.Source sourceOne = new ThresholdsConfig.Source( uriOne,
                                                                         ThresholdFormat.WRDS,
                                                                         "moondust",
                                                                         "-93.0",
                                                                         "fooAuthority",
                                                                         "fooRater",
                                                                         "bats",
                                                                         LeftOrRightOrBaseline.LEFT );
        ThresholdsConfig someThresholds = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.PROBABILITY,
                                                                ThresholdDataType.LEFT,
                                                                sourceOne,
                                                                ThresholdOperator.GREATER_THAN );
        List<ThresholdsConfig> thresholdSets = List.of( someThresholds );
        MetricsConfig someMetricsWrapped = new MetricsConfig( thresholdSets, null, someMetrics, null, null );

        // Second group of metrics with thresholds
        List<MetricConfig> moreMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        URI uriTwo = URI.create( "baz/qux.csv" );
        ThresholdsConfig.Source sourceTwo = new ThresholdsConfig.Source( uriTwo,
                                                                         ThresholdFormat.CSV,
                                                                         "marsdirt",
                                                                         "-94.0",
                                                                         "barAuthority",
                                                                         "barRater",
                                                                         "cats",
                                                                         LeftOrRightOrBaseline.RIGHT );
        ThresholdsConfig moreThresholds = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.VALUE,
                                                                ThresholdDataType.LEFT_AND_ANY_RIGHT,
                                                                sourceTwo,
                                                                ThresholdOperator.LESS_THAN_OR_EQUAL_TO );
        List<ThresholdsConfig> moreThresholdsSets = List.of( moreThresholds );
        MetricsConfig moreMetricsWrapped = new MetricsConfig( moreThresholdsSets, null, moreMetrics, null, null );

        // Add the two groups together
        List<MetricsConfig> innerMetrics = List.of( someMetricsWrapped, moreMetricsWrapped );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actualEvaluation = DeclarationMigrator.from( project, false );

        Set<ThresholdSource> actual = actualEvaluation.thresholdSources();

        ThresholdSource expectedOne =
                ThresholdSourceBuilder.builder()
                                      .uri( uriOne )
                                      .applyTo( ThresholdOrientation.LEFT )
                                      .featureNameFrom( DatasetOrientation.LEFT )
                                      .missingValue( -93.0 )
                                      .operator( wres.config.yaml.components.ThresholdOperator.GREATER )
                                      .unit( "moondust" )
                                      .parameter( "bats" )
                                      .provider( "fooAuthority" )
                                      .ratingProvider( "fooRater" )
                                      .type( ThresholdType.PROBABILITY )
                                      .build();

        ThresholdSource expectedTwo =
                ThresholdSourceBuilder.builder()
                                      .uri( uriTwo )
                                      .applyTo( ThresholdOrientation.LEFT_AND_ANY_RIGHT )
                                      .featureNameFrom( DatasetOrientation.RIGHT )
                                      .missingValue( -94.0 )
                                      .operator( wres.config.yaml.components.ThresholdOperator.LESS_EQUAL )
                                      .unit( "marsdirt" )
                                      .parameter( "cats" )
                                      .provider( "barAuthority" )
                                      .ratingProvider( "barRater" )
                                      .type( ThresholdType.VALUE )
                                      .build();

        Set<ThresholdSource> expected = Set.of( expectedOne, expectedTwo );

        assertEquals( expected, actual );
    }

    /**
     * Redmine issue #116697.
     */

    @Test
    void testMigrateProjectWithRescaleLenienceTrue()
    {
        DesiredTimeScaleConfig desiredTimeScale = new DesiredTimeScaleConfig( TimeScaleFunction.MEAN,
                                                                              1,
                                                                              DurationUnit.HOURS,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              null,
                                                                              LenienceType.TRUE );
        PairConfig innerPairs = new PairConfig( null,
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
                                                desiredTimeScale,
                                                null,
                                                null,
                                                null,
                                                null,
                                                null );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   innerPairs,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration migrated = DeclarationMigrator.from( project, false );

        TimeScale timeScale = TimeScale.newBuilder().setPeriod( Duration.newBuilder()
                                                                        .setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        wres.config.yaml.components.TimeScale expected = new wres.config.yaml.components.TimeScale( timeScale );
        wres.config.yaml.components.TimeScale actual = migrated.timeScale();
        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithEnsembleAverageTypeDeclared()
    {
        GraphicalType graphicalType = new GraphicalType( List.of(), 500, 300 );
        DestinationConfig one = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                       graphicalType,
                                                       null,
                                                       DestinationType.PNG,
                                                       null );
        List<DestinationConfig> destinations = List.of( one );
        ProjectConfig.Outputs innerOutputs = new ProjectConfig.Outputs( destinations, DurationUnit.HOURS );

        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        MetricsConfig someMetricsWrapped = new MetricsConfig( null,
                                                              null,
                                                              someMetrics,
                                                              null,
                                                              EnsembleAverageType.MEDIAN );

        List<MetricsConfig> innerMetrics = List.of( someMetricsWrapped );
        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   innerOutputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        assertEquals( Pool.EnsembleAverageType.MEDIAN, actual.ensembleAverageType() );
    }

    @Test
    void testMigrateProjectWithLegacyNetcdfFormatOptions()
    {
        // #120069
        NetcdfType netcdfType = new NetcdfType( "foo", "bar", null, null, false );
        DestinationConfig netcdf = new DestinationConfig( OutputTypeSelection.LEAD_THRESHOLD,
                                                          null,
                                                          netcdfType,
                                                          DestinationType.NETCDF,
                                                          null );
        List<DestinationConfig> destinations = List.of( netcdf );
        ProjectConfig.Outputs innerOutputs = new ProjectConfig.Outputs( destinations, DurationUnit.HOURS );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   this.metrics,
                                                   innerOutputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationMigrator.from( project, false );

        Outputs expected = Outputs.newBuilder()
                                  .setNetcdf( Formats.NETCDF_FORMAT.toBuilder()
                                                                   .setTemplatePath( "foo" )
                                                                   .setGridded( false )
                                                                   .setVariableName( "bar" ) )
                                  .build();

        assertEquals( expected, actual.formats()
                                      .outputs() );
    }

    @Test
    void testMigrateProjectWithMultipleSetsOfClassifierThresholds()
    {
        // #120048
        ThresholdsConfig one = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.PROBABILITY_CLASSIFIER,
                                                     ThresholdDataType.LEFT_AND_RIGHT,
                                                     "0.05,0.1",
                                                     ThresholdOperator.LESS_THAN_OR_EQUAL_TO );
        ThresholdsConfig two = new ThresholdsConfig( wres.config.xml.generated.ThresholdType.PROBABILITY_CLASSIFIER,
                                                     ThresholdDataType.ANY_RIGHT,
                                                     "0.05,0.1",
                                                     ThresholdOperator.EQUAL_TO );

        List<ThresholdsConfig> thresholdSets = List.of( one, two );
        List<MetricConfig> metrics = List.of( new MetricConfig( null, MetricConfigName.PROBABILITY_OF_DETECTION ) );
        MetricsConfig metricsConfig = new MetricsConfig( thresholdSets, null, metrics, null, null );
        List<MetricsConfig> innerMetrics = List.of( metricsConfig );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   this.pairs,
                                                   innerMetrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actualEvaluation = DeclarationMigrator.from( project, false );

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( 0.05 )
                                  .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                  .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pOne )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( 0.1 )
                                  .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                  .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                  .build();

        wres.config.yaml.components.Threshold pTwoWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pTwo )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Threshold pThree = Threshold.newBuilder()
                                    .setLeftThresholdProbability( 0.05 )
                                    .setOperator( Threshold.ThresholdOperator.EQUAL )
                                    .setDataType( Threshold.ThresholdDataType.ANY_RIGHT )
                                    .build();

        wres.config.yaml.components.Threshold pThreeWrapped =
                ThresholdBuilder.builder()
                                .threshold( pThree )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        Threshold pFour = Threshold.newBuilder()
                                   .setLeftThresholdProbability( 0.1 )
                                   .setOperator( Threshold.ThresholdOperator.EQUAL )
                                   .setDataType( Threshold.ThresholdDataType.ANY_RIGHT )
                                   .build();

        wres.config.yaml.components.Threshold pFourWrapped =
                ThresholdBuilder.builder()
                                .threshold( pFour )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        Set<wres.config.yaml.components.Threshold> expected = Set.of( pOneWrapped,
                                                                      pTwoWrapped,
                                                                      pThreeWrapped,
                                                                      pFourWrapped );

        assertEquals( expected, actualEvaluation.classifierThresholds() );
    }
}
