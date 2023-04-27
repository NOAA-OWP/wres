package wres.config.yaml;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.MetricConstants;
import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DateCondition;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.DurationBoundsType;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.FeatureDimension;
import wres.config.generated.FeatureGroup;
import wres.config.generated.FeaturePool;
import wres.config.generated.GraphicalType;
import wres.config.generated.IntBoundsType;
import wres.config.generated.LenienceType;
import wres.config.generated.MetricConfig;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.NamedFeature;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.Polygon;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdOperator;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeScaleFunction;
import wres.config.generated.UnnamedFeature;
import wres.config.generated.UrlParameter;
import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.AnalysisDurationsBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EnsembleFilterBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.FeatureServiceGroupBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.LeadTimeIntervalBuilder;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdService;
import wres.config.yaml.components.ThresholdServiceBuilder;
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
import wres.config.yaml.components.Variable;
import wres.config.yaml.components.VariableBuilder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link DeclarationFactory}.
 *
 * @author James Brown
 */

class DeclarationFactoryTest
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
    void testDeserializeWithShortSources() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithShortSourcesAndBaseline() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                baseline:
                  - yet_another_file.csv
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();

        BaselineDataset finalBaselineDataset = BaselineDatasetBuilder.builder()
                                                                     .dataset( baselineDataset )
                                                                     .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .baseline( finalBaselineDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithLongSources() throws IOException
    {
        String yaml = """
                observed:
                  sources:
                    - some_file.csv
                    - uri: file:/some/directory
                      pattern: '**/*.csv*'
                      time_zone_offset: CST
                      missing_value: -999.0
                    - uri: https://foo.bar
                      interface: usgs nwis
                      parameters:
                        foo: bar
                        baz: qux
                      time_scale:
                        function: mean
                        period: 1
                        unit: hours
                  type: observations
                  variable: foo
                  time_zone_offset: CST
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone_offset: -06:00
                    - uri: https://qux.quux
                      interface: wrds ahps
                      time_scale:
                        function: mean
                        period: 2
                        unit: hours
                  type: ensemble forecasts
                reference_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                valid_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                        """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();
        URI anotherObservedUri = URI.create( "file:/some/directory" );
        Source anotherObservedSource = SourceBuilder.builder()
                                                    .uri( anotherObservedUri )
                                                    .pattern( "**/*.csv*" )
                                                    .timeZoneOffset( ZoneOffset.of( "-0600" ) )
                                                    .missingValue( -999.0 )
                                                    .build();

        URI yetAnotherObservedUri = URI.create( "https://foo.bar" );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder().setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        wres.config.yaml.components.TimeScale outerTimeScale = new wres.config.yaml.components.TimeScale( timeScale );
        Source yetAnotherObservedSource = SourceBuilder.builder()
                                                       .uri( yetAnotherObservedUri )
                                                       .sourceInterface( SourceInterface.USGS_NWIS )
                                                       .parameters( Map.of( "foo", "bar", "baz", "qux" ) )
                                                       .timeScale( outerTimeScale )
                                                       .build();

        List<Source> observedSources =
                List.of( observedSource, anotherObservedSource, yetAnotherObservedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .type( DataType.OBSERVATIONS )
                                                .variable( new Variable( "foo", null ) )
                                                .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        URI anotherPredictedUri = URI.create( "file:/some/other/directory" );
        Source anotherPredictedSource = SourceBuilder.builder()
                                                     .uri( anotherPredictedUri )
                                                     .pattern( "**/*.xml*" )
                                                     .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                                     .build();

        URI yetAnotherPredictedUri = URI.create( "https://qux.quux" );
        TimeScale timeScalePredicted = TimeScale.newBuilder()
                                                .setPeriod( Duration.newBuilder().setSeconds( 7200 ) )
                                                .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                .build();
        wres.config.yaml.components.TimeScale
                outerTimeScalePredicted = new wres.config.yaml.components.TimeScale( timeScalePredicted );
        Source yetAnotherPredictedSource = SourceBuilder.builder()
                                                        .uri( yetAnotherPredictedUri )
                                                        .sourceInterface( SourceInterface.WRDS_AHPS )
                                                        .timeScale(
                                                                outerTimeScalePredicted )
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
                                                 .build();

        assertAll( () -> assertEquals( observedDataset, actual.left() ),
                   () -> assertEquals( predictedDataset, actual.right() )
        );
    }

    @Test
    void testDeserializeWithLongMetrics() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                minimum_sample_size: 23
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [0.3]
                    minimum_sample_size: 23
                  - name: pearson correlation coefficient
                    probability_thresholds:
                      values: [ 0.1 ]
                      operator: greater equal
                      apply_to: observed
                  - name: time to peak error
                    summary_statistics:
                      - mean
                      - median
                      - minimum
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold aValueThreshold = Threshold.newBuilder()
                                             .setLeftThresholdValue( DoubleValue.of( 0.3 ) )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .build();
        wres.config.yaml.components.Threshold valueThreshold =
                ThresholdBuilder.builder()
                                .threshold( aValueThreshold )
                                .type( ThresholdType.VALUE )
                                .build();
        Set<wres.config.yaml.components.Threshold> valueThresholds = Set.of( valueThreshold );
        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .valueThresholds( valueThresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( firstParameters )
                                    .build();

        Threshold aThreshold = Threshold.newBuilder()
                                        .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                        .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                        .build();
        wres.config.yaml.components.Threshold probabilityThreshold =
                ThresholdBuilder.builder()
                                .threshold( aThreshold )
                                .type( ThresholdType.PROBABILITY )
                                .build();
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = Set.of( probabilityThreshold );
        MetricParameters secondParameters =
                MetricParametersBuilder.builder()
                                       .probabilityThresholds( probabilityThresholds )
                                       .build();

        Metric second = MetricBuilder.builder()
                                     .name( MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                                     .parameters( secondParameters )
                                     .build();

        // Predictable iteration order
        Set<MetricConstants> summaryStatistics = new TreeSet<>();
        summaryStatistics.add( MetricConstants.MEAN );
        summaryStatistics.add( MetricConstants.MEDIAN );
        summaryStatistics.add( MetricConstants.MINIMUM );

        MetricParameters thirdParameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .build();
        Metric third = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( thirdParameters )
                                    .build();

        // Insertion order
        Set<Metric> metrics = new LinkedHashSet<>();
        metrics.add( first );
        metrics.add( second );
        metrics.add( third );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( metrics )
                                                                     .build();

        assertEquals( expected.metrics(), actual.metrics() );
    }

    @Test
    void testDeserializeWithFeatureGroup() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                feature_groups:
                  - name: a group
                    features:
                      - observed: DRRC2
                        predicted: DRRC2
                      - observed: DOLC2
                        predicted: DOLC2
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder().setName( DRRC2 ) )
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder().setName( DOLC2 ) )
                                            .setRight( Geometry.newBuilder().setName( DOLC2 ) )
                                            .build();

        List<GeometryTuple> geometries = List.of( first, second );

        GeometryGroup geometryGroup = GeometryGroup.newBuilder()
                                                   .setRegionName( "a group" )
                                                   .addAllGeometryTuples( geometries )
                                                   .build();

        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( geometryGroup ) )
                                                          .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .featureGroups( featureGroups )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeatureServiceAndSingletonGroup() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                feature_service:
                  uri: https://foo.service
                  group: RFC
                  value: CNRFC
                  pool: true
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        FeatureServiceGroup featureServiceGroup = FeatureServiceGroupBuilder.builder()
                                                                            .group( "RFC" )
                                                                            .value( "CNRFC" )
                                                                            .pool( true )
                                                                            .build();
        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .featureGroups( Set.of( featureServiceGroup ) )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .featureService( featureService )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeatureServiceAndTwoGroups() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                feature_service:
                  uri: https://foo.service
                  groups:
                    - group: RFC
                      value: CNRFC
                    - group: RFC
                      value: NWRFC
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        FeatureServiceGroup groupOne = FeatureServiceGroupBuilder.builder()
                                                                 .group( "RFC" )
                                                                 .value( "CNRFC" )
                                                                 .build();

        FeatureServiceGroup groupTwo = FeatureServiceGroupBuilder.builder()
                                                                 .group( "RFC" )
                                                                 .value( "NWRFC" )
                                                                 .build();

        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .featureGroups( Set.of( groupOne, groupTwo ) )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .featureService( featureService )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSpatialMask() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                spatial_mask:
                  name: a spatial mask!
                  wkt: POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225))
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        SpatialMask expectedMask
                = new SpatialMask( "a spatial mask!",
                                   "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225))",
                                   null );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .spatialMask( expectedMask )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithTimePools() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                reference_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                reference_date_pools:
                  period: 13
                  frequency: 7
                  unit: hours
                valid_dates:
                  minimum: 2552-03-17T00:00:00Z
                  maximum: 2552-03-20T00:00:00Z
                valid_date_pools:
                  period: 11
                  frequency: 2
                  unit: hours
                lead_times:
                  minimum: 0
                  maximum: 40
                  unit: hours
                lead_time_pools:
                  period: 23
                  frequency: 17
                  unit: hours
                analysis_durations:
                  minimum_exclusive: 0
                  maximum: 1
                  unit: hours
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        TimeInterval referenceDates = new TimeInterval( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                        Instant.parse( "2551-03-20T00:00:00Z" ) );

        TimePools referenceDatePools = new TimePools( java.time.Duration.ofHours( 13 ),
                                                      java.time.Duration.ofHours( 7 ) );

        TimeInterval validDates = new TimeInterval( Instant.parse( "2552-03-17T00:00:00Z" ),
                                                    Instant.parse( "2552-03-20T00:00:00Z" ) );

        TimePools validDatePools = new TimePools( java.time.Duration.ofHours( 11 ),
                                                  java.time.Duration.ofHours( 2 ) );

        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                                  java.time.Duration.ofHours( 40 ) );
        TimePools leadTimePools = new TimePools( java.time.Duration.ofHours( 23 ),
                                                 java.time.Duration.ofHours( 17 ) );

        AnalysisDurations analysisDurations = new AnalysisDurations( 0, 1, ChronoUnit.HOURS );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .referenceDates( referenceDates )
                                                                     .referenceDatePools( referenceDatePools )
                                                                     .validDates( validDates )
                                                                     .validDatePools( validDatePools )
                                                                     .leadTimes( leadTimeInterval )
                                                                     .leadTimePools( leadTimePools )
                                                                     .analysisDurations( analysisDurations )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithTimeScale() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                time_scale:
                  function: mean
                  period: 1
                  unit: hours
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder().setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        wres.config.yaml.components.TimeScale outerTimeScale = new wres.config.yaml.components.TimeScale( timeScale );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .timeScale( outerTimeScale )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithUnitAndUnitAliases() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                unit: m3/s
                unit_aliases:
                  - unit: '[degF]'
                    alias: °F
                  - unit: '[cel]'
                    alias: °C
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        UnitAlias one = new UnitAlias( "°F", "[degF]" );
        UnitAlias two = new UnitAlias( "°C", "[cel]" );
        Set<UnitAlias> unitAliases = Set.of( one, two );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .unit( "m3/s" )
                                                                     .unitAliases( unitAliases )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithPairFrequency() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                pair_frequency:
                  period: 12
                  unit: hours
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .pairFrequency( java.time.Duration.ofHours( 12 ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithCrossPairExact() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                cross_pair: exact
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .crossPair( CrossPair.EXACT )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithEnsembleAverage() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                ensemble_average: median
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSeasonFilter() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                season:
                   minimum_month: 4
                   minimum_day: 1
                   maximum_month: 7
                   maximum_day: 31
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Season season = new Season( MonthDay.of( 4, 1 ), MonthDay.of( 7, 31 ) );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .season( season )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithValuesFilter() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                values:
                  minimum: 12.1
                  maximum: 23.2
                  below_minimum: 0.0
                  above_maximum: 27.0
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Values values = new Values( 12.1, 23.2, 0.0, 27.0 );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .values( values )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithOutputFormats() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                duration_format: hours
                decimal_format: '0.000000'
                output_formats:
                  - netcdf2
                  - pairs
                  - csv2
                  - format: csv
                  - format: png
                    width: 800
                    height: 600
                    orientation: threshold lead
                  - format: netcdf
                    template_path: foo.bar
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Outputs.NumericFormat numericFormat = Outputs.NumericFormat.newBuilder()
                                                                   .setDecimalFormat( "#0.000000" )
                                                                   .build();
        Outputs.GraphicFormat graphicFormat = Outputs.GraphicFormat.newBuilder()
                                                                   .setWidth( 800 )
                                                                   .setHeight( 600 )
                                                                   .setShape( Outputs.GraphicFormat.GraphicShape.THRESHOLD_LEAD )
                                                                   .build();
        Outputs formats = Outputs.newBuilder()
                                 .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
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
                                 .setNetcdf( Formats.NETCDF_FORMAT.toBuilder()
                                                                  .setTemplatePath( "foo.bar" )
                                                                  .build() )
                                 .build();

        DecimalFormat formatter = new DecimalFormatPretty( "#0.000000" );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .decimalFormat( formatter )
                                                                     .durationFormat( ChronoUnit.HOURS )
                                                                     .formats( new Formats( formats ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                probability_thresholds: [0.1,0.2,0.9]
                value_thresholds:
                  name: MAJOR FLOOD
                  values:
                    - { value: 23.0, feature: DRRC2 }
                    - { value: 27.0, feature: DOLC2 }
                  operator: greater
                  apply_to: predicted
                classifier_thresholds:
                  name: COLONEL DROUGHT
                  values:
                    - { value: 0.2, feature: DRRC2 }
                    - { value: 0.3, feature: DOLC2 }
                  operator: greater
                  apply_to: observed
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();
        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();
        Threshold pThree = Threshold.newBuilder()
                                    .setLeftThresholdProbability( DoubleValue.of( 0.9 ) )
                                    .setOperator( Threshold.ThresholdOperator.GREATER )
                                    .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pTwo )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pThreeWrapped = ThresholdBuilder.builder()
                                                                              .threshold( pThree )
                                                                              .type( ThresholdType.PROBABILITY )
                                                                              .build();

        // Insertion order
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = new LinkedHashSet<>();
        probabilityThresholds.add( pOneWrapped );
        probabilityThresholds.add( pTwoWrapped );
        probabilityThresholds.add( pThreeWrapped );

        Threshold vOne = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 23.0 ) )
                                  .setDataType( Threshold.ThresholdDataType.RIGHT )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "MAJOR FLOOD" )
                                  .build();
        Threshold vTwo = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 27.0 ) )
                                  .setDataType( Threshold.ThresholdDataType.RIGHT )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "MAJOR FLOOD" )
                                  .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DRRC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        wres.config.yaml.components.Threshold vTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vTwo )
                                                                            .type( ThresholdType.VALUE )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DOLC2" )
                                                                                              .build() )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( vOneWrapped );
        valueThresholds.add( vTwoWrapped );

        Threshold cOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "COLONEL DROUGHT" )
                                  .build();
        Threshold cTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.3 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "COLONEL DROUGHT" )
                                  .build();

        wres.config.yaml.components.Threshold cOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cOne )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DRRC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        wres.config.yaml.components.Threshold cTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cTwo )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DOLC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> classifierThresholds = new LinkedHashSet<>();
        classifierThresholds.add( cOneWrapped );
        classifierThresholds.add( cTwoWrapped );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .probabilityThresholds( probabilityThresholds )
                                                                     .valueThresholds( valueThresholds )
                                                                     .classifierThresholds( classifierThresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithMultipleSetsOfValueThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                value_thresholds:
                  - name: moon
                    values: [0.1]
                  - name: bat
                    values: [0.2]
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold vOne =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder()
                                                              .setLeftThresholdValue( DoubleValue.of( 0.1 ) )
                                                              .setName( "moon" )
                                                              .build();
        Threshold vTwo =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder()
                                                              .setLeftThresholdValue( DoubleValue.of( 0.2 ) )
                                                              .setName( "bat" )
                                                              .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        wres.config.yaml.components.Threshold vTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vTwo )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( vOneWrapped );
        valueThresholds.add( vTwoWrapped );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .valueThresholds( valueThresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeTimeToPeakError() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                metrics:
                  - name: time to peak error
                    summary_statistics:
                      - mean
                      - minimum
                      - mean absolute
                      - standard deviation
                      - maximum
                      - median
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( Set.of( MetricConstants.MEAN,
                                                                   MetricConstants.MEDIAN,
                                                                   MetricConstants.MEAN_ABSOLUTE,
                                                                   MetricConstants.MAXIMUM,
                                                                   MetricConstants.MINIMUM,
                                                                   MetricConstants.STANDARD_DEVIATION ) )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( firstParameters )
                                    .build();

        Set<Metric> metrics = Set.of( first );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( metrics )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithThresholdSetsAndMetricsAndAnchorReference() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                threshold_sets:
                  - probability_thresholds: &id1
                      values: [ 0.1,0.2 ]
                      operator: greater equal
                      apply_to: observed
                metrics:
                  - name: mean absolute error
                    probability_thresholds: *id1
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                  .build();
        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pTwo )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        // Insertion order
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = new LinkedHashSet<>();
        probabilityThresholds.add( pOneWrapped );
        probabilityThresholds.add( pTwoWrapped );

        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .probabilityThresholds( probabilityThresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                    .parameters( firstParameters )
                                    .build();

        Set<Metric> metrics = Set.of( first );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .thresholdSets( probabilityThresholds )
                                                                     .metrics( metrics )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithBaselineAndSeparateMetrics() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                baseline:
                  sources:
                    - another_file.csv
                  separate_metrics: true
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.predictedDataset )
                                                         .separateMetrics( true )
                                                         .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .baseline( baseline )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithThresholdService() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                threshold_service:
                  uri: https://foo
                  provider: bar
                  rating_provider: baz
                  parameter: moon
                  missing_value: -9999999
                  unit: qux
                  feature_name_from: observed
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        ThresholdService thresholdService = ThresholdServiceBuilder.builder()
                                                                   .uri( URI.create( "https://foo" ) )
                                                                   .provider( "bar" )
                                                                   .ratingProvider( "baz" )
                                                                   .parameter( "moon" )
                                                                   .missingValue( -9999999.0 )
                                                                   .unit( "qux" )
                                                                   .featureNameFrom( DatasetOrientation.LEFT )
                                                                   .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .thresholdService( thresholdService )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeUsingPathToFile() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path path = fileSystem.getPath( "foo.file" );
            Files.createFile( path );
            String pathString = path.toString();

            String yaml = """
                    observed:
                      - some_file.csv
                    predicted:
                      - another_file.csv
                      """;

            Files.writeString( path, yaml );

            EvaluationDeclaration actual = DeclarationFactory.from( pathString, fileSystem, false );

            EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                         .left( this.observedDataset )
                                                                         .right( this.predictedDataset )
                                                                         .build();
            assertEquals( expected, actual );
        }
    }

    @Test
    void testSerializeWithShortSources() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                  """;

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithShortSourcesAndBaseline() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                baseline:
                  sources:
                    - yet_another_file.csv
                  """;

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();

        BaselineDataset finalBaselineDataset = BaselineDatasetBuilder.builder()
                                                                     .dataset( baselineDataset )
                                                                     .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .baseline( finalBaselineDataset )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithLongSources() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                    - uri: file:/some/directory
                      pattern: '**/*.csv*'
                      time_zone_offset: "-0600"
                      missing_value: -999.0
                    - uri: https://foo.bar
                      interface: usgs nwis
                      parameters:
                        foo: bar
                      time_scale:
                        function: mean
                        period: 1
                        unit: hours
                  variable: foo
                  type: observations
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone_offset: "-0600"
                    - uri: https://qux.quux
                      interface: wrds ahps
                      time_scale:
                        function: mean
                        period: 2
                        unit: hours
                  type: ensemble forecasts
                  time_shift:
                    period: -2
                    unit: hours
                reference_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                valid_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                        """;

        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();
        URI anotherObservedUri = URI.create( "file:/some/directory" );
        Source anotherObservedSource = SourceBuilder.builder()
                                                    .uri( anotherObservedUri )
                                                    .pattern( "**/*.csv*" )
                                                    .timeZoneOffset( ZoneOffset.of( "-0600" ) )
                                                    .missingValue( -999.0 )
                                                    .build();

        URI yetAnotherObservedUri = URI.create( "https://foo.bar" );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder().setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        wres.config.yaml.components.TimeScale outerTimeScale = new wres.config.yaml.components.TimeScale( timeScale );
        Source yetAnotherObservedSource = SourceBuilder.builder()
                                                       .uri( yetAnotherObservedUri )
                                                       .sourceInterface( SourceInterface.USGS_NWIS )
                                                       .parameters( Map.of( "foo", "bar" ) )
                                                       .timeScale( outerTimeScale )
                                                       .build();

        List<Source> observedSources =
                List.of( observedSource, anotherObservedSource, yetAnotherObservedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .type( DataType.OBSERVATIONS )
                                                .variable( new Variable( "foo", null ) )
                                                .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        URI anotherPredictedUri = URI.create( "file:/some/other/directory" );
        Source anotherPredictedSource = SourceBuilder.builder()
                                                     .uri( anotherPredictedUri )
                                                     .pattern( "**/*.xml*" )
                                                     .timeZoneOffset( ZoneOffset.of( "-06:00" ) )
                                                     .build();

        URI yetAnotherPredictedUri = URI.create( "https://qux.quux" );
        TimeScale timeScalePredicted = TimeScale.newBuilder()
                                                .setPeriod( Duration.newBuilder().setSeconds( 7200 ) )
                                                .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                                .build();
        wres.config.yaml.components.TimeScale
                outerTimeScalePredicted = new wres.config.yaml.components.TimeScale( timeScalePredicted );
        Source yetAnotherPredictedSource = SourceBuilder.builder()
                                                        .uri( yetAnotherPredictedUri )
                                                        .sourceInterface( SourceInterface.WRDS_AHPS )
                                                        .timeScale( outerTimeScalePredicted )
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
                                                 .timeShift( java.time.Duration.ofHours( -2 ) )
                                                 .build();

        TimeInterval timeInterval = new TimeInterval( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                      Instant.parse( "2551-03-20T00:00:00Z" ) );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( observedDataset )
                                                                       .right( predictedDataset )
                                                                       .referenceDates( timeInterval )
                                                                       .validDates( timeInterval )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithLongMetrics() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                minimum_sample_size: 23
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [0.3]
                  - name: pearson correlation coefficient
                    probability_thresholds:
                      values: [0.1]
                      operator: greater equal
                  - name: time to peak error
                    summary_statistics:
                      - mean
                      - median
                      - minimum
                  """;

        Threshold aValueThreshold = Threshold.newBuilder()
                                             .setLeftThresholdValue( DoubleValue.of( 0.3 ) )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .build();
        wres.config.yaml.components.Threshold valueThreshold =
                ThresholdBuilder.builder()
                                .threshold( aValueThreshold )
                                .type( ThresholdType.VALUE )
                                .build();
        Set<wres.config.yaml.components.Threshold> valueThresholds = Set.of( valueThreshold );
        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .valueThresholds( valueThresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( firstParameters )
                                    .build();

        Threshold aThreshold = Threshold.newBuilder()
                                        .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                        .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                        .build();
        wres.config.yaml.components.Threshold probabilityThreshold =
                ThresholdBuilder.builder()
                                .threshold( aThreshold )
                                .type( ThresholdType.PROBABILITY )
                                .build();
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = Set.of( probabilityThreshold );
        MetricParameters secondParameters =
                MetricParametersBuilder.builder()
                                       .probabilityThresholds( probabilityThresholds )
                                       .build();

        Metric second = MetricBuilder.builder()
                                     .name( MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                                     .parameters( secondParameters )
                                     .build();

        // preserve insertion order
        Set<MetricConstants> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( MetricConstants.MEAN );
        summaryStatistics.add( MetricConstants.MEDIAN );
        summaryStatistics.add( MetricConstants.MINIMUM );

        MetricParameters thirdParameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .build();
        Metric third = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( thirdParameters )
                                    .build();

        // Insertion order
        Set<Metric> metrics = new LinkedHashSet<>();
        metrics.add( first );
        metrics.add( second );
        metrics.add( third );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .metrics( metrics )
                                                                       .minimumSampleSize( 23 )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithFeatureGroup() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                feature_groups:
                  - name: a group
                    features:
                      - DRRC2
                      - DOLC2
                  """;

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder().setName( DRRC2 ) )
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder().setName( DOLC2 ) )
                                            .setRight( Geometry.newBuilder().setName( DOLC2 ) )
                                            .build();

        List<GeometryTuple> geometries = List.of( first, second );

        GeometryGroup geometryGroup = GeometryGroup.newBuilder()
                                                   .setRegionName( "a group" )
                                                   .addAllGeometryTuples( geometries )
                                                   .build();

        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( geometryGroup ) )
                                                          .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .featureGroups( featureGroups )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithFeatureServiceAndSingletonGroup() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                feature_service:
                  uri: https://foo.service
                  group: RFC
                  value: CNRFC
                  pool: true
                  """;

        FeatureServiceGroup featureServiceGroup = FeatureServiceGroupBuilder.builder()
                                                                            .group( "RFC" )
                                                                            .value( "CNRFC" )
                                                                            .pool( true )
                                                                            .build();
        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .featureGroups( Set.of( featureServiceGroup ) )
                                                             .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .featureService( featureService )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithFeatureServiceAndTwoGroups() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                feature_service:
                  uri: https://foo.service
                  groups:
                    - group: RFC
                      value: CNRFC
                    - group: RFC
                      value: NWRFC
                  """;

        FeatureServiceGroup groupOne = FeatureServiceGroupBuilder.builder()
                                                                 .group( "RFC" )
                                                                 .value( "CNRFC" )
                                                                 .build();

        FeatureServiceGroup groupTwo = FeatureServiceGroupBuilder.builder()
                                                                 .group( "RFC" )
                                                                 .value( "NWRFC" )
                                                                 .build();

        // Preserve insertion order
        Set<FeatureServiceGroup> groups = new LinkedHashSet<>();
        groups.add( groupOne );
        groups.add( groupTwo );

        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .featureGroups( groups )
                                                             .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .featureService( featureService )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithSpatialMask() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                spatial_mask:
                  name: a spatial mask!
                  wkt: "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225))"
                  """;

        SpatialMask expectedMask
                = new SpatialMask( "a spatial mask!",
                                   "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225))",
                                   null );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .spatialMask( expectedMask )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithTimePools() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                reference_dates:
                  minimum: 2551-03-17T00:00:00Z
                  maximum: 2551-03-20T00:00:00Z
                reference_date_pools:
                  period: 13
                  frequency: 7
                  unit: hours
                valid_dates:
                  minimum: 2552-03-17T00:00:00Z
                  maximum: 2552-03-20T00:00:00Z
                valid_date_pools:
                  period: 11
                  frequency: 2
                  unit: hours
                lead_times:
                  minimum: 0
                  maximum: 40
                  unit: hours
                lead_time_pools:
                  period: 23
                  frequency: 17
                  unit: hours
                analysis_durations:
                  minimum_exclusive: 0
                  maximum: 1
                  unit: hours
                pair_frequency:
                  period: 3
                  unit: hours
                  """;

        TimeInterval referenceDates = new TimeInterval( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                        Instant.parse( "2551-03-20T00:00:00Z" ) );

        TimePools referenceDatePools = new TimePools( java.time.Duration.ofHours( 13 ),
                                                      java.time.Duration.ofHours( 7 ) );

        TimeInterval validDates = new TimeInterval( Instant.parse( "2552-03-17T00:00:00Z" ),
                                                    Instant.parse( "2552-03-20T00:00:00Z" ) );

        TimePools validDatePools = new TimePools( java.time.Duration.ofHours( 11 ),
                                                  java.time.Duration.ofHours( 2 ) );

        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                                  java.time.Duration.ofHours( 40 ) );
        TimePools leadTimePools = new TimePools( java.time.Duration.ofHours( 23 ),
                                                 java.time.Duration.ofHours( 17 ) );

        AnalysisDurations analysisDurations = new AnalysisDurations( 0, 1, ChronoUnit.HOURS );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .referenceDates( referenceDates )
                                                                       .referenceDatePools( referenceDatePools )
                                                                       .validDates( validDates )
                                                                       .validDatePools( validDatePools )
                                                                       .leadTimes( leadTimeInterval )
                                                                       .leadTimePools( leadTimePools )
                                                                       .analysisDurations( analysisDurations )
                                                                       .pairFrequency( java.time.Duration.ofHours( 3 ) )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithTimeScale() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                time_scale:
                  function: mean
                  period: 1
                  unit: hours
                  """;

        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder().setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        wres.config.yaml.components.TimeScale outerTimeScale = new wres.config.yaml.components.TimeScale( timeScale );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .timeScale( outerTimeScale )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithUnitAndUnitAliases() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                unit: m3/s
                unit_aliases:
                  - alias: °F
                    unit: "[degF]"
                  - alias: °C
                    unit: "[cel]"
                  """;

        UnitAlias one = new UnitAlias( "°F", "[degF]" );
        UnitAlias two = new UnitAlias( "°C", "[cel]" );

        // Preserve insertion order
        Set<UnitAlias> unitAliases = new LinkedHashSet<>();
        unitAliases.add( one );
        unitAliases.add( two );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .unit( "m3/s" )
                                                                       .unitAliases( unitAliases )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithPairFrequency() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                pair_frequency:
                  period: 12
                  unit: hours
                  """;

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .pairFrequency( java.time.Duration.ofHours( 12 ) )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithCrossPairExact() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                cross_pair: exact
                  """;

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .crossPair( CrossPair.EXACT )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithEnsembleAverage() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                ensemble_average: median
                  """;

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithSeasonFilter() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                season:
                  minimum_day: 1
                  minimum_month: 4
                  maximum_day: 31
                  maximum_month: 7
                  """;

        Season season = new Season( MonthDay.of( 4, 1 ), MonthDay.of( 7, 31 ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .season( season )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithValuesFilter() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                values:
                  minimum: 12.1
                  maximum: 23.2
                  below_minimum: 0.0
                  above_maximum: 27.0
                  """;

        Values values = new Values( 12.1, 23.2, 0.0, 27.0 );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .values( values )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithOutputFormats() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                duration_format: hours
                decimal_format: '#0.000'
                output_formats:
                  - csv2
                  - csv
                  - pairs
                  - format: netcdf
                    template_path: foo.bar
                  - format: png
                    width: 1200
                    height: 800
                    orientation: threshold lead
                """;

        Outputs.NumericFormat numericFormat = Outputs.NumericFormat.newBuilder()
                                                                   .setDecimalFormat( "#0.000000" )
                                                                   .build();
        Outputs.GraphicFormat graphicFormat = Outputs.GraphicFormat.newBuilder()
                                                                   .setWidth( 1200 )
                                                                   .setHeight( 800 )
                                                                   .setShape( Outputs.GraphicFormat.GraphicShape.THRESHOLD_LEAD )
                                                                   .build();
        Outputs outputs = Outputs.newBuilder()
                                 .setNetcdf( Outputs.NetcdfFormat.newBuilder()
                                                                 .setTemplatePath( "foo.bar" ) )
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
                                 .build();

        DecimalFormat formatter = new DecimalFormatPretty( "#0.000" );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .decimalFormat( formatter )
                                                                       .durationFormat( ChronoUnit.HOURS )
                                                                       .formats( new Formats( outputs ) )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithThresholds() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                probability_thresholds: [0.1, 0.2, 0.9]
                value_thresholds:
                  name: MAJOR FLOOD
                  values:
                    - value: 27.0
                      feature: DOLC2
                    - value: 23.0
                      feature: DRRC2
                  apply_to: predicted
                classifier_thresholds:
                  name: COLONEL DROUGHT
                  values:
                    - value: 0.3
                      feature: DOLC2
                    - value: 0.2
                      feature: DRRC2
                """;

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();
        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();
        Threshold pThree = Threshold.newBuilder()
                                    .setLeftThresholdProbability( DoubleValue.of( 0.9 ) )
                                    .setOperator( Threshold.ThresholdOperator.GREATER )
                                    .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pTwo )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pThreeWrapped = ThresholdBuilder.builder()
                                                                              .threshold( pThree )
                                                                              .type( ThresholdType.PROBABILITY )
                                                                              .build();

        // Insertion order
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = new LinkedHashSet<>();
        probabilityThresholds.add( pOneWrapped );
        probabilityThresholds.add( pTwoWrapped );
        probabilityThresholds.add( pThreeWrapped );

        Threshold vOne = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 27.0 ) )
                                  .setDataType( Threshold.ThresholdDataType.RIGHT )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "MAJOR FLOOD" )
                                  .build();
        Threshold vTwo = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 23.0 ) )
                                  .setDataType( Threshold.ThresholdDataType.RIGHT )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "MAJOR FLOOD" )
                                  .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .type( ThresholdType.VALUE )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DOLC2" )
                                                                                              .build() )
                                                                            .build();

        wres.config.yaml.components.Threshold vTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vTwo )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DRRC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( vOneWrapped );
        valueThresholds.add( vTwoWrapped );

        Threshold cOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.3 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "COLONEL DROUGHT" )
                                  .build();
        Threshold cTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "COLONEL DROUGHT" )
                                  .build();

        wres.config.yaml.components.Threshold cOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cOne )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DOLC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        wres.config.yaml.components.Threshold cTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cTwo )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "DRRC2" )
                                                                                              .build() )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> classifierThresholds = new LinkedHashSet<>();
        classifierThresholds.add( cOneWrapped );
        classifierThresholds.add( cTwoWrapped );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .probabilityThresholds( probabilityThresholds )
                                                                       .valueThresholds( valueThresholds )
                                                                       .classifierThresholds( classifierThresholds )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithThresholdSets() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                threshold_sets:
                  - probability_thresholds:
                      values: [0.1, 0.2]
                      operator: greater equal
                """;

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                  .build();
        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        wres.config.yaml.components.Threshold pTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pTwo )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        // Insertion order
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = new LinkedHashSet<>();
        probabilityThresholds.add( pOneWrapped );
        probabilityThresholds.add( pTwoWrapped );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .thresholdSets( probabilityThresholds )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithBaselineAndSeparateMetrics() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                baseline:
                  sources:
                    - another_file.csv
                  separate_metrics: true
                  """;

        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.predictedDataset )
                                                         .separateMetrics( true )
                                                         .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .baseline( baseline )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithThresholdService() throws IOException
    {
        String expected = """
                observed:
                  sources:
                    - some_file.csv
                predicted:
                  sources:
                    - another_file.csv
                threshold_service:
                  uri: https://foo
                  parameter: moon
                  unit: qux
                  provider: bar
                  rating_provider: baz
                  missing_value: -9999999.0
                  """;

        ThresholdService thresholdService = ThresholdServiceBuilder.builder()
                                                                   .uri( URI.create( "https://foo" ) )
                                                                   .provider( "bar" )
                                                                   .ratingProvider( "baz" )
                                                                   .parameter( "moon" )
                                                                   .missingValue( -9999999.0 )
                                                                   .unit( "qux" )
                                                                   .featureNameFrom( DatasetOrientation.LEFT )
                                                                   .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .thresholdService( thresholdService )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
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

        EvaluationDeclaration actual = DeclarationFactory.from( project );

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

        EvaluationDeclaration actual = DeclarationFactory.from( project );


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
                                                  .timeScale( TimeScaleBuilder.builder()
                                                                              .timeScale( timeScale )
                                                                              .build() )
                                                  .parameters( Map.of( "moon", "bat" ) )
                                                  .build();

        Dataset predictedLong = DatasetBuilder.builder()
                                              .sources( List.of( predictedSourceLong ) )
                                              .timeShift( java.time.Duration.ofHours( 2 ) )
                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                              .build();

        BaselineDataset baselineLong = BaselineDatasetBuilder.builder()
                                                             .dataset( this.predictedDataset )
                                                             .persistence( 2 )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( observedLong )
                                                                     .right( predictedLong )
                                                                     .baseline( baselineLong )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithPairOptions()
    {
        wres.config.generated.FeatureService featureService =
                new wres.config.generated.FeatureService( URI.create( "https://moo" ),
                                                          List.of( new FeatureGroup( "small", "ball", false ) ) );
        List<wres.config.generated.UnitAlias> aliases =
                List.of( new wres.config.generated.UnitAlias( "banana", "pear" ) );
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
                                                 new DurationBoundsType( 4, 5, DurationUnit.HOURS ),
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
                                                 new wres.config.generated.CrossPair( false ),
                                                 null );

        ProjectConfig project = new ProjectConfig( this.inputs,
                                                   pairOptions,
                                                   this.metrics,
                                                   this.outputs,
                                                   null,
                                                   null );

        EvaluationDeclaration actual = DeclarationFactory.from( project );

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
        SpatialMask spatialMask = SpatialMaskBuilder.builder()
                                                    .wkt( "POLYGON ((1 2, 3 4, 5 6, 1 2))" )
                                                    .srid( 4326L )
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
        AnalysisDurations analysisDurations = AnalysisDurationsBuilder.builder()
                                                                      .minimumExclusive( 4 )
                                                                      .maximum( 5 )
                                                                      .unit( ChronoUnit.HOURS )
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

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
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
                                                                     .analysisDurations( analysisDurations )
                                                                     .referenceDates( referenceTimeInterval )
                                                                     .validDatePools( validTimePools )
                                                                     .referenceDatePools( referenceTimePools )
                                                                     .leadTimePools( leadTimePools )
                                                                     .season( season )
                                                                     .values( values )
                                                                     .crossPair( CrossPair.FUZZY )
                                                                     .unit( "dogfish" )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testMigrateProjectWithTwoGroupsOfMetricsAndThresholds()
    {
        // First group of metrics with thresholds
        List<MetricConfig> someMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ABSOLUTE_ERROR ) );
        ThresholdsConfig someThresholds = new ThresholdsConfig( wres.config.generated.ThresholdType.PROBABILITY,
                                                                ThresholdDataType.LEFT, "0.1",
                                                                ThresholdOperator.GREATER_THAN );
        List<ThresholdsConfig> thresholdSets = List.of( someThresholds );
        MetricsConfig someMetricsWrapped = new MetricsConfig( thresholdSets, null, someMetrics, null, null );

        // Second group of metrics with thresholds
        List<MetricConfig> moreMetrics = List.of( new MetricConfig( null, MetricConfigName.MEAN_ERROR ) );
        ThresholdsConfig moreThresholds = new ThresholdsConfig( wres.config.generated.ThresholdType.PROBABILITY,
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

        EvaluationDeclaration actual = DeclarationFactory.from( project );

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( pOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> someExpectedThresholds = Set.of( pOneWrapped );
        MetricParameters someParameters = MetricParametersBuilder.builder()
                                                                 .probabilityThresholds( someExpectedThresholds )
                                                                 .build();

        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                        .parameters( someParameters )
                                        .build();

        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
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

        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ERROR )
                                        .parameters( someMoreParameters )
                                        .build();

        Set<Metric> expectedMetrics = Set.of( metricOne, metricTwo );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( expectedMetrics )
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

        EvaluationDeclaration actual = DeclarationFactory.from( project );

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

        EvaluationDeclaration actual = DeclarationFactory.from( project );

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

        EvaluationDeclaration actual = DeclarationFactory.from( project );

        assertEquals( 14, actual.minimumSampleSize() );
    }
}