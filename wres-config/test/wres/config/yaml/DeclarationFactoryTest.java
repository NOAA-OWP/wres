package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.FeatureServiceGroupBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.FormatsBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
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
    /** An observed dataset for re-use. */
    private Dataset observedDataset;
    /** A predicted dataset for re-use. */
    private Dataset predictedDataset;
    /** Re-used feature string. */
    private static final String DRRC2 = "DRRC2";
    /** Re-used feature string. */
    private static final String DOLC2 = "DOLC2";
    /** All data threshold. */
    private static final wres.config.yaml.components.Threshold ALL_DATA_THRESHOLD =
            new wres.config.yaml.components.Threshold( wres.statistics.generated.Threshold.newBuilder()
                                                                                          .setLeftThresholdValue(
                                                                                                  DoubleValue.of( Double.NEGATIVE_INFINITY ) )
                                                                                          .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                                                          .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                                                                          .build(),
                                                       wres.config.yaml.components.ThresholdType.VALUE,
                                                       null );

    @BeforeEach
    void runBeforeEach()
    {
        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "another_file.csv" );
        Source predictedSource = SourceBuilder.builder()
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
                                       .minimumSampleSize( 23 )
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
        Set<ComponentName> summaryStatistics = new TreeSet<>();
        summaryStatistics.add( ComponentName.MEAN );
        summaryStatistics.add( ComponentName.MEDIAN );
        summaryStatistics.add( ComponentName.MINIMUM );

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

        TimePools referenceDatePools = new TimePools( 13, 7, ChronoUnit.HOURS );

        TimeInterval validDates = new TimeInterval( Instant.parse( "2552-03-17T00:00:00Z" ),
                                                    Instant.parse( "2552-03-20T00:00:00Z" ) );

        TimePools validDatePools = new TimePools( 11, 2, ChronoUnit.HOURS );

        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( 0, 40, ChronoUnit.HOURS );
        TimePools leadTimePools = new TimePools( 23, 17, ChronoUnit.HOURS );

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
        Formats formats = FormatsBuilder.builder()
                                        .netcdf2Format( Outputs.Netcdf2Format.getDefaultInstance() )
                                        .pairsFormat( Outputs.PairFormat.newBuilder()
                                                                        .setOptions( numericFormat )
                                                                        .build() )
                                        .csv2Format( Outputs.Csv2Format.newBuilder()
                                                                       .setOptions( numericFormat )
                                                                       .build() )
                                        .csvFormat( Outputs.CsvFormat.newBuilder()
                                                                     .setOptions( numericFormat )
                                                                     .build() )
                                        .pngFormat( Outputs.PngFormat.newBuilder()
                                                                     .setOptions( graphicFormat )
                                                                     .build() )
                                        .build();

        DecimalFormat formatter = new DecimalFormatPretty( "#0.000000" );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .decimalFormat( formatter )
                                                                     .durationFormat( ChronoUnit.HOURS )
                                                                     .formats( formats )
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
                                                                            .featureName( "DRRC2" )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        wres.config.yaml.components.Threshold vTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vTwo )
                                                                            .type( ThresholdType.VALUE )
                                                                            .featureName( "DOLC2" )
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
                                                                            .featureName( "DRRC2" )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        wres.config.yaml.components.Threshold cTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cTwo )
                                                                            .featureName( "DOLC2" )
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
    void testDeserializeWithThresholdSetsAndMetricsAndAnchorAlias() throws IOException
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
    void testInterpolateCsv2FormatWhenNoneDeclared()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( this.predictedDataset )
                                                                        .build();

        EvaluationDeclaration interpolate = DeclarationFactory.interpolate( declaration );

        Formats actual = interpolate.formats();
        Formats expected = FormatsBuilder.builder()
                                         .csv2Format( Formats.CSV2_FORMAT )
                                         .build();
        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateAllValidMetricsForSingleValuedTimeSeries() throws IOException
    {
        Dataset predictedDataset = DatasetBuilder.builder( this.predictedDataset )
                                                 .type( DataType.SIMULATIONS )
                                                 .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( predictedDataset )
                                                                        .build();

        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( declaration );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        assertEquals( MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics(), actualMetrics );
    }

    @Test
    void testInterpolateAllValidMetricsForEnsembleTimeSeries() throws IOException
    {
        Dataset predictedDataset = DatasetBuilder.builder( this.predictedDataset )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
                                                 .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( predictedDataset )
                                                                        .build();

        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( declaration );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> expectedMetrics = new HashSet<>( MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics() );
        expectedMetrics.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );

        assertEquals( expectedMetrics, actualMetrics );
    }

    // The testDeserializeAndInterpolate* tests are integration tests of deserialization plus interpolation

    @Test
    void testDeserializeAndInterpolateAllValidMetricsForSingleValuedTimeSeriesWithThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  sources:
                    - uri: another_file.csv
                  type: simulations
                value_thresholds: [23.0, 25.0]
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actual );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
        Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();

        Set<MetricConstants> expectedMetrics = new HashSet<>( singleValued );
        expectedMetrics.addAll( dichotomous );

        assertEquals( expectedMetrics, actualMetrics );
    }

    @Test
    void testDeserializeAndInterpolateAllValidMetricsForEnsembleTimeSeriesWithThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  sources:
                    - uri: another_file.csv
                  type: ensemble forecasts
                value_thresholds: [23.0, 25.0]
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actual );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> expectedMetrics = new HashSet<>( MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics() );
        expectedMetrics.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );

        assertEquals( expectedMetrics, actualMetrics );
    }

    @ParameterizedTest
    @ValueSource( strings = {
            """
                    observed:
                      - some_file.csv
                    predicted:
                      sources:
                        - uri: another_file.csv
                          interface: nwm medium range deterministic channel rt conus
                    """,
            """
                    observed:
                        - some_file.csv
                    predicted:
                        - another_file.csv
                    reference_dates:
                      minimum: 2023-02-01T00:00:00Z
                      maximum: 2062-02-01T00:00:00Z
                        """,
            """
                    observed:
                      - some_file.csv
                    predicted:
                      - another_file.csv
                    lead_time_pools:
                      period: 23
                      unit: days
                        """
    } )
    void testDeserializeAndInterpolatePredictedDatasetAsSingleValuedForecastWhenNoEnsembleDeclaration( String declaration )
            throws IOException
    {
        EvaluationDeclaration actual = DeclarationFactory.from( declaration );
        EvaluationDeclaration interpolated = DeclarationFactory.interpolate( actual );
        DataType actualType = interpolated.right()
                                          .type();
        assertEquals( DataType.SINGLE_VALUED_FORECASTS, actualType );
    }

    @ParameterizedTest
    @ValueSource( strings = {
            """
                    observed:
                      - some_file.csv
                    predicted:
                      - another_file.csv
                    metrics:
                      - continuous ranked probability score
                    """,
            """
                    observed:
                      - some_file.csv
                    predicted:
                      - another_file.csv
                    ensemble_average: mean
                    """,
            """
                    observed:
                      - some_file.csv
                    predicted:
                      sources:
                        - uri: another_file.csv
                      ensemble_filter: [ '2009' ]
                    """,
            """
                    observed:
                      - some_file.csv
                    predicted:
                      sources:
                        - uri: another_file.csv
                          interface: nwm medium range ensemble channel rt conus hourly
                    """
    } )
    void testDeserializeAndInterpolatePredictedDatasetAsEnsembleForecast( String declaration ) throws IOException
    {
        EvaluationDeclaration actual = DeclarationFactory.from( declaration );
        EvaluationDeclaration interpolated = DeclarationFactory.interpolate( actual );
        DataType actualType = interpolated.right()
                                          .type();
        assertEquals( DataType.ENSEMBLE_FORECASTS, actualType );
    }

    @Test
    void testDeserializeAndInterpolateObservedDatasetAsAnalysesWhenAnalysisDurationsDeclared() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                analysis_durations:
                  minimum_exclusive: 0
                  maximum: 0
                  unit: hours
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration interpolated = DeclarationFactory.interpolate( actual );
        DataType actualType = interpolated.left()
                                          .type();
        assertEquals( DataType.ANALYSES, actualType );
    }

    @Test
    void testDeserializeAndInterpolateWithShortAndLongFeatures() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                features:
                  - observed:
                      name: '09165000'
                      wkt: POINT (-67.945 46.8886111)
                    predicted: DRRC2
                  - observed: '09166500'
                    predicted:
                      name: DOLC2
                      wkt: POINT (-999 -998)
                  - CREC1
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actual );

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "09165000" )
                                                             .setWkt( "POINT (-67.945 46.8886111)" ) )
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder()
                                                              .setName( "09166500" ) )
                                            .setRight( Geometry.newBuilder()
                                                               .setName( DOLC2 )
                                                               .setWkt( "POINT (-999 -998)" ) )
                                            .build();

        GeometryTuple third = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "CREC1" ) )
                                           .setRight( Geometry.newBuilder()
                                                              .setName( "CREC1" ) )
                                           .build();

        // Insertion order
        Set<GeometryTuple> geometries = new LinkedHashSet<>();
        geometries.add( first );
        geometries.add( second );
        geometries.add( third );
        Features features = new Features( geometries );

        assertEquals( features, actualInterpolated.features() );
    }

    @Test
    void testDeserializeAndInterpolateAddsUnitToValueThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                unit: foo
                value_thresholds: [26,9,73]
                threshold_sets:
                  - value_thresholds: [23,2,45]
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [12, 24, 27]
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actual );

        assertAll( () -> assertEquals( "foo", actualInterpolated.valueThresholds()
                                                                .iterator()
                                                                .next()
                                                                .threshold()
                                                                .getThresholdValueUnits() ),
                   () -> assertEquals( "foo", actualInterpolated.thresholdSets()
                                                                .iterator()
                                                                .next()
                                                                .threshold()
                                                                .getThresholdValueUnits() ),
                   () -> assertEquals( "foo", actualInterpolated.metrics()
                                                                .iterator()
                                                                .next()
                                                                .parameters()
                                                                .valueThresholds()
                                                                .iterator()
                                                                .next()
                                                                .threshold()
                                                                .getThresholdValueUnits() )
        );
    }

    @Test
    void testDeserializeAndInterpolateAddsGraphicsFormatsWhenDeclaredByMetricsAlone() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                metrics:
                  - name: mean square error skill score
                    ensemble_average: median
                    png: true
                  - name: pearson correlation coefficient
                    ensemble_average: mean
                    svg: true
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actual );

        assertAll( () -> assertEquals( Formats.PNG_FORMAT, actualInterpolated.formats()
                                                                             .pngFormat() ),

                   () -> assertEquals( Formats.SVG_FORMAT, actualInterpolated.formats()
                                                                             .svgFormat() )
        );
    }

    @Test
    void testDeserializeAndInterpolateAddsThresholdsToMetrics() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                value_thresholds: [1]
                classifier_thresholds: [0.1]
                threshold_sets:
                  - value_thresholds: [2]
                  - classifier_thresholds: [0.2]
                metrics:
                  - mean square error skill score
                  - probability of detection
                  """;

        EvaluationDeclaration actualDeclaration = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationFactory.interpolate( actualDeclaration );

        Set<Metric> actual = actualInterpolated.metrics();

        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 1 ) )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .build();
        wres.config.yaml.components.Threshold oneWrapped =
                ThresholdBuilder.builder()
                                .threshold( one )
                                .type( ThresholdType.VALUE )
                                .build();

        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 2 ) )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .build();

        wres.config.yaml.components.Threshold twoWrapped =
                ThresholdBuilder.builder()
                                .threshold( two )
                                .type( ThresholdType.VALUE )
                                .build();

        // Preserve insertion order
        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( oneWrapped );
        valueThresholds.add( twoWrapped );
        valueThresholds.add( ALL_DATA_THRESHOLD );

        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .valueThresholds( valueThresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( firstParameters )
                                    .build();

        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                   .setOperator( Threshold.ThresholdOperator.GREATER )
                                   .build();
        wres.config.yaml.components.Threshold threeWrapped =
                ThresholdBuilder.builder()
                                .threshold( three )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        Threshold four = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.2 ) )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();

        wres.config.yaml.components.Threshold fourWrapped =
                ThresholdBuilder.builder()
                                .threshold( four )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        // Preserve insertion order
        Set<wres.config.yaml.components.Threshold> classifierThresholds = new LinkedHashSet<>();
        classifierThresholds.add( threeWrapped );
        classifierThresholds.add( fourWrapped );

        // Re-use the value thresholds without the all data threshold
        valueThresholds.remove( ALL_DATA_THRESHOLD );

        MetricParameters secondParameters =
                MetricParametersBuilder.builder()
                                       .valueThresholds( valueThresholds )
                                       .classifierThresholds( classifierThresholds )
                                       .build();
        Metric second = MetricBuilder.builder()
                                     .name( MetricConstants.PROBABILITY_OF_DETECTION )
                                     .parameters( secondParameters )
                                     .build();

        // Preserve iteration order
        Set<Metric> expected = new LinkedHashSet<>();
        expected.add( first );
        expected.add( second );

        assertEquals( expected, actual );
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
                      time_zone_offset: -0600
                      missing_value: -999.0
                    - uri: https://foo.bar
                      interface: usgs nwis
                      parameters:
                        foo: bar
                      time_scale:
                        function: mean
                        period: 3600
                        unit: seconds
                  variable: foo
                  type: observations
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone_offset: -0600
                    - uri: https://qux.quux
                      interface: wrds ahps
                      time_scale:
                        function: mean
                        period: 7200
                        unit: seconds
                  type: ensemble forecasts
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
                                                        .timeScale(
                                                                outerTimeScalePredicted )
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
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
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [0.3]
                    minimum_sample_size: 23
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
                                       .minimumSampleSize( 23 )
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
        Set<ComponentName> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( ComponentName.MEAN );
        summaryStatistics.add( ComponentName.MEDIAN );
        summaryStatistics.add( ComponentName.MINIMUM );

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
                  """;

        TimeInterval referenceDates = new TimeInterval( Instant.parse( "2551-03-17T00:00:00Z" ),
                                                        Instant.parse( "2551-03-20T00:00:00Z" ) );

        TimePools referenceDatePools = new TimePools( 13, 7, ChronoUnit.HOURS );

        TimeInterval validDates = new TimeInterval( Instant.parse( "2552-03-17T00:00:00Z" ),
                                                    Instant.parse( "2552-03-20T00:00:00Z" ) );

        TimePools validDatePools = new TimePools( 11, 2, ChronoUnit.HOURS );

        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( 0, 40, ChronoUnit.HOURS );
        TimePools leadTimePools = new TimePools( 23, 17, ChronoUnit.HOURS );

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
                  period: 3600
                  unit: seconds
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
                  period: 43200
                  unit: seconds
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
                  - netcdf2
                  - csv2
                  - csv
                  - pairs
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
        Formats formats = FormatsBuilder.builder()
                                        .netcdf2Format( Outputs.Netcdf2Format.getDefaultInstance() )
                                        .pairsFormat( Outputs.PairFormat.newBuilder()
                                                                        .setOptions( numericFormat )
                                                                        .build() )
                                        .csv2Format( Outputs.Csv2Format.newBuilder()
                                                                       .setOptions( numericFormat )
                                                                       .build() )
                                        .csvFormat( Outputs.CsvFormat.newBuilder()
                                                                     .setOptions( numericFormat )
                                                                     .build() )
                                        .pngFormat( Outputs.PngFormat.newBuilder()
                                                                     .setOptions( graphicFormat )
                                                                     .build() )
                                        .build();

        DecimalFormat formatter = new DecimalFormatPretty( "#0.000" );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .decimalFormat( formatter )
                                                                       .durationFormat( ChronoUnit.HOURS )
                                                                       .formats( formats )
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
                                                                            .featureName( "DRRC2" )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        wres.config.yaml.components.Threshold vTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vTwo )
                                                                            .type( ThresholdType.VALUE )
                                                                            .featureName( "DOLC2" )
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
                                                                            .featureName( "DRRC2" )
                                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                            .build();

        wres.config.yaml.components.Threshold cTwoWrapped = ThresholdBuilder.builder()
                                                                            .threshold( cTwo )
                                                                            .featureName( "DOLC2" )
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
}