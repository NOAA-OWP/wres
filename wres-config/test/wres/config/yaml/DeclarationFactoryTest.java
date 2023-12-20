package wres.config.yaml;

import java.io.IOException;
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
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.CrossPairMethod;
import wres.config.yaml.components.CrossPairScope;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EnsembleFilterBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.FeatureServiceGroupBuilder;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.SamplingUncertainty;
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
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;
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
    void testDeserializeWithSingletonArraySources() throws IOException
    {
        String yaml = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSingletonSources() throws IOException
    {
        String yaml = """
                observed:
                  sources:
                    uri: some_file.csv
                    interface: usgs nwis
                predicted:
                  sources:
                    uri: another_file.csv
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        List<Source> leftSources = this.observedDataset.sources();
        List<Source> leftSourcesAdj = leftSources.stream()
                                                 .map( n -> SourceBuilder.builder( n )
                                                                         .sourceInterface( SourceInterface.USGS_NWIS )
                                                                         .build() )
                                                 .toList();
        Dataset left = DatasetBuilder.builder( this.observedDataset )
                                     .sources( leftSourcesAdj )
                                     .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( left )
                                                                     .right( this.predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithNoPredictedDatasetToSupportDataDirect() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted:
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithNoPredictedSourcesToSupportDataDirect() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted:
                  sources:
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( DatasetBuilder.builder().build() )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithNoPredictedSourcesAndSomeParametersToSupportDataDirect() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted:
                  label: "foo"
                  variable: "bar"
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( DatasetBuilder.builder()
                                                                  .label( "foo" )
                                                                  .variable( new Variable( "bar", null ) )
                                                                  .build() )
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
                      missing_value: [ -998.0, -999.0 ]
                    - uri: https://foo.bar
                      interface: usgs nwis
                      parameters:
                        foo: bar
                        baz: qux
                  type: observations
                  variable: foo
                  time_zone_offset: CST
                  time_scale:
                    function: mean
                    period: 1
                    unit: hours
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone_offset: -06:00
                    - uri: https://qux.quux
                      interface: wrds ahps
                  type: ensemble forecasts
                  time_scale:
                    function: mean
                    period: 2
                    unit: hours
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
                                                    .missingValue( List.of( -998.0, -999.0 ) )
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
                                                       .build();

        List<Source> observedSources =
                List.of( observedSource, anotherObservedSource, yetAnotherObservedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .type( DataType.OBSERVATIONS )
                                                .variable( new Variable( "foo", null ) )
                                                .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                .timeScale( outerTimeScale )
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
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
                                                 .timeScale( outerTimeScalePredicted )
                                                 .build();

        assertAll( () -> assertEquals( observedDataset, actual.left() ),
                   () -> assertEquals( predictedDataset, actual.right() )
        );
    }

    @Test
    void testDeserializeWithPersistenceBaseline() throws IOException
    {
        String declaration = """
                observed: some_file.csv
                predicted: another_file.csv
                baseline:
                  sources: yet_another_file.csv
                  method: persistence
                  """;

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .generatedBaseline( persistence )
                                                         .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .baseline( baseline )
                                                                     .build();

        EvaluationDeclaration actual = DeclarationFactory.from( declaration );

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithClimatologyBaselineAndParameters() throws IOException
    {
        String declaration = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                baseline:
                  sources: yet_another_file.csv
                  method:
                    name: climatology
                    average: median
                  """;

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .average( Pool.EnsembleAverageType.MEDIAN )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .generatedBaseline( persistence )
                                                         .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .baseline( baseline )
                                                                     .build();

        EvaluationDeclaration actual = DeclarationFactory.from( declaration );

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSingletonMetric() throws IOException
    {
        String declaration = """
                observed: some_file.csv
                predicted: another_file.csv
                metrics: pearson correlation coefficient
                  """;

        Metric metric = MetricBuilder.builder()
                                     .name( MetricConstants.PEARSON_CORRELATION_COEFFICIENT )
                                     .build();

        // Insertion order
        Set<Metric> metrics = Set.of( metric );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .metrics( metrics )
                                                                     .build();

        EvaluationDeclaration actual = DeclarationFactory.from( declaration );

        assertEquals( expected, actual );
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
                    thresholds: [0.3]
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
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( valueThreshold );
        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .thresholds( thresholds )
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
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        SummaryStatistic median = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                                .build();
        SummaryStatistic minimum = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MINIMUM )
                                                .build();
        summaryStatistics.add( mean );
        summaryStatistics.add( median );
        summaryStatistics.add( minimum );

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
    void testDeserializeWithFeaturesAndPredictedNameOnly() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                features:
                  - predicted: DRRC2
                  - predicted: DOLC2
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setRight( Geometry.newBuilder().setName( DRRC2 ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setRight( Geometry.newBuilder().setName( DOLC2 ) )
                                            .build();

        Set<GeometryTuple> geometries = Set.of( first, second );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .features( features )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeatureServiceAndNoGroups() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                feature_service:
                  uri: https://foo.service
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .featureService( featureService )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeatureServiceAndImplicitUri() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted: another_file.csv
                feature_service: https://foo.service
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        FeatureService featureService = FeatureServiceBuilder.builder()
                                                             .uri( URI.create( "https://foo.service" ) )
                                                             .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .featureService( featureService )
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
    void testDeserializeWithSpatialMask() throws IOException, ParseException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                spatial_mask:
                  name: a spatial mask!
                  wkt: POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225, -76.825 39.225))
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );


        String wkt = "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225, -76.825 39.225))";
        WKTReader reader = new WKTReader();
        org.locationtech.jts.geom.Geometry geometry = reader.read( wkt );
        SpatialMask expectedMask = new SpatialMask( "a spatial mask!", geometry );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .spatialMask( expectedMask )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithEnsembleFilter() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  sources: another_file.csv
                  ensemble_filter:
                    members: 23
                    exclude: true
                  """;

        EvaluationDeclaration deserialized = DeclarationFactory.from( yaml );

        EnsembleFilter actual = deserialized.right()
                                            .ensembleFilter();

        EnsembleFilter expected = EnsembleFilterBuilder.builder()
                                                       .members( Set.of( "23" ) )
                                                       .exclude( true )
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
                analysis_times:
                  minimum: 0
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

        AnalysisTimes analysisTimes = new AnalysisTimes( java.time.Duration.ZERO,
                                                         java.time.Duration.ofHours( 1 ) );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .referenceDates( referenceDates )
                                                                     .referenceDatePools( referenceDatePools )
                                                                     .validDates( validDates )
                                                                     .validDatePools( validDatePools )
                                                                     .leadTimes( leadTimeInterval )
                                                                     .leadTimePools( leadTimePools )
                                                                     .analysisTimes( analysisTimes )
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
    void testDeserializeWithSingletonArrayOfUnitAliases() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted: another_file.csv
                unit_aliases:
                  unit: '[degF]'
                  alias: °F
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        UnitAlias one = new UnitAlias( "°F", "[degF]" );
        Set<UnitAlias> unitAliases = Set.of( one );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
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
    void testDeserializeWithSamplingUncertainty() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                sampling_uncertainty:
                  sample_size: 1000
                  quantiles: [0.05,0.95]
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        SortedSet<Double> quantiles = new TreeSet<>();
        quantiles.add( 0.05 );
        quantiles.add( 0.95 );
        SamplingUncertainty samplingUncertainty = new SamplingUncertainty( quantiles, 1000 );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .sampleUncertainty( samplingUncertainty )
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
                                                                     .crossPair( new CrossPair( CrossPairMethod.EXACT,
                                                                                                null ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithCrossPairMethodExactAndScopeAcrossFeatures() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                cross_pair:
                  method: exact
                  scope: across features
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        EvaluationDeclaration expected =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .crossPair( new CrossPair( CrossPairMethod.EXACT,
                                                                       CrossPairScope.ACROSS_FEATURES ) )
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
    void testDeserializeWithSingletonOutputFormat() throws IOException
    {
        String yaml = """
                observed: some_file.csv
                predicted: another_file.csv
                output_formats: netcdf2
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Outputs formats = Outputs.newBuilder()
                                 .setNetcdf2( Outputs.Netcdf2Format.getDefaultInstance() )
                                 .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .formats( new Formats( formats ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeaturefulThresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                probability_thresholds: [0.1,0.2,0.9]
                thresholds:
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

        Set<wres.config.yaml.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( vOneWrapped );
        thresholds.add( vTwoWrapped );

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
                                                                     .thresholds( thresholds )
                                                                     .classifierThresholds( classifierThresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithFeaturefulThresholdsAndPredictedOrientation() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                features:
                  - observed: DRRC2
                    predicted: FOO
                thresholds:
                  name: MAJOR FLOOD
                  values:
                    - { value: 23.0, feature: FOO }
                  operator: greater
                  apply_to: predicted
                  feature_name_from: predicted
                """;

        EvaluationDeclaration actualEvaluation = DeclarationFactory.from( yaml );

        Threshold vOne = Threshold.newBuilder()
                                  .setLeftThresholdValue( DoubleValue.of( 23.0 ) )
                                  .setDataType( Threshold.ThresholdDataType.RIGHT )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .setName( "MAJOR FLOOD" )
                                  .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .feature( Geometry.newBuilder()
                                                                                              .setName( "FOO" )
                                                                                              .build() )
                                                                            .type( ThresholdType.VALUE )
                                                                            .featureNameFrom( DatasetOrientation.RIGHT )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> expected = Set.of( vOneWrapped );

        Set<wres.config.yaml.components.Threshold> actual = actualEvaluation.thresholds();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithMultipleSetsOfthresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                thresholds:
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

        Set<wres.config.yaml.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( vOneWrapped );
        thresholds.add( vTwoWrapped );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .thresholds( thresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSingletonValueThreshold() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                thresholds: 0.1
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold vOne =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder()
                                                              .setLeftThresholdValue( DoubleValue.of( 0.1 ) )
                                                              .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .type( ThresholdType.VALUE )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( vOneWrapped );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .thresholds( thresholds )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSingletonProbabilityThresholdAndOperator() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                probability_thresholds:
                  values: 0.5
                  operator: greater equal
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Threshold vOne =
                DeclarationFactory.DEFAULT_CANONICAL_THRESHOLD.toBuilder()
                                                              .setLeftThresholdProbability( DoubleValue.of( 0.5 ) )
                                                              .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                                              .build();

        wres.config.yaml.components.Threshold vOneWrapped = ThresholdBuilder.builder()
                                                                            .threshold( vOne )
                                                                            .type( ThresholdType.PROBABILITY )
                                                                            .build();

        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( vOneWrapped );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .probabilityThresholds( thresholds )
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

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        SummaryStatistic median = SummaryStatistic.newBuilder()
                                                  .setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                                  .build();
        SummaryStatistic meanAbsolute = SummaryStatistic.newBuilder()
                                                        .setStatistic( SummaryStatistic.StatisticName.MEAN_ABSOLUTE )
                                                        .build();
        SummaryStatistic minimum = SummaryStatistic.newBuilder()
                                                   .setStatistic( SummaryStatistic.StatisticName.MINIMUM )
                                                   .build();
        SummaryStatistic maximum = SummaryStatistic.newBuilder()
                                                   .setStatistic( SummaryStatistic.StatisticName.MAXIMUM )
                                                   .build();
        SummaryStatistic sd = SummaryStatistic.newBuilder()
                                              .setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                              .build();
        summaryStatistics.add( mean );
        summaryStatistics.add( median );
        summaryStatistics.add( meanAbsolute );
        summaryStatistics.add( minimum );
        summaryStatistics.add( maximum );
        summaryStatistics.add( sd );

        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
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
    void testDeserializeWithSingletonThresholdSource() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                threshold_sources: https://foo
                  """;

        EvaluationDeclaration actualEvaluation = DeclarationFactory.from( yaml );
        Set<ThresholdSource> actual = actualEvaluation.thresholdSources();

        ThresholdSource thresholdSourceOne = ThresholdSourceBuilder.builder()
                                                                   .uri( URI.create( "https://foo" ) )
                                                                   .build();

        Set<ThresholdSource> expected = Set.of( thresholdSourceOne );

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSingletonThresholdSourceAndProperties() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                threshold_sources:
                  uri: https://foo
                  provider: bar
                  rating_provider: baz
                  parameter: moon
                  missing_value: -9999999
                  unit: qux
                  feature_name_from: observed
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        ThresholdSource thresholdSource = ThresholdSourceBuilder.builder()
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
                                                                     .thresholdSources( Set.of( thresholdSource ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithMultipleThresholdSources() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                threshold_sources:
                  - https://foo
                  - uri: https://bar
                  - https://baz
                  """;

        EvaluationDeclaration actualEvaluation = DeclarationFactory.from( yaml );
        Set<ThresholdSource> actual = actualEvaluation.thresholdSources();

        ThresholdSource thresholdSourceOne = ThresholdSourceBuilder.builder()
                                                                   .uri( URI.create( "https://foo" ) )
                                                                   .build();

        ThresholdSource thresholdSourceTwo = ThresholdSourceBuilder.builder()
                                                                   .uri( URI.create( "https://bar" ) )
                                                                   .build();

        ThresholdSource thresholdSourceThree = ThresholdSourceBuilder.builder()
                                                                     .uri( URI.create( "https://baz" ) )
                                                                     .build();

        Set<ThresholdSource> expected = Set.of( thresholdSourceOne, thresholdSourceTwo, thresholdSourceThree );

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithTimingErrorSummaryStatisticsAndThresholdsPerMetric() throws IOException
    {
        String yaml = """
                    observed:
                      - some_file.csv
                    predicted:
                      - another_file.csv
                    metrics:
                      - name: time to peak error
                        thresholds:
                          values: [183.0, 184.0]
                          apply_to: observed and predicted
                        summary_statistics:
                          - mean
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        // Preserve insertion order
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        summaryStatistics.add( mean );

        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 183.0 ) )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                 .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( DoubleValue.of( 184.0 ) )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                 .build();

        wres.config.yaml.components.Threshold oneWrapped = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();

        wres.config.yaml.components.Threshold twoWrapped = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();

        // Insertion order
        Set<wres.config.yaml.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( oneWrapped );
        thresholds.add( twoWrapped );

        MetricParameters parameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .thresholds( thresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( parameters )
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
    void testDeserializeWithSummaryStatistics() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                summary_statistics:
                  - mean
                  - quantiles
                  - histogram
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        SummaryStatistic quantileOne = SummaryStatistic.newBuilder()
                                                       .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                       .setProbability( 0.1 )
                                                       .build();
        SummaryStatistic quantileTwo = SummaryStatistic.newBuilder()
                                                       .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                       .setProbability( 0.5 )
                                                       .build();
        SummaryStatistic quantileThree = SummaryStatistic.newBuilder()
                                                       .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                       .setProbability( 0.9 )
                                                       .build();
        SummaryStatistic histogram = SummaryStatistic.newBuilder()
                                                     .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                     .setHistogramBins( DeclarationFactory.DEFAULT_HISTOGRAM_BINS )
                                                     .build();

        summaryStatistics.add( mean );
        summaryStatistics.add( quantileOne );
        summaryStatistics.add( quantileTwo );
        summaryStatistics.add( quantileThree );
        summaryStatistics.add( histogram );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .summaryStatistics( summaryStatistics )
                                                                     .build();
        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithSummaryStatisticsAndParameters() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                summary_statistics:
                  - mean
                  - name: quantiles
                    probabilities: [0.05,0.95]
                  - name: histogram
                    bins: 5
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        SummaryStatistic quantileOne = SummaryStatistic.newBuilder()
                                                       .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                       .setProbability( 0.05 )
                                                       .build();
        SummaryStatistic quantileTwo = SummaryStatistic.newBuilder()
                                                       .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                       .setProbability( 0.95 )
                                                       .build();
        SummaryStatistic histogram = SummaryStatistic.newBuilder()
                                                     .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                     .setHistogramBins( 5 )
                                                     .build();

        summaryStatistics.add( mean );
        summaryStatistics.add( quantileOne );
        summaryStatistics.add( quantileTwo );
        summaryStatistics.add( histogram );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( this.observedDataset )
                                                                     .right( this.predictedDataset )
                                                                     .summaryStatistics( summaryStatistics )
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

            EvaluationDeclaration actual = DeclarationFactory.from( pathString, fileSystem, false, false );

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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                baseline:
                  sources: yet_another_file.csv
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
    void testSerializeWithPersistenceBaseline() throws IOException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                baseline:
                  sources: yet_another_file.csv
                  method: persistence
                  """;

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .generatedBaseline( persistence )
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
    void testSerializeWithClimatologyBaselineAndParameters() throws IOException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                baseline:
                  sources: yet_another_file.csv
                  method:
                    name: climatology
                    average: median
                  """;

        URI baselineUri = URI.create( "yet_another_file.csv" );
        Source baselineSource = SourceBuilder.builder()
                                             .uri( baselineUri )
                                             .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.CLIMATOLOGY )
                                                                .average( Pool.EnsembleAverageType.MEDIAN )
                                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .generatedBaseline( persistence )
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
                  variable:
                    name: foo
                    label: fooest
                  type: observations
                  time_scale:
                    function: mean
                    period: 1
                    unit: hours
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone_offset: "-0600"
                    - uri: https://qux.quux
                      interface: wrds ahps
                  type: ensemble forecasts
                  time_shift:
                    period: -2
                    unit: hours
                  time_scale:
                    function: mean
                    period: 2
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
                                                    .missingValue( List.of( -999.0 ) )
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
                                                       .build();

        List<Source> observedSources =
                List.of( observedSource, anotherObservedSource, yetAnotherObservedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .type( DataType.OBSERVATIONS )
                                                .variable( new Variable( "foo", "fooest" ) )
                                                .timeScale( outerTimeScale )
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
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .type( DataType.ENSEMBLE_FORECASTS )
                                                 .timeShift( java.time.Duration.ofHours( -2 ) )
                                                 .timeScale( outerTimeScalePredicted )
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                minimum_sample_size: 23
                metrics:
                  - name: mean square error skill score
                    thresholds: 0.3
                  - name: pearson correlation coefficient
                    probability_thresholds:
                      values: 0.1
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
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( valueThreshold );
        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .thresholds( thresholds )
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

        // Preserve insertion order
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic.Builder template = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS );
        SummaryStatistic mean = template.setStatistic( SummaryStatistic.StatisticName.MEAN )
                                        .build();
        SummaryStatistic median = template.setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                          .build();
        SummaryStatistic minimum = template.setStatistic( SummaryStatistic.StatisticName.MINIMUM )
                                           .build();
        summaryStatistics.add( mean );
        summaryStatistics.add( median );
        summaryStatistics.add( minimum );

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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                feature_groups:
                  - name: a group
                    features:
                      - {observed: DRRC2, predicted: DRRC2}
                      - {observed: DOLC2, predicted: DOLC2}
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
    void testSerializeWithSpatialMask() throws IOException, ParseException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                spatial_mask:
                  name: a spatial mask!
                  wkt: "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225, -76.825 39.225))"
                  """;

        WKTReader reader = new WKTReader();
        String wkt = "POLYGON ((-76.825 39.225, -76.825 39.275, -76.775 39.275, -76.775 39.225, -76.825 39.225))";
        org.locationtech.jts.geom.Geometry geometry = reader.read( wkt );
        SpatialMask expectedMask = new SpatialMask( "a spatial mask!", geometry );

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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                analysis_times:
                  minimum: 0
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

        AnalysisTimes analysisTimes = new AnalysisTimes( java.time.Duration.ZERO,
                                                         java.time.Duration.ofHours( 1 ) );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .referenceDates( referenceDates )
                                                                       .referenceDatePools( referenceDatePools )
                                                                       .validDates( validDates )
                                                                       .validDatePools( validDatePools )
                                                                       .leadTimes( leadTimeInterval )
                                                                       .leadTimePools( leadTimePools )
                                                                       .analysisTimes( analysisTimes )
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                cross_pair: exact
                  """;

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .crossPair( new CrossPair( CrossPairMethod.EXACT,
                                                                                                  null ) )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithCrossPairMethodFuzzyAndScopeWithinFeatures() throws IOException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                cross_pair:
                  method: fuzzy
                  scope: within features
                  """;

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .crossPair( new CrossPair( CrossPairMethod.FUZZY,
                                                                       CrossPairScope.WITHIN_FEATURES ) )
                                            .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithEnsembleAverage() throws IOException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                thresholds:
                  name: MAJOR FLOOD
                  values:
                    - {value: 27.0, feature: DOLC2}
                    - {value: 23.0, feature: DRRC2}
                  apply_to: predicted
                probability_thresholds: [0.1, 0.2, 0.9]
                classifier_thresholds:
                  name: COLONEL DROUGHT
                  values:
                    - {value: 0.3, feature: DOLC2}
                    - {value: 0.2, feature: DRRC2}
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

        Set<wres.config.yaml.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( vOneWrapped );
        thresholds.add( vTwoWrapped );

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
                                                                       .thresholds( thresholds )
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
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
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                baseline:
                  sources: another_file.csv
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
    void testSerializeWithThresholdSources() throws IOException
    {
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                threshold_sources:
                  uri: https://foo
                  parameter: moon
                  unit: qux
                  provider: bar
                  rating_provider: baz
                  missing_value: -9999999.0
                  """;

        ThresholdSource thresholdSource = ThresholdSourceBuilder.builder()
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
                                                                       .thresholdSources( Set.of( thresholdSource ) )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }

    @Test
    void testSerializeWithMultipleSetsOfClassifierThresholds() throws IOException
    {
        // #120048
        String expected = """
                observed:
                  sources: some_file.csv
                predicted:
                  sources: another_file.csv
                classifier_thresholds:
                  - values: [0.05, 0.1]
                    operator: less equal
                    apply_to: observed and predicted
                  - values: [0.05, 0.1]
                    operator: equal
                    apply_to: any predicted
                """;

        Threshold pOne = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.05 ) )
                                  .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                  .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                  .build();

        wres.config.yaml.components.Threshold pOneWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pOne )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Threshold pTwo = Threshold.newBuilder()
                                  .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                  .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                  .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                  .build();

        wres.config.yaml.components.Threshold pTwoWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pTwo )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Threshold pThree = Threshold.newBuilder()
                                    .setLeftThresholdProbability( DoubleValue.of( 0.05 ) )
                                    .setOperator( Threshold.ThresholdOperator.EQUAL )
                                    .setDataType( Threshold.ThresholdDataType.ANY_RIGHT )
                                    .build();

        wres.config.yaml.components.Threshold pThreeWrapped =
                ThresholdBuilder.builder()
                                .threshold( pThree )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        Threshold pFour = Threshold.newBuilder()
                                   .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                   .setOperator( Threshold.ThresholdOperator.EQUAL )
                                   .setDataType( Threshold.ThresholdDataType.ANY_RIGHT )
                                   .build();

        wres.config.yaml.components.Threshold pFourWrapped =
                ThresholdBuilder.builder()
                                .threshold( pFour )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        // Thresholds in insertion order
        Set<wres.config.yaml.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( pOneWrapped );
        thresholds.add( pTwoWrapped );
        thresholds.add( pThreeWrapped );
        thresholds.add( pFourWrapped );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( this.observedDataset )
                                                                       .right( this.predictedDataset )
                                                                       .classifierThresholds( thresholds )
                                                                       .build();

        String actual = DeclarationFactory.from( evaluation );

        assertEquals( expected, actual );
    }
}