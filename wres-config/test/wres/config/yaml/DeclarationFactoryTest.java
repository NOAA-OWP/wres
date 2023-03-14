package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.ThresholdBuilder;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;

/**
 * Tests the {@link DeclarationFactory}.
 *
 * @author James Brown
 */

class DeclarationFactoryTest
{
    @Test
    void testDeserializeWithShortSources() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - forecasts_with_NWS_feature_authority.csv
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        List<Source> observedSources = List.of( observedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .build();

        List<Source> predictedSources = List.of( predictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( observedDataset )
                                                                     .right( predictedDataset )
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
                      time_zone: CST
                      missing_value: -999.0
                    - uri: https://foo.bar
                      interface: usgs_nwis
                      time_scale:
                        function: mean
                        period: 1
                        unit: hours
                predicted:
                  sources:
                    - forecasts_with_NWS_feature_authority.csv
                    - uri: file:/some/other/directory
                      pattern: '**/*.xml*'
                      time_zone: CST
                    - uri: https://qux.quux
                      interface: wrds_ahps
                      time_scale:
                        function: mean
                        period: 2
                        unit: hours
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
                                                    .timeZone( "CST" )
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
                                                       .api( "usgs_nwis" )
                                                       .timeScale( outerTimeScale )
                                                       .build();

        List<Source> observedSources =
                List.of( observedSource, anotherObservedSource, yetAnotherObservedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        URI anotherPredictedUri = URI.create( "file:/some/other/directory" );
        Source anotherPredictedSource = SourceBuilder.builder()
                                                     .uri( anotherPredictedUri )
                                                     .pattern( "**/*.xml*" )
                                                     .timeZone( "CST" )
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
                                                        .api( "wrds_ahps" )
                                                        .timeScale(
                                                                outerTimeScalePredicted )
                                                        .build();

        List<Source> predictedSources =
                List.of( predictedSource, anotherPredictedSource, yetAnotherPredictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( observedDataset )
                                                                     .right( predictedDataset )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithShortAndLongFeatures() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - forecasts_with_NWS_feature_authority.csv
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

        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        List<Source> observedSources = List.of( observedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .build();

        List<Source> predictedSources = List.of( predictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .build();

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "09165000" )
                                                             .setWkt( "POINT (-67.945 46.8886111)" ) )
                                           .setRight( Geometry.newBuilder().setName( "DRRC2" ) )
                                           .build();

        GeometryTuple second = GeometryTuple.newBuilder()
                                            .setLeft( Geometry.newBuilder()
                                                              .setName( "09166500" ) )
                                            .setRight( Geometry.newBuilder()
                                                               .setName( "DOLC2" )
                                                               .setWkt( "POINT (-999 -998)" ) )
                                            .build();

        GeometryTuple third = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "CREC1" ) )
                                           .setRight( Geometry.newBuilder()
                                                              .setName( "CREC1" ) )
                                           .build();

        Set<GeometryTuple> geometries = Set.of( first, second, third );
        Features features = new Features( geometries );

        EvaluationDeclaration expected =
                EvaluationDeclarationBuilder.builder()
                                            .left( observedDataset )
                                            .right( predictedDataset )
                                            .features( features )
                                            .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithLongMetrics() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - forecasts_with_NWS_feature_authority.csv
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [0.3]
                    minimum_sample_size: 23
                  - name: pearson correlation coefficient
                    probability_thresholds:
                      values: [ 0.1 ]
                      operator: greater than or equal to
                      apply_to: observed
                  - name: time to peak error
                    summary_statistics:
                      - mean
                      - median
                      - minimum
                  """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI observedUri = URI.create( "some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        List<Source> observedSources = List.of( observedSource );
        Dataset observedDataset = DatasetBuilder.builder()
                                                .sources( observedSources )
                                                .build();

        List<Source> predictedSources = List.of( predictedSource );
        Dataset predictedDataset = DatasetBuilder.builder()
                                                 .sources( predictedSources )
                                                 .build();

        Threshold aValueThreshold = Threshold.newBuilder()
                                             .setLeftThresholdValue( DoubleValue.of( 0.3 ) )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .build();
        wres.config.yaml.components.Threshold valueThreshold =
                ThresholdBuilder.builder().threshold( aValueThreshold )
                                .build();
        Set<wres.config.yaml.components.Threshold> valueThresholds = Set.of( valueThreshold );
        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .minimumSampleSize( 23 )
                                       .valueThresholds( valueThresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( firstParameters )
                                    .build();

        Threshold aThreshold = Threshold.newBuilder()
                                        .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                        .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                        .build();
        wres.config.yaml.components.Threshold probabilityThreshold =
                ThresholdBuilder.builder().threshold( aThreshold )
                                .build();
        Set<wres.config.yaml.components.Threshold> probabilityThresholds = Set.of( probabilityThreshold );
        MetricParameters secondParameters =
                MetricParametersBuilder.builder()
                                       .probabilityThresholds( probabilityThresholds )
                                       .build();

        Metric second = MetricBuilder.builder()
                                     .name( MetricName.PEARSON_CORRELATION_COEFFICIENT )
                                     .parameters( secondParameters )
                                     .build();

        Set<ComponentName> summaryStatistics = Set.of( ComponentName.MEAN,
                                                       ComponentName.MEDIAN,
                                                       ComponentName.MINIMUM );
        MetricParameters thirdParameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .build();
        Metric third = MetricBuilder.builder()
                                    .name( MetricName.TIME_TO_PEAK_ERROR )
                                    .parameters( thirdParameters )
                                    .build();

        List<Metric> metrics = List.of( first, second, third );

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( observedDataset )
                                                                     .right( predictedDataset )
                                                                     .metrics( metrics )
                                                                     .build();

        assertEquals( expected, actual );
    }
}
