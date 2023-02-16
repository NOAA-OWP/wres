package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import com.google.protobuf.DoubleValue;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
                left:
                  - some_file.csv
                right:
                  - forecasts_with_NWS_feature_authority.csv
                  """;

        DeclarationFactory.EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI leftUri = URI.create( "some_file.csv" );
        DeclarationFactory.Source leftSource = DeclarationFactorySourceBuilder.builder()
                                                                              .uri( leftUri )
                                                                              .build();

        URI rightUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        DeclarationFactory.Source rightSource = DeclarationFactorySourceBuilder.builder()
                                                                               .uri( rightUri )
                                                                               .build();

        List<DeclarationFactory.Source> leftSources = List.of( leftSource );
        DeclarationFactory.Dataset leftDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                 .sources( leftSources )
                                                                                 .build();

        List<DeclarationFactory.Source> rightSources = List.of( rightSource );
        DeclarationFactory.Dataset rightDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                  .sources( rightSources )
                                                                                  .build();

        DeclarationFactory.EvaluationDeclaration expected =
                DeclarationFactoryEvaluationDeclarationBuilder.builder()
                                                              .left( leftDataset )
                                                              .right( rightDataset )
                                                              .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithLongSources() throws IOException
    {
        String yaml = """
                left:
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
                right:
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

        DeclarationFactory.EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI leftUri = URI.create( "some_file.csv" );
        DeclarationFactory.Source leftSource = DeclarationFactorySourceBuilder.builder()
                                                                              .uri( leftUri )
                                                                              .build();
        URI anotherLeftUri = URI.create( "file:/some/directory" );
        DeclarationFactory.Source anotherLeftSource = DeclarationFactorySourceBuilder.builder()
                                                                                     .uri( anotherLeftUri )
                                                                                     .pattern( "**/*.csv*" )
                                                                                     .timeZone( "CST" )
                                                                                     .missingValue( -999.0 )
                                                                                     .build();

        URI yetAnotherLeftUri = URI.create( "https://foo.bar" );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( Duration.newBuilder().setSeconds( 3600 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();
        DeclarationFactory.Source yetAnotherLeftSource = DeclarationFactorySourceBuilder.builder()
                                                                                        .uri( yetAnotherLeftUri )
                                                                                        .api( "usgs_nwis" )
                                                                                        .timeScale( timeScale )
                                                                                        .build();

        List<DeclarationFactory.Source> leftSources = List.of( leftSource, anotherLeftSource, yetAnotherLeftSource );
        DeclarationFactory.Dataset leftDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                 .sources( leftSources )
                                                                                 .build();

        URI rightUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        DeclarationFactory.Source rightSource = DeclarationFactorySourceBuilder.builder()
                                                                               .uri( rightUri )
                                                                               .build();

        URI anotherRightUri = URI.create( "file:/some/other/directory" );
        DeclarationFactory.Source anotherRightSource = DeclarationFactorySourceBuilder.builder()
                                                                                      .uri( anotherRightUri )
                                                                                      .pattern( "**/*.xml*" )
                                                                                      .timeZone( "CST" )
                                                                                      .build();

        URI yetAnotherRightUri = URI.create( "https://qux.quux" );
        TimeScale timeScaleRight = TimeScale.newBuilder()
                                            .setPeriod( Duration.newBuilder().setSeconds( 7200 ) )
                                            .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                            .build();
        DeclarationFactory.Source yetAnotherRightSource = DeclarationFactorySourceBuilder.builder()
                                                                                         .uri( yetAnotherRightUri )
                                                                                         .api( "wrds_ahps" )
                                                                                         .timeScale( timeScaleRight )
                                                                                         .build();


        List<DeclarationFactory.Source> rightSources =
                List.of( rightSource, anotherRightSource, yetAnotherRightSource );
        DeclarationFactory.Dataset rightDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                  .sources( rightSources )
                                                                                  .build();

        DeclarationFactory.EvaluationDeclaration expected =
                DeclarationFactoryEvaluationDeclarationBuilder.builder()
                                                              .left( leftDataset )
                                                              .right( rightDataset )
                                                              .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithShortAndLongFeatures() throws IOException
    {
        String yaml = """
                left:
                  - some_file.csv
                right:
                  - forecasts_with_NWS_feature_authority.csv
                features:
                  - left:
                      name: '09165000'
                      wkt: POINT (-67.945 46.8886111)
                    right: DRRC2
                  - left: '09166500'
                    right:
                      name: DOLC2
                      wkt: POINT (-999 -998)
                  - CREC1
                  """;

        DeclarationFactory.EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI leftUri = URI.create( "some_file.csv" );
        DeclarationFactory.Source leftSource = DeclarationFactorySourceBuilder.builder()
                                                                              .uri( leftUri )
                                                                              .build();

        URI rightUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        DeclarationFactory.Source rightSource = DeclarationFactorySourceBuilder.builder()
                                                                               .uri( rightUri )
                                                                               .build();

        List<DeclarationFactory.Source> leftSources = List.of( leftSource );
        DeclarationFactory.Dataset leftDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                 .sources( leftSources )
                                                                                 .build();

        List<DeclarationFactory.Source> rightSources = List.of( rightSource );
        DeclarationFactory.Dataset rightDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                  .sources( rightSources )
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

        Set<GeometryTuple> features = Set.of( first, second, third );

        DeclarationFactory.EvaluationDeclaration expected =
                DeclarationFactoryEvaluationDeclarationBuilder.builder()
                                                              .left( leftDataset )
                                                              .right( rightDataset )
                                                              .features( features )
                                                              .build();

        assertEquals( expected, actual );
    }

    @Test
    void testDeserializeWithLongMetrics() throws IOException
    {
        String yaml = """
                left:
                  - some_file.csv
                right:
                  - forecasts_with_NWS_feature_authority.csv
                metrics:
                  - name: mean square error skill score
                    value_thresholds: [0.3]
                    minimum_sample_size: 23
                  - name: pearson correlation coefficient
                    probability_thresholds:
                      values: [ 0.1 ]
                      operator: greater than or equal to
                      apply_to: left
                  - name: time to peak error
                    summary_statistics:
                      - mean
                      - median
                      - minimum
                  """;

        DeclarationFactory.EvaluationDeclaration actual = DeclarationFactory.from( yaml );

        URI leftUri = URI.create( "some_file.csv" );
        DeclarationFactory.Source leftSource = DeclarationFactorySourceBuilder.builder()
                                                                              .uri( leftUri )
                                                                              .build();

        URI rightUri = URI.create( "forecasts_with_NWS_feature_authority.csv" );
        DeclarationFactory.Source rightSource = DeclarationFactorySourceBuilder.builder()
                                                                               .uri( rightUri )
                                                                               .build();

        List<DeclarationFactory.Source> leftSources = List.of( leftSource );
        DeclarationFactory.Dataset leftDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                 .sources( leftSources )
                                                                                 .build();

        List<DeclarationFactory.Source> rightSources = List.of( rightSource );
        DeclarationFactory.Dataset rightDataset = DeclarationFactoryDatasetBuilder.builder()
                                                                                  .sources( rightSources )
                                                                                  .build();

        Threshold aValueThreshold = Threshold.newBuilder()
                                             .setLeftThresholdValue( DoubleValue.of( 0.3 ) )
                                             .setOperator( Threshold.ThresholdOperator.GREATER )
                                             .build();
        DeclarationFactory.Threshold valueThreshold =
                DeclarationFactoryThresholdBuilder.builder().threshold( aValueThreshold )
                                                  .build();
        Set<DeclarationFactory.Threshold> valueThresholds = Set.of( valueThreshold );
        DeclarationFactory.MetricParameters firstParameters =
                DeclarationFactoryMetricParametersBuilder.builder()
                                                         .minimumSampleSize( 23 )
                                                         .valueThresholds( valueThresholds )
                                                         .build();
        DeclarationFactory.Metric first = DeclarationFactoryMetricBuilder.builder()
                                                                         .name( MetricName.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                                                         .parameters( firstParameters )
                                                                         .build();

        Threshold aThreshold = Threshold.newBuilder()
                                        .setLeftThresholdProbability( DoubleValue.of( 0.1 ) )
                                        .setOperator( Threshold.ThresholdOperator.GREATER_EQUAL )
                                        .build();
        DeclarationFactory.Threshold probabilityThreshold =
                DeclarationFactoryThresholdBuilder.builder().threshold( aThreshold )
                                                  .build();
        Set<DeclarationFactory.Threshold> probabilityThresholds = Set.of( probabilityThreshold );
        DeclarationFactory.MetricParameters secondParameters =
                DeclarationFactoryMetricParametersBuilder.builder()
                                                         .probabilityThresholds( probabilityThresholds )
                                                         .build();

        DeclarationFactory.Metric second = DeclarationFactoryMetricBuilder.builder()
                                                                          .name( MetricName.PEARSON_CORRELATION_COEFFICIENT )
                                                                          .parameters( secondParameters )
                                                                          .build();

        Set<ComponentName> summaryStatistics = Set.of( ComponentName.MEAN,
                                                       ComponentName.MEDIAN,
                                                       ComponentName.MINIMUM );
        DeclarationFactory.MetricParameters thirdParameters =
                DeclarationFactoryMetricParametersBuilder.builder()
                                                         .summaryStatistics( summaryStatistics )
                                                         .build();
        DeclarationFactory.Metric third = DeclarationFactoryMetricBuilder.builder()
                                                                         .name( MetricName.TIME_TO_PEAK_ERROR )
                                                                         .parameters( thirdParameters )
                                                                         .build();

        List<DeclarationFactory.Metric> metrics = List.of( first, second, third );

        DeclarationFactory.EvaluationDeclaration expected =
                DeclarationFactoryEvaluationDeclarationBuilder.builder()
                                                              .left( leftDataset )
                                                              .right( rightDataset )
                                                              .metrics( metrics )
                                                              .build();

        assertEquals( expected, actual );
    }
}
