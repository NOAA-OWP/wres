package wres.config.yaml;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.DoubleValue;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisDurationsBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link DeclarationUtilitiesTest}.
 * @author James Brown
 */
class DeclarationUtilitiesTest
{
    @Test
    void testHasBaselineReturnsTrue()
    {
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( DatasetBuilder.builder()
                                                                                 .build() )
                                                         .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .baseline( baseline )
                                                                       .build();

        assertTrue( DeclarationUtilities.hasBaseline( evaluation ) );
    }

    @Test
    void testHasBaselineReturnsFalse()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .build();

        assertFalse( DeclarationUtilities.hasBaseline( evaluation ) );
    }

    @Test
    void testGetFeatures()
    {
        GeometryTuple singleton = GeometryTuple.newBuilder()
                                               .setLeft( Geometry.newBuilder()
                                                                 .setName( "foo" ) )
                                               .setRight( Geometry.newBuilder()
                                                                  .setName( "bar" ) )
                                               .build();
        GeometryTuple grouped = GeometryTuple.newBuilder()
                                             .setLeft( Geometry.newBuilder()
                                                               .setName( "baz" ) )
                                             .setRight( Geometry.newBuilder()
                                                                .setName( "qux" ) )
                                             .build();

        Features features = new Features( Set.of( singleton ) );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( Set.of( grouped ) )
                                           .build();
        FeatureGroups featureGroups = new FeatureGroups( Set.of( group ) );
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .features( features )
                                                                       .featureGroups( featureGroups )
                                                                       .build();
        Set<GeometryTuple> expected = Set.of( singleton, grouped );

        assertEquals( expected, DeclarationUtilities.getFeatures( evaluation ) );
    }

    @Test
    void testFromEnumName()
    {
        String from = "An_eNum_nAmE";
        String expected = "an enum name";
        assertEquals( expected, DeclarationUtilities.fromEnumName( from ) );
    }

    @Test
    void testToEnumName()
    {
        String from = "an enum name";
        String expected = "AN_ENUM_NAME";
        assertEquals( expected, DeclarationUtilities.toEnumName( from ) );
    }

    @Test
    void testGetDurationInPreferredUnitsReturnsHours()
    {
        Duration duration = Duration.ofHours( 3 );
        Pair<Long, String> preferred = DeclarationUtilities.getDurationInPreferredUnits( duration );
        assertEquals( Pair.of( 3L, "hours" ), preferred );
    }

    @Test
    void testGetDurationInPreferredUnitsReturnsSeconds()
    {
        Duration duration = Duration.ofMinutes( 12 );
        Pair<Long, String> preferred = DeclarationUtilities.getDurationInPreferredUnits( duration );
        assertEquals( Pair.of( 720L, "seconds" ), preferred );
    }

    @Test
    void testGetFeatureNamesFor()
    {
        Geometry left = Geometry.newBuilder()
                                .setName( "foo" )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( "bar" )
                                 .build();
        Geometry baseline = Geometry.newBuilder()
                                    .setName( "baz" )
                                    .build();

        GeometryTuple singleton = GeometryTuple.newBuilder()
                                               .setLeft( left )
                                               .setRight( right )
                                               .setBaseline( baseline )
                                               .build();

        Set<GeometryTuple> singletonSet = Set.of( singleton );

        assertAll( () -> assertEquals( Set.of( "foo" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.LEFT ) ),
                   () -> assertEquals( Set.of( "bar" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( Set.of( "baz" ),
                                       DeclarationUtilities.getFeatureNamesFor( singletonSet,
                                                                                DatasetOrientation.BASELINE ) ) );

    }

    @Test
    void testGetFeatureAuthorityFor()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                      .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                                              .build() )
                                      .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( left )
                                                                       .right( right )
                                                                       .baseline( baseline )
                                                                       .build();

        assertAll( () -> assertEquals( FeatureAuthority.USGS_SITE_CODE,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.LEFT ) ),
                   () -> assertEquals( FeatureAuthority.NWM_FEATURE_ID,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.RIGHT ) ),
                   () -> assertEquals( FeatureAuthority.NWS_LID,
                                       DeclarationUtilities.getFeatureAuthorityFor( evaluation,
                                                                                    DatasetOrientation.BASELINE ) ) );
    }

    @Test
    void testHasBaselineBuilderReturnsTrue()
    {
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( DatasetBuilder.builder()
                                                                                 .build() )
                                                         .build();
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder()
                                                                           .baseline( baseline );

        assertTrue( DeclarationUtilities.hasBaseline( builder ) );
    }

    @Test
    void testHasBaselineBuilderReturnsFalse()
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder();

        assertFalse( DeclarationUtilities.hasBaseline( builder ) );
    }

    @Test
    void testGetFeatureAuthoritiesReturnsExplicitAuthorityForEachDataset()
    {
        Dataset left = DatasetBuilder.builder()
                                     .featureAuthority( FeatureAuthority.USGS_SITE_CODE )
                                     .label( "foo" )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .featureAuthority( FeatureAuthority.NWM_FEATURE_ID )
                                      .label( "bar" )
                                      .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .featureAuthority( FeatureAuthority.NWS_LID )
                                                              .label( "baz" )
                                                              .build() )
                                      .build();

        assertAll( () -> assertEquals( Set.of( FeatureAuthority.USGS_SITE_CODE ),
                                       DeclarationUtilities.getFeatureAuthorities( left ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWM_FEATURE_ID ),
                                       DeclarationUtilities.getFeatureAuthorities( right ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWS_LID ),
                                       DeclarationUtilities.getFeatureAuthorities( baseline.dataset() ) ) );
    }

    @Test
    void testGetFeatureAuthoritiesReturnsInterpolatedAuthorityForEachDataset()
    {
        List<wres.config.yaml.components.Source> leftSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface( SourceInterface.USGS_NWIS )
                                      .build() );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .label( "foo" )
                                     .build();
        List<wres.config.yaml.components.Source> rightSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface( SourceInterface.NWM_LONG_RANGE_CHANNEL_RT_CONUS )
                                      .build() );
        Dataset right =
                DatasetBuilder.builder()
                              .sources( rightSources )
                              .label( "bar" )
                              .build();
        List<wres.config.yaml.components.Source> baselineSources =
                List.of( SourceBuilder.builder()
                                      .sourceInterface(
                                              SourceInterface.WRDS_AHPS )
                                      .build() );
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .label( "baz" )
                                                              .build() )
                                      .build();

        assertAll( () -> assertEquals( Set.of( FeatureAuthority.USGS_SITE_CODE ),
                                       DeclarationUtilities.getFeatureAuthorities( left ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWM_FEATURE_ID ),
                                       DeclarationUtilities.getFeatureAuthorities( right ) ),
                   () -> assertEquals( Set.of( FeatureAuthority.NWS_LID ),
                                       DeclarationUtilities.getFeatureAuthorities( baseline.dataset() ) ) );
    }

    @Test
    void testHasAnalysisDurationsReturnsTrue()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisDurations( AnalysisDurationsBuilder.builder()
                                                                                                                   .minimumExclusive(
                                                                                                                           3 )
                                                                                                                   .unit( ChronoUnit.HOURS )
                                                                                                                   .build() )
                                                                       .build();

        assertTrue( DeclarationUtilities.hasAnalysisDurations( evaluation ) );
    }

    @Test
    void testHasAnalysisDurationsReturnsFalse()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .build();

        assertFalse( DeclarationUtilities.hasAnalysisDurations( evaluation ) );
    }

    @Test
    void testGroupThresholdsByType()
    {
        Threshold probability = Threshold.newBuilder()
                                         .setName( "foo" )
                                         .build();
        Threshold value = Threshold.newBuilder()
                                   .setName( "bar" )
                                   .build();
        Threshold classifier = Threshold.newBuilder()
                                        .setName( "foo" )
                                        .build();
        wres.config.yaml.components.Threshold probabilityWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.PROBABILITY )
                                                            .threshold( probability )
                                                            .build();
        wres.config.yaml.components.Threshold valueWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.VALUE )
                                                            .threshold( value )
                                                            .build();
        wres.config.yaml.components.Threshold classifierWrapped =
                wres.config.yaml.components.ThresholdBuilder.builder()
                                                            .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                            .threshold( classifier )
                                                            .build();
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( probabilityWrapped,
                                                                        valueWrapped,
                                                                        classifierWrapped );

        Map<ThresholdType, Set<wres.config.yaml.components.Threshold>> actual =
                DeclarationUtilities.groupThresholdsByType( thresholds );

        Map<ThresholdType, Set<wres.config.yaml.components.Threshold>> expected =
                Map.of( ThresholdType.PROBABILITY, Set.of( probabilityWrapped ),
                        ThresholdType.VALUE, Set.of( valueWrapped ),
                        ThresholdType.PROBABILITY_CLASSIFIER, Set.of( classifierWrapped ) );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMetricGroupsForProcessing()
    {
        Set<wres.config.yaml.components.Threshold> thresholdsOne
                = Set.of( ThresholdBuilder.builder()
                                          .threshold( Threshold.newBuilder()
                                                               .setLeftThresholdValue( DoubleValue.of( 23.0 ) )
                                                               .setOperator( Threshold.ThresholdOperator.GREATER )
                                                               .build() )
                                          .type( ThresholdType.VALUE )
                                          .featureNameFrom( DatasetOrientation.LEFT )
                                          .build() );
        Metric one = MetricBuilder.Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .valueThresholds( thresholdsOne )
                                                                  .build() );
        Metric two = MetricBuilder.Metric( MetricConstants.MEAN_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .valueThresholds( thresholdsOne )
                                                                  .build() );

        Set<wres.config.yaml.components.Threshold> thresholdsTwo
                = Set.of( ThresholdBuilder.builder()
                                          .threshold( Threshold.newBuilder()
                                                               .setLeftThresholdValue( DoubleValue.of( 0.3 ) )
                                                               .setOperator( Threshold.ThresholdOperator.LESS )
                                                               .build() )
                                          .type( ThresholdType.PROBABILITY )
                                          .featureNameFrom( DatasetOrientation.RIGHT )
                                          .build() );

        Metric three = MetricBuilder.Metric( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                             MetricParametersBuilder.builder()
                                                                    .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                    .probabilityThresholds( thresholdsTwo )
                                                                    .build() );
        Metric four = MetricBuilder.Metric( MetricConstants.MEAN_SQUARE_ERROR,
                                            MetricParametersBuilder.builder()
                                                                   .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                   .probabilityThresholds( thresholdsTwo )
                                                                   .build() );

        Set<Metric> metrics = Set.of( one, two, three, four );

        Set<Set<Metric>> actual = DeclarationUtilities.getMetricGroupsForProcessing( metrics );

        Set<Set<Metric>> expected = Set.of( Set.of( three, four ), Set.of( one, two ) );

        assertEquals( expected, actual );
    }
}
