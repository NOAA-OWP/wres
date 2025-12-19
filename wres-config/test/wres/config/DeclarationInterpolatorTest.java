package wres.config;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.components.AnalysisTimes;
import wres.config.components.AnalysisTimesBuilder;
import wres.config.components.BaselineDataset;
import wres.config.components.BaselineDatasetBuilder;
import wres.config.components.CovariateDataset;
import wres.config.components.CovariateDatasetBuilder;
import wres.config.components.CovariatePurpose;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetBuilder;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EvaluationDeclarationBuilder;
import wres.config.components.EventDetection;
import wres.config.components.EventDetectionBuilder;
import wres.config.components.EventDetectionDataset;
import wres.config.components.FeatureAuthority;
import wres.config.components.FeatureGroups;
import wres.config.components.FeatureGroupsBuilder;
import wres.config.components.Features;
import wres.config.components.FeaturesBuilder;
import wres.config.components.Formats;
import wres.config.components.LeadTimeInterval;
import wres.config.components.LeadTimeIntervalBuilder;
import wres.config.components.Metric;
import wres.config.components.MetricBuilder;
import wres.config.components.MetricParameters;
import wres.config.components.MetricParametersBuilder;
import wres.config.components.Source;
import wres.config.components.SourceBuilder;
import wres.config.components.SourceInterface;
import wres.config.components.ThresholdBuilder;
import wres.config.components.ThresholdSource;
import wres.config.components.ThresholdSourceBuilder;
import wres.config.components.ThresholdType;
import wres.config.components.TimeInterval;
import wres.config.components.VariableBuilder;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeWindow;

/**
 * Tests the {@link DeclarationFactory}.
 *
 * @author James Brown
 */

class DeclarationInterpolatorTest
{
    private static final String FEATURE_NAME_ONE = "CHICKEN";
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_ONE ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_ONE ) )
                         .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_ONE ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_ONE ) )
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_ONE ) )
                         .build();
    private static final GeometryTuple LEFT_NAME_ONE_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_ONE ) )
                         .build();
    private static final GeometryTuple RIGHT_NAME_ONE_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_ONE ) )
                         .build();
    private static final GeometryTuple BASELINE_NAME_ONE_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_ONE ) )
                         .build();

    private static final String FEATURE_NAME_TWO = "CHEESE";
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_TWO ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_TWO ) )
                         .build();
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_TWO ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_TWO ) )
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_TWO ) )
                         .build();
    private static final GeometryTuple LEFT_NAME_TWO_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_TWO ) )
                         .build();
    private static final GeometryTuple RIGHT_NAME_TWO_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_TWO ) )
                         .build();
    private static final GeometryTuple BASELINE_NAME_TWO_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_TWO ) )
                         .build();
    private static final String FEATURE_NAME_THREE = "SAUSAGE";
    private static final GeometryTuple FULLY_DECLARED_FEATURE_ALL_NAME_THREE_WITH_BASELINE =
            GeometryTuple.newBuilder()
                         .setLeft( Geometry.newBuilder()
                                           .setName( FEATURE_NAME_THREE ) )
                         .setRight( Geometry.newBuilder()
                                            .setName( FEATURE_NAME_THREE ) )
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_THREE ) )
                         .build();
    private static final GeometryTuple BASELINE_NAME_THREE_DECLARED_FEATURE =
            GeometryTuple.newBuilder()
                         .setBaseline( Geometry.newBuilder()
                                               .setName( FEATURE_NAME_THREE ) )
                         .build();
    /** An observed dataset for re-use. */
    private Dataset observedDataset;
    /** A predicted dataset for re-use. */
    private Dataset predictedDataset;
    /** Re-used feature string. */
    private static final String DRRC2 = "DRRC2";
    /** Re-used feature string. */
    private static final String DOLC2 = "DOLC2";
    /** All data threshold. */
    private static final wres.config.components.Threshold
            ALL_DATA_THRESHOLD =
            wres.config.components.ThresholdBuilder.builder()
                                                   .threshold( Threshold.newBuilder()
                                                                        .setObservedThresholdValue( Double.NEGATIVE_INFINITY )
                                                                        .setOperator( Threshold.ThresholdOperator.GREATER )
                                                                        .setDataType( Threshold.ThresholdDataType.OBSERVED_AND_PREDICTED )
                                                                        .build() )
                                                   .type( ThresholdType.VALUE )
                                                   .generated( true )
                                                   .build();

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

        List<Source> observedSourcesInner = List.of( observedSource );
        this.observedDataset = DatasetBuilder.builder()
                                             .sources( observedSourcesInner )
                                             .build();

        List<Source> predictedSourcesInner = List.of( predictedSource );
        this.predictedDataset = DatasetBuilder.builder()
                                              .sources( predictedSourcesInner )
                                              .build();
    }

    @Test
    void testInterpolateCsv2FormatWhenNoneDeclared()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( this.predictedDataset )
                                                                        .build();

        EvaluationDeclaration interpolate = DeclarationInterpolator.interpolate( declaration, false );

        Formats actual = interpolate.formats();
        Outputs outputs = Outputs.newBuilder()
                                 .setCsv2( Formats.CSV2_FORMAT )
                                 .build();
        Formats expected = new Formats( outputs );
        assertEquals( expected, actual );
    }

    @Test
    void testInterpolatePngWithoutSvgWhenMetricParametersIncludePngOnly()
    {
        Metric metric = new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, MetricParametersBuilder.builder()
                                                                                                .png( true )
                                                                                                .build() );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( this.predictedDataset )
                                                                        .metrics( Set.of( metric ) )
                                                                        .build();

        EvaluationDeclaration interpolate = DeclarationInterpolator.interpolate( declaration, false );

        Formats actual = interpolate.formats();
        Outputs outputs = Outputs.newBuilder()
                                 .setPng( Formats.PNG_FORMAT )
                                 .build();
        Formats expected = new Formats( outputs );
        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateAllValidMetricsForSingleValuedTimeSeries()
    {
        Dataset predictedDatasetInner = DatasetBuilder.builder( this.predictedDataset )
                                                      .type( DataType.SIMULATIONS )
                                                      .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( predictedDatasetInner )
                                                                        .build();

        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( declaration, false );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> expected = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics()
                                                                                     .stream()
                                                                                     .filter( m -> !m.isExplicitBaselineRequired() )
                                                                                     .collect( Collectors.toSet() );
        assertEquals( expected, actualMetrics );
    }

    @Test
    void testInterpolateAllValidMetricsForEnsembleTimeSeries()
    {
        Dataset predictedDatasetInner = DatasetBuilder.builder( this.predictedDataset )
                                                      .type( DataType.ENSEMBLE_FORECASTS )
                                                      .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( predictedDatasetInner )
                                                                        .build();

        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( declaration, false );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> expectedMetrics = new HashSet<>( MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics() );

        expectedMetrics = expectedMetrics
                .stream()
                .filter( m -> !m.isExplicitBaselineRequired() )
                .collect( Collectors.toSet() );

        assertEquals( expectedMetrics, actualMetrics );
    }

    @Test
    void testInterpolateFeatureAuthorities()
    {
        URI observedUri = URI.create( "https://usgs.gov/nwis" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        URI predictedUri = URI.create( "another_file.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .sourceInterface( SourceInterface.NWM_LONG_RANGE_CHANNEL_RT_CONUS )
                                              .build();

        Source baselineSource = SourceBuilder.builder()
                                             .uri( predictedUri )
                                             .sourceInterface( SourceInterface.WRDS_AHPS )
                                             .build();

        List<Source> observedSourcesInner = List.of( observedSource );
        Dataset left = DatasetBuilder.builder()
                                     .sources( observedSourcesInner )
                                     .build();

        List<Source> predictedSourcesInner = List.of( predictedSource );
        Dataset right = DatasetBuilder.builder()
                                      .sources( predictedSourcesInner )
                                      .build();

        List<Source> baselineSources = List.of( baselineSource );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( left )
                                                                       .right( right )
                                                                       .baseline( baseline )
                                                                       .build();

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation, false );

        assertAll( () -> assertEquals( FeatureAuthority.USGS_SITE_CODE, actual.left()
                                                                              .featureAuthority() ),
                   () -> assertEquals( FeatureAuthority.NWM_FEATURE_ID, actual.right()
                                                                              .featureAuthority() ),
                   () -> assertEquals( FeatureAuthority.NWS_LID, actual.baseline()
                                                                       .dataset()
                                                                       .featureAuthority() )
        );
    }

    @Test
    void testInterpolateLeftFeatureFromDeclaredRightWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE );
        EvaluationDeclaration evaluation
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            false );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 2, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE ) );
    }

    @Test
    void testInterpolateRightFeatureFromDeclaredLeftWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                              LEFT_NAME_TWO_DECLARED_FEATURE );
        EvaluationDeclaration expected
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            false );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( expected, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 2, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE ) );
    }

    @Test
    void testInterpolateRightAndBaselineFeatureFromDeclaredLeftWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                              LEFT_NAME_TWO_DECLARED_FEATURE );
        EvaluationDeclaration expected
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            true );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( expected, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 2, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
    }

    @Test
    void testInterpolateLeftAndBaselineFeatureFromDeclaredRightWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE );
        EvaluationDeclaration expected
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            true );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( expected, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 2, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
    }

    @Test
    void testInterpolateLeftAndRightFeatureFromDeclaredBaselineWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( BASELINE_NAME_ONE_DECLARED_FEATURE,
                                              BASELINE_NAME_TWO_DECLARED_FEATURE );
        EvaluationDeclaration expected
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            true );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( expected, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 2, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
    }

    @Test
    void testInterpolateLeftFeatureFromDeclaredRightAndRightFeatureFromDeclaredLeftWhenSameAuthority()
    {
        Set<GeometryTuple> features = Set.of( LEFT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE,
                                              BASELINE_NAME_THREE_DECLARED_FEATURE );
        EvaluationDeclaration expected
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            null,
                                                                            true );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( expected, false );

        Set<GeometryTuple> resultFeatures = actual.features()
                                                  .geometries();

        // Expect features fully declared.
        assertEquals( 3, resultFeatures.size() );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_TWO_WITH_BASELINE ) );
        assertTrue( resultFeatures.contains( FULLY_DECLARED_FEATURE_ALL_NAME_THREE_WITH_BASELINE ) );
    }

    @Test
    void testInterpolateLeftFeatureFromDeclaredRightWhenSameAuthorityAndGrouped()
    {
        Set<GeometryTuple> features = Set.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE );

        String groupName = "A GROUP!";
        GeometryGroup featureGroup = GeometryGroup.newBuilder()
                                                  .addAllGeometryTuples( features )
                                                  .setRegionName( groupName )
                                                  .build();

        EvaluationDeclaration evaluation
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( null,
                                                                            Set.of( featureGroup ),
                                                                            false );

        EvaluationDeclaration actualEvaluation = DeclarationInterpolator.interpolate( evaluation, false );

        Set<GeometryTuple> expectedFeatures = Set.of( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE,
                                                      FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE );

        assertEquals( 1, actualEvaluation.featureGroups()
                                         .geometryGroups()
                                         .size() );

        // Only interested in the set of grouped geometries here, not the order in which they appear, since the group
        // is a list and the list abstraction is protobuf-generated code
        Set<GeometryTuple> actualGroupFeatures = actualEvaluation.featureGroups()
                                                                 .geometryGroups()
                                                                 .stream()
                                                                 .flatMap( next -> next.getGeometryTuplesList()
                                                                                       .stream() )
                                                                 .collect( Collectors.toSet() );

        assertEquals( expectedFeatures, actualGroupFeatures );
        assertTrue( actualEvaluation.featureGroups()
                                    .geometryGroups()
                                    .stream()
                                    .anyMatch( next -> groupName.equals( next.getRegionName() ) ) );
    }

    @Test
    void testInterpolateLeftFeatureFromDeclaredRightWhenSameAuthorityAndBothSingletonAndGrouped()
    {
        Set<GeometryTuple> features = Set.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE );

        String groupName = "A GROUP!";
        GeometryGroup featureGroup = GeometryGroup.newBuilder()
                                                  .addAllGeometryTuples( features )
                                                  .setRegionName( groupName )
                                                  .build();

        EvaluationDeclaration evaluation
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( features,
                                                                            Set.of( featureGroup ),
                                                                            false );

        EvaluationDeclaration actualEvaluation = DeclarationInterpolator.interpolate( evaluation, false );

        Set<GeometryTuple> expectedFeatures = Set.of( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE,
                                                      FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE );

        Set<GeometryTuple> actualFeatures = actualEvaluation.features()
                                                            .geometries();

        assertEquals( expectedFeatures, actualFeatures );

        assertEquals( 1, actualEvaluation.featureGroups()
                                         .geometryGroups()
                                         .size() );

        // Only interested in the set of grouped geometries here, not the order in which they appear, since the group
        // is a list and the list abstraction is protobuf-generated code
        Set<GeometryTuple> actualGroupFeatures = actualEvaluation.featureGroups()
                                                                 .geometryGroups()
                                                                 .stream()
                                                                 .flatMap( next -> next.getGeometryTuplesList()
                                                                                       .stream() )
                                                                 .collect( Collectors.toSet() );

        assertEquals( expectedFeatures, actualGroupFeatures );

        assertTrue( actualEvaluation.featureGroups()
                                    .geometryGroups()
                                    .stream()
                                    .anyMatch( next -> groupName.equals( next.getRegionName() ) ) );
    }

    @Test
    void testInterpolateLeftFeatureFromDeclaredRightWhenSameAuthorityAndSingletonFeaturesDenseAndGroupedFeaturesSparse()
    {
        Set<GeometryTuple> features = Set.of( RIGHT_NAME_ONE_DECLARED_FEATURE,
                                              RIGHT_NAME_TWO_DECLARED_FEATURE );

        // Pass in sparsely declared features
        Set<GeometryTuple> singletons = Set.of( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE,
                                                FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE );

        String groupName = "A GROUP!";
        GeometryGroup featureGroup = GeometryGroup.newBuilder()
                                                  .addAllGeometryTuples( features )
                                                  .setRegionName( groupName )
                                                  .build();

        EvaluationDeclaration evaluation
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( singletons,
                                                                            Set.of( featureGroup ),
                                                                            false );

        EvaluationDeclaration actualEvaluation = DeclarationInterpolator.interpolate( evaluation, false );

        Set<GeometryTuple> expectedFeatures = Set.of( FULLY_DECLARED_FEATURE_ALL_NAME_ONE_NO_BASELINE,
                                                      FULLY_DECLARED_FEATURE_ALL_NAME_TWO_NO_BASELINE );

        Set<GeometryTuple> actualFeatures = actualEvaluation.features()
                                                            .geometries();

        assertEquals( expectedFeatures, actualFeatures );

        assertEquals( 1, actualEvaluation.featureGroups()
                                         .geometryGroups()
                                         .size() );

        // Only interested in the set of grouped geometries here, not the order in which they appear, since the group
        // is a list and the list abstraction is protobuf-generated code
        Set<GeometryTuple> actualGroupFeatures = actualEvaluation.featureGroups()
                                                                 .geometryGroups()
                                                                 .stream()
                                                                 .flatMap( next -> next.getGeometryTuplesList()
                                                                                       .stream() )
                                                                 .collect( Collectors.toSet() );

        assertEquals( expectedFeatures, actualGroupFeatures );
        assertTrue( actualEvaluation.featureGroups()
                                    .geometryGroups()
                                    .stream()
                                    .anyMatch( next -> groupName.equals( next.getRegionName() ) ) );
    }

    @Test
    void testInterpolationOfFeaturesFromFeaturefulThresholds()
    {
        Geometry featureFoo = Geometry.newBuilder()
                                      .setName( "foo" )
                                      .build();
        Geometry featureBar = Geometry.newBuilder()
                                      .setName( "bar" )
                                      .build();
        Geometry featureBaz = Geometry.newBuilder()
                                      .setName( "baz" )
                                      .build();

        Threshold one = Threshold.newBuilder()
                                 .setObservedThresholdValue( 1.0 )
                                 .build();
        wres.config.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                      .threshold( one )
                                                                      .feature( featureFoo )
                                                                      .featureNameFrom( DatasetOrientation.RIGHT )
                                                                      .type( ThresholdType.VALUE )
                                                                      .build();
        Threshold two = Threshold.newBuilder()
                                 .setObservedThresholdValue( 2.0 )
                                 .build();
        wres.config.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                      .threshold( two )
                                                                      .feature( featureBaz )
                                                                      .featureNameFrom( DatasetOrientation.LEFT )
                                                                      .type( ThresholdType.VALUE )
                                                                      .build();
        Threshold three = Threshold.newBuilder()
                                   .setObservedThresholdValue( 2.0 )
                                   .build();
        wres.config.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                        .threshold( three )
                                                                        .feature( featureBar )
                                                                        .featureNameFrom( DatasetOrientation.LEFT )
                                                                        .type( ThresholdType.VALUE )
                                                                        .build();

        Dataset left = DatasetBuilder.builder( this.observedDataset )
                                     .featureAuthority( FeatureAuthority.CUSTOM )
                                     .build();

        Dataset right = DatasetBuilder.builder( this.predictedDataset )
                                      .featureAuthority( FeatureAuthority.CUSTOM )
                                      .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .thresholds( Set.of( wrappedOne,
                                                                 wrappedTwo,
                                                                 wrappedThree ) )
                                            .build();

        EvaluationDeclaration actualEvaluation = DeclarationInterpolator.interpolate( declaration, false );

        Set<GeometryTuple> actual = actualEvaluation.features()
                                                    .geometries();

        GeometryTuple featureFooFoo = GeometryTuple.newBuilder()
                                                   .setLeft( featureFoo )
                                                   .setRight( featureFoo )
                                                   .build();
        GeometryTuple featureBarBar = GeometryTuple.newBuilder()
                                                   .setLeft( featureBar )
                                                   .setRight( featureBar )
                                                   .build();
        GeometryTuple featureBazBaz = GeometryTuple.newBuilder()
                                                   .setLeft( featureBaz )
                                                   .setRight( featureBaz )
                                                   .build();

        Set<GeometryTuple> expected = Set.of( featureFooFoo, featureBarBar, featureBazBaz );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateGlobalAndLocalThresholds()
    {
        wres.config.components.Threshold one =
                ThresholdBuilder.builder()
                                .threshold( Threshold.newBuilder()
                                                     .setObservedThresholdValue( 1.0 )
                                                     .build() )
                                .type( ThresholdType.VALUE )
                                .build();

        wres.config.components.Threshold two =
                ThresholdBuilder.builder()
                                .threshold( Threshold.newBuilder()
                                                     .setObservedThresholdValue( 2.0 )
                                                     .build() )
                                .type( ThresholdType.VALUE )
                                .build();

        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( MetricParametersBuilder.builder()
                                                                        .thresholds( Set.of( two ) )
                                                                        .build() )
                                    .build();
        Metric second = MetricBuilder.builder()
                                     .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                     .build();

        Set<Metric> metrics = Set.of( first, second );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .thresholds( Set.of( one ) )
                                            .metrics( metrics )
                                            .build();
        EvaluationDeclaration actualDeclaration = DeclarationInterpolator.interpolate( declaration, false );
        Set<Metric> actual = actualDeclaration.metrics();

        Metric firstExpected =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( ALL_DATA_THRESHOLD, two ) )
                                                                 .build() )
                             .build();
        Metric secondExpected =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( ALL_DATA_THRESHOLD, one ) )
                                                                 .build() )
                             .build();

        Set<Metric> expected = Set.of( firstExpected, secondExpected );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateTimeZoneOffsets()
    {
        Dataset left = DatasetBuilder.builder( this.observedDataset )
                                     .timeZoneOffset( ZoneOffset.ofHours( 3 ) )
                                     .build();

        // Add a source with an explicit time zone offset, which should not be overwritten
        List<Source> sources = new ArrayList<>( this.predictedDataset.sources() );
        sources.add( SourceBuilder.builder()
                                  .timeZoneOffset( ZoneOffset.ofHours( 2 ) )
                                  .build() );
        Dataset innerPredictedDataset = DatasetBuilder.builder( this.predictedDataset )
                                                      .sources( sources )
                                                      .build();
        Dataset right = DatasetBuilder.builder( innerPredictedDataset )
                                      .timeZoneOffset( ZoneOffset.ofHours( 4 ) )
                                      .build();

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( left )
                                                                        .right( right )
                                                                        .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( declaration, false );

        Set<ZoneOffset> actualLeft = interpolated.left()
                                                 .sources()
                                                 .stream()
                                                 .map( Source::timeZoneOffset )
                                                 .collect( Collectors.toSet() );

        Set<ZoneOffset> actualRight = interpolated.right()
                                                  .sources()
                                                  .stream()
                                                  .map( Source::timeZoneOffset )
                                                  .collect( Collectors.toSet() );

        assertAll( () -> assertEquals( Set.of( ZoneOffset.ofHours( 3 ) ), actualLeft ),
                   () -> assertEquals( Set.of( ZoneOffset.ofHours( 2 ),
                                               ZoneOffset.ofHours( 4 ) ), actualRight ) );
    }

    @Test
    void testGetSparseFeaturesToInterpolate()
    {
        // Singleton features
        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( Geometry.newBuilder()
                                                           .setName( "foo" ) )
                                         .build();
        GeometryTuple two = GeometryTuple.newBuilder()
                                         .setRight( Geometry.newBuilder()
                                                            .setName( "bar" ) )
                                         .build();
        GeometryTuple three = GeometryTuple.newBuilder()
                                           .setBaseline( Geometry.newBuilder()
                                                                 .setName( "baz" ) )
                                           .build();
        // Add one dense
        GeometryTuple four = GeometryTuple.newBuilder()
                                          .setLeft( Geometry.newBuilder()
                                                            .setName( "a" ) )
                                          .setRight( Geometry.newBuilder()
                                                             .setName( "dense" ) )
                                          .setBaseline( Geometry.newBuilder()
                                                                .setName( "feature" ) )
                                          .build();
        Set<GeometryTuple> geometries = Set.of( one, two, three, four );

        // Grouped features
        GeometryTuple five = GeometryTuple.newBuilder()
                                          .setLeft( Geometry.newBuilder()
                                                            .setName( "qux" ) )
                                          .build();
        // Add one dense
        GeometryTuple six = GeometryTuple.newBuilder()
                                         .setLeft( Geometry.newBuilder()
                                                           .setName( "another" ) )
                                         .setRight( Geometry.newBuilder()
                                                            .setName( "dense" ) )
                                         .setBaseline( Geometry.newBuilder()
                                                               .setName( "feature" ) )
                                         .build();
        Set<GeometryTuple> groupGeometries = Set.of( five, six );
        GeometryGroup featureGroup = GeometryGroup.newBuilder()
                                                  .addAllGeometryTuples( groupGeometries )
                                                  .setRegionName( "A group!" ).build();

        // Create the declaration
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Collections.singleton( featureGroup ) )
                                                          .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .features( features )
                                                                        .featureGroups( featureGroups )
                                                                        .build();

        Set<GeometryTuple> actual = DeclarationInterpolator.getSparseFeaturesToInterpolate( declaration );

        Set<GeometryTuple> expected = Set.of( one, two, three, five );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateEnsembleAverageType()
    {
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( MetricParametersBuilder.builder()
                                                                        .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                        .build() )
                                    .build();
        Metric second = MetricBuilder.builder()
                                     .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                     .build();

        Set<Metric> metrics = Set.of( first, second );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                            .metrics( metrics )
                                            .build();
        EvaluationDeclaration actualDeclaration = DeclarationInterpolator.interpolate( declaration, false );
        Set<Metric> actual = actualDeclaration.metrics();

        Metric firstExpected =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .ensembleAverageType( Pool.EnsembleAverageType.MEAN )
                                                                 .thresholds( Set.of( ALL_DATA_THRESHOLD ) )
                                                                 .build() )
                             .build();
        Metric secondExpected =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                 .thresholds( Set.of( ALL_DATA_THRESHOLD ) )
                                                                 .build() )
                             .build();

        Set<Metric> expected = Set.of( firstExpected, secondExpected );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateAddsFeatureFulThresholdsToMetrics()
    {
        Set<GeometryTuple> singletons = Set.of( LEFT_NAME_ONE_DECLARED_FEATURE );
        String groupName = "A group";
        GeometryGroup featureGroup = GeometryGroup.newBuilder()
                                                  .addAllGeometryTuples( Set.of( LEFT_NAME_TWO_DECLARED_FEATURE ) )
                                                  .setRegionName( groupName )
                                                  .build();

        EvaluationDeclaration evaluation
                = DeclarationInterpolatorTest.getBoilerplateEvaluationWith( singletons,
                                                                            Set.of( featureGroup ),
                                                                            false );

        EvaluationDeclarationBuilder builder =
                EvaluationDeclarationBuilder.builder( evaluation )
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .thresholds( Set.of( ALL_DATA_THRESHOLD ) );

        // Create some metric-specific thresholds, one with a feature, one without
        wres.config.components.Threshold one =
                ThresholdBuilder.builder()
                                .feature( LEFT_NAME_ONE_DECLARED_FEATURE.getLeft() )
                                .threshold( Threshold.newBuilder()
                                                     .setObservedThresholdValue( 1.0 ).build() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.components.Threshold two =
                ThresholdBuilder.builder()
                                .threshold( Threshold.newBuilder()
                                                     .setObservedThresholdValue( 2.0 ).build() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        // Add some metrics
        Metric first =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( one, two ) )
                                                                 .build() )
                             .build();
        Metric second =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ERROR )
                             .build();
        Set<Metric> metrics = Set.of( first, second );
        builder.metrics( metrics );

        EvaluationDeclaration declarationToInterpolate = builder.build();
        EvaluationDeclaration actualDeclaration =
                DeclarationInterpolator.interpolate( declarationToInterpolate, false );

        wres.config.components.Threshold expectedOne =
                ThresholdBuilder.builder( ALL_DATA_THRESHOLD )
                                .feature( LEFT_NAME_ONE_DECLARED_FEATURE.getLeft() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.components.Threshold expectedTwo =
                ThresholdBuilder.builder( ALL_DATA_THRESHOLD )
                                .feature( LEFT_NAME_TWO_DECLARED_FEATURE.getLeft() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.components.Threshold expectedThree =
                ThresholdBuilder.builder( two )
                                .feature( LEFT_NAME_ONE_DECLARED_FEATURE.getLeft() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.components.Threshold expectedFour =
                ThresholdBuilder.builder( two )
                                .feature( LEFT_NAME_TWO_DECLARED_FEATURE.getLeft() )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();

        Metric expectedFirst =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( expectedOne,
                                                                                      expectedTwo,
                                                                                      one,
                                                                                      expectedThree,
                                                                                      expectedFour ) )
                                                                 .build() )
                             .build();
        Metric expectedSecond =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ERROR )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( expectedOne,
                                                                                      expectedTwo ) )
                                                                 .build() )
                             .build();
        Set<Metric> expected = Set.of( expectedFirst, expectedSecond );
        Set<Metric> actual = actualDeclaration.metrics();

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateMeasurementUnitsForValueThresholds()
    {
        // Create some metric-specific thresholds, one with a feature, one without
        Threshold oneInner = Threshold.newBuilder()
                                      .setObservedThresholdValue( 1.0 )
                                      .build();
        wres.config.components.Threshold one =
                ThresholdBuilder.builder()
                                .threshold( oneInner )
                                .type( ThresholdType.VALUE )
                                .build();

        // Add some metrics
        Metric first =
                MetricBuilder.builder()
                             .name( MetricConstants.PROBABILITY_OF_DETECTION )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( one ) )
                                                                 .build() )
                             .build();

        Set<Metric> metrics = Set.of( first );

        ThresholdSource source = ThresholdSourceBuilder.builder()
                                                       .uri( URI.create( "http://foo.html" ) )
                                                       .type( ThresholdType.VALUE )
                                                       .build();

        EvaluationDeclaration declarationToInterpolate =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .metrics( metrics )
                                            .thresholds( Set.of( one ) )
                                            .thresholdSources( Set.of( source ) )
                                            .thresholdSets( Set.of( one ) )
                                            .unit( "BANANAS" )
                                            .build();

        EvaluationDeclaration actual =
                DeclarationInterpolator.interpolate( declarationToInterpolate, false );

        Threshold oneInnerExpected = oneInner.toBuilder()
                                             .setThresholdValueUnits( "BANANAS" )
                                             .build();
        wres.config.components.Threshold oneExpected =
                ThresholdBuilder.builder()
                                .threshold( oneInnerExpected )
                                .type( ThresholdType.VALUE )
                                .build();

        Metric firstExpected =
                MetricBuilder.builder()
                             .name( MetricConstants.PROBABILITY_OF_DETECTION )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .thresholds( Set.of( oneExpected ) )
                                                                 .build() )
                             .build();

        Set<Metric> metricsExpected = Set.of( firstExpected );

        ThresholdSource sourceExpected = ThresholdSourceBuilder.builder()
                                                               .uri( URI.create( "http://foo.html" ) )
                                                               .type( ThresholdType.VALUE )
                                                               .unit( "BANANAS" )
                                                               .build();

        assertAll( () -> assertEquals( Set.of( oneExpected ), actual.thresholds() ),
                   () -> assertEquals( Set.of( oneExpected ), actual.thresholdSets() ),
                   () -> assertEquals( Set.of( sourceExpected ), actual.thresholdSources() ),
                   () -> assertEquals( metricsExpected, actual.metrics() ) );
    }

    @Test
    void testInterpolateDataTypesWhenUndeclaredAndIngestTypesSupplied()
    {
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( this.predictedDataset )
                                                         .build();

        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( this.observedDataset )
                                                            .build();

        List<CovariateDataset> covariates = List.of( covariate );
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .baseline( baseline )
                                            .covariates( covariates )
                                            .build();

        DataTypes types = new DataTypes( DataType.OBSERVATIONS,
                                         DataType.ENSEMBLE_FORECASTS,
                                         DataType.ENSEMBLE_FORECASTS,
                                         DataType.OBSERVATIONS );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation,
                                                                            types,
                                                                            variableNames,
                                                                            "foo",
                                                                            TimeScale.getDefaultInstance(),
                                                                            true );
        DataType actualLeft = actual.left()
                                    .type();
        DataType actualRight = actual.right()
                                     .type();
        DataType actualBaseline = actual.baseline()
                                        .dataset()
                                        .type();
        DataType actualCovariate = actual.covariates()
                                         .get( 0 )
                                         .dataset()
                                         .type();

        assertAll( () -> assertEquals( DataType.OBSERVATIONS, actualLeft ),
                   () -> assertEquals( DataType.ENSEMBLE_FORECASTS, actualRight ),
                   () -> assertEquals( DataType.ENSEMBLE_FORECASTS, actualBaseline ),
                   () -> assertEquals( DataType.OBSERVATIONS, actualCovariate ) );
    }

    @Test
    void testInterpolateDataTypesWhenUndeclaredAndTypeInferredFromDeclarationDoesNotMatchIngestTypesButMismatchAllowed()
    {
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            // Inferred as single-valued forecasts because no ensemble declaration
                                            .leadTimes( LeadTimeIntervalBuilder.builder()
                                                                               .minimum( Duration.ofHours( 1 ) )
                                                                               .maximum( Duration.ofHours( 2 ) )
                                                                               .build() )
                                            .build();

        DataTypes dataTypes = new DataTypes( DataType.OBSERVATIONS,
                                             DataType.ENSEMBLE_FORECASTS,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation,
                                                                            dataTypes,
                                                                            variableNames,
                                                                            "foo",
                                                                            TimeScale.getDefaultInstance(),
                                                                            true );
        DataType actualLeft = actual.left()
                                    .type();
        DataType actualRight = actual.right()
                                     .type();

        assertAll( () -> assertEquals( DataType.OBSERVATIONS, actualLeft ),
                   () -> assertEquals( DataType.ENSEMBLE_FORECASTS, actualRight ) );
    }

    @Test
    void testInterpolateDataTypesWhenUndeclaredAndTypeInferredFromDeclarationDoesNotMatchIngestTypesButMismatchDisallowed()
    {
        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .minimum( Duration.ofHours( 1 ) )
                                                          .maximum( Duration.ofHours( 2 ) )
                                                          .build();
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            // Inferred as single-valued forecasts because no ensemble declaration
                                            .analysisTimes( analysisTimes )
                                            .build();

        DataTypes dataTypes = new DataTypes( DataType.OBSERVATIONS,
                                             DataType.ENSEMBLE_FORECASTS,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );

        TimeScale timeScale = TimeScale.getDefaultInstance();
        DeclarationException expected =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( evaluation,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         true ) );

        assertTrue( expected.getMessage()
                            .contains( "but the data type inferred from the declaration was 'analyses', "
                                       + "which is inconsistent" ) );
    }

    @Test
    void testInterpolateDataTypesWhenTypesDeclaredAndIngestTypesDifferResultsInUseOfDeclaredTypes()
    {
        Dataset left = DatasetBuilder.builder( this.observedDataset )
                                     .type( DataType.ANALYSES )
                                     .build();

        Dataset right = DatasetBuilder.builder( this.predictedDataset )
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( right )
                                            .build();

        DataTypes dataTypes = new DataTypes( DataType.OBSERVATIONS,
                                             DataType.ENSEMBLE_FORECASTS,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation,
                                                                            dataTypes,
                                                                            variableNames,
                                                                            "foo",
                                                                            TimeScale.getDefaultInstance(),
                                                                            true );
        DataType actualLeft = actual.left()
                                    .type();
        DataType actualRight = actual.right()
                                     .type();

        assertAll( () -> assertEquals( DataType.ANALYSES, actualLeft ),
                   () -> assertEquals( DataType.SINGLE_VALUED_FORECASTS, actualRight ) );
    }

    @Test
    void testInterpolateDimensionForSummaryStatistics()
    {
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                .build();
        summaryStatistics.add( mean );

        MetricParameters parameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( parameters )
                                    .build();

        Set<Metric> metrics = Set.of( first );

        // Add summary statistics in two separate contexts
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .metrics( metrics )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation, false );

        Set<SummaryStatistic> actualOne = actual.summaryStatistics();
        Set<SummaryStatistic> actualTwo = actual.metrics()
                                                .stream()
                                                .findFirst()
                                                .orElseThrow()
                                                .parameters()
                                                .summaryStatistics();

        SummaryStatistic expectedFirst = SummaryStatistic.newBuilder()
                                                         .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                         .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                         .build();

        SummaryStatistic expectedSecond = SummaryStatistic.newBuilder()
                                                          .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                          .addDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS )
                                                          .build();

        assertAll( () -> assertEquals( Set.of( expectedFirst ), actualOne ),
                   () -> assertEquals( Set.of( expectedSecond ), actualTwo ) );

    }

    @Test
    void testInterpolateDimensionForSummaryStatisticsWhenNoneDeclared()
    {
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic mean = SummaryStatistic.newBuilder()
                                                .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                .build();
        summaryStatistics.add( mean );

        MetricParameters parameters =
                MetricParametersBuilder.builder()
                                       .summaryStatistics( summaryStatistics )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                    .parameters( parameters )
                                    .build();

        Set<Metric> metrics = Set.of( first );

        // Add summary statistics in two separate contexts
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .metrics( metrics )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation, false );

        Set<SummaryStatistic> actualOne = actual.summaryStatistics();
        Set<SummaryStatistic> actualTwo = actual.metrics()
                                                .stream()
                                                .findFirst()
                                                .orElseThrow()
                                                .parameters()
                                                .summaryStatistics();

        SummaryStatistic expectedFirst = SummaryStatistic.newBuilder()
                                                         .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                         .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                         .build();

        SummaryStatistic expectedSecond = SummaryStatistic.newBuilder()
                                                          .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                          .addDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS )
                                                          .build();

        assertAll( () -> assertEquals( Set.of( expectedFirst ), actualOne ),
                   () -> assertEquals( Set.of( expectedSecond ), actualTwo ) );

    }

    @Test
    void testInterpolateCombinedDimensionsForSummaryStatistics()
    {
        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        SummaryStatistic meanFeatures = SummaryStatistic.newBuilder()
                                                        .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                        .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                        .build();

        SummaryStatistic meanValidTime = SummaryStatistic.newBuilder()
                                                         .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                         .addDimension( SummaryStatistic.StatisticDimension.VALID_DATE_POOLS )
                                                         .build();
        SummaryStatistic stdevFeatures = SummaryStatistic.newBuilder()
                                                         .setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                                         .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                         .build();

        SummaryStatistic stdevValidTime = SummaryStatistic.newBuilder()
                                                          .setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                                          .addDimension( SummaryStatistic.StatisticDimension.VALID_DATE_POOLS )
                                                          .build();

        summaryStatistics.add( meanValidTime );
        summaryStatistics.add( meanFeatures );
        summaryStatistics.add( stdevFeatures );
        summaryStatistics.add( stdevValidTime );

        // Add summary statistics in two separate contexts
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( evaluation, false );

        Set<SummaryStatistic> actual = interpolated.summaryStatistics();

        SummaryStatistic expectedFirst = SummaryStatistic.newBuilder()
                                                         .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                         .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                         .addDimension( SummaryStatistic.StatisticDimension.VALID_DATE_POOLS )
                                                         .build();

        SummaryStatistic expectedSecond = SummaryStatistic.newBuilder()
                                                          .setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                                          .addDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                          .addDimension( SummaryStatistic.StatisticDimension.VALID_DATE_POOLS )
                                                          .build();

        Set<SummaryStatistic> expected = Set.of( expectedFirst, expectedSecond );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateVariableNames()
    {
        BaselineDataset baseline = new BaselineDataset( this.observedDataset, null, null );
        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( this.observedDataset )
                                                            .build();
        List<CovariateDataset> covariates = List.of( covariate );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .baseline( baseline )
                                            .covariates( covariates )
                                            .build();
        VariableNames variableNames = new VariableNames( "foo",
                                                         "bar",
                                                         "baz",
                                                         Set.of( "qux" ) );
        DataTypes dataTypes = new DataTypes( null, null, null, null );

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( declaration,
                                                                            dataTypes,
                                                                            variableNames,
                                                                            "foo",
                                                                            TimeScale.getDefaultInstance(),
                                                                            false );

        assertAll( () -> assertEquals( "foo", actual.left()
                                                    .variable()
                                                    .name() ),
                   () -> assertEquals( "bar", actual.right()
                                                    .variable()
                                                    .name() ),
                   () -> assertEquals( "baz", actual.baseline()
                                                    .dataset()
                                                    .variable()
                                                    .name() ),
                   () -> assertEquals( "qux", actual.covariates()
                                                    .get( 0 )
                                                    .dataset()
                                                    .variable()
                                                    .name() ) );
    }

    @Test
    void testInterpolateVariableNamesForTooManyDeclaredCovariatesProducesError()
    {
        BaselineDataset baseline = new BaselineDataset( this.observedDataset, null, null );
        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( this.observedDataset )
                                                            .build();
        List<CovariateDataset> covariates = List.of( covariate, covariate );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .baseline( baseline )
                                            .covariates( covariates )
                                            .build();
        VariableNames variableNames = new VariableNames( "foo",
                                                         "bar",
                                                         "baz",
                                                         Set.of( "qux" ) );
        DataTypes dataTypes = new DataTypes( null, null, null, null );

        TimeScale timeScale = TimeScale.getDefaultInstance();
        DeclarationException actualException =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( declaration,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         false ) );
        assertTrue( actualException.getMessage()
                                   .contains( "one or more of the 'covariates' had no declared 'variable' and 'name'" ) );

    }

    @Test
    void testInterpolateVariableNamesForTooManyIngestedCovariatesProducesError()
    {
        BaselineDataset baseline = new BaselineDataset( this.observedDataset, null, null );
        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( this.observedDataset )
                                                            .build();
        List<CovariateDataset> covariates = List.of( covariate );
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .baseline( baseline )
                                            .covariates( covariates )
                                            .build();
        VariableNames variableNames = new VariableNames( "foo",
                                                         "bar",
                                                         "baz",
                                                         Set.of( "qux", "quux" ) );
        DataTypes dataTypes = new DataTypes( null, null, null, null );

        TimeScale timeScale = TimeScale.getDefaultInstance();
        DeclarationException actualException =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( declaration,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         false ) );
        assertTrue( actualException.getMessage()
                                   .contains( "Discovered a 'covariate' dataset that contains more than one variable" ) );

    }

    @Test
    void testInterpolateInconsistentVariableNamesProducesError()
    {
        Dataset left = DatasetBuilder.builder( this.observedDataset )
                                     .variable( VariableBuilder.builder()
                                                               .name( "foo" )
                                                               .build() )
                                     .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( left )
                                            .right( this.predictedDataset )
                                            .build();
        VariableNames variableNames = new VariableNames( "bar",
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );

        TimeScale timeScale = TimeScale.getDefaultInstance();
        DeclarationException actualException =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( declaration,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         false ) );
        assertTrue( actualException.getMessage()
                                   .contains( "When attempting to set the variable name for the 'observed' dataset, "
                                              + "found a declared variable name" ) );

    }

    @Test
    void testInterpolateFeatureOrientationForCovariate()
    {
        // Implicit declaration of feature authority for the right-ish data, to be re-used for the covariate data
        Source source = SourceBuilder.builder()
                                     .sourceInterface( SourceInterface.WRDS_NWM )
                                     .build();

        Dataset rightData = DatasetBuilder.builder( this.observedDataset )
                                          .sources( List.of( source ) )
                                          .build();
        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( rightData )
                                                            .build();

        List<CovariateDataset> covariates = List.of( covariate );
        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( rightData )
                                            .covariates( covariates )
                                            .build();
        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( evaluation, false );

        DatasetOrientation actualCovariateOrieintation = actual.covariates()
                                                               .get( 0 )
                                                               .featureNameOrientation();

        assertEquals( DatasetOrientation.RIGHT, actualCovariateOrieintation );
    }

    @Test
    void testInterpolateMeasurementUnit()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .build();

        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );
        TimeScale timeScale = TimeScale.getDefaultInstance();

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( declaration,
                                                                            dataTypes,
                                                                            variableNames,
                                                                            "foo",
                                                                            timeScale,
                                                                            false );
        assertEquals( "foo", actual.unit() );
    }

    @Test
    void testInterpolateInconsistentMeasurementUnitProducesError()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .unit( "bar" )
                                                                        .build();

        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );
        TimeScale timeScale = TimeScale.getDefaultInstance();

        DeclarationException actualException =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( declaration,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         false ) );
        assertTrue( actualException.getMessage()
                                   .contains( "The declared measurement unit of 'bar' is inconsistent" ) );
    }

    @Test
    void testInterpolateTimeScale()
    {
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .build();

        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 64 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();

        EvaluationDeclaration actual = DeclarationInterpolator.interpolate( declaration,
                                                                            dataTypes,
                                                                            variableNames,
                                                                            "foo",
                                                                            timeScale,
                                                                            false );
        assertEquals( timeScale, actual.timeScale()
                                       .timeScale() );
    }

    @Test
    void testInterpolateInconsistentTimeScaleProducesError()
    {
        TimeScale existingTimeScale = TimeScale.newBuilder()
                                               .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 63 ) )
                                               .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                               .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .timeScale( new wres.config.components.TimeScale( existingTimeScale ) )
                                            .build();

        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 64 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();

        DeclarationException actualException =
                assertThrows( DeclarationException.class,
                              () -> DeclarationInterpolator.interpolate( declaration,
                                                                         dataTypes,
                                                                         variableNames,
                                                                         "foo",
                                                                         timeScale,
                                                                         false ) );
        assertTrue( actualException.getMessage()
                                   .contains( "The declared evaluation timescale" ) );
    }

    @Test
    void testInterpolateMissingTimeWindowComponents()
    {
        Instant validWindowOne = Instant.parse( "2551-03-17T00:00:00Z" );
        Instant validWindowTwo = Instant.parse( "2551-03-20T00:00:00Z" );

        Duration durationOne = Duration.ofHours( 7 );
        Duration durationTwo = Duration.ofHours( 12 );

        TimeWindow one = TimeWindow.newBuilder()
                                   .setEarliestValidTime( MessageUtilities.getTimestamp( validWindowOne ) )
                                   .setLatestValidTime( MessageUtilities.getTimestamp( validWindowTwo ) )
                                   .build();

        TimeWindow two = TimeWindow.newBuilder()
                                   .setEarliestLeadDuration( MessageUtilities.getDuration( durationOne ) )
                                   .setLatestLeadDuration( MessageUtilities.getDuration( durationTwo ) )
                                   .build();

        Set<TimeWindow> timeWindows = Set.of( one, two );

        Instant validOne = Instant.parse( "2033-12-01T09:15:23Z" );
        Instant validTwo = Instant.parse( "2083-12-01T09:15:23Z" );

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( this.predictedDataset )
                                            .leadTimes( new LeadTimeInterval( Duration.ofHours( 1 ),
                                                                              Duration.ofHours( 100 ) ) )
                                            .validDates( new TimeInterval( validOne, validTwo ) )
                                            .timePools( timeWindows )
                                            .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( declaration, false );

        Set<TimeWindow> actual = interpolated.timePools();

        TimeWindow expectedOne = TimeWindow.newBuilder()
                                           .setEarliestValidTime( MessageUtilities.getTimestamp( validWindowOne ) )
                                           .setLatestValidTime( MessageUtilities.getTimestamp( validWindowTwo ) )
                                           .setEarliestReferenceTime( MessageUtilities.getTimestamp( Instant.MIN ) )
                                           .setLatestReferenceTime( MessageUtilities.getTimestamp( Instant.MAX ) )
                                           .setEarliestLeadDuration( MessageUtilities.getDuration( Duration.ofHours( 1 ) ) )
                                           .setLatestLeadDuration( MessageUtilities.getDuration( Duration.ofHours( 100 ) ) )
                                           .build();

        TimeWindow expectedTwo = TimeWindow.newBuilder()
                                           .setEarliestValidTime( MessageUtilities.getTimestamp( validOne ) )
                                           .setLatestValidTime( MessageUtilities.getTimestamp( validTwo ) )
                                           .setEarliestReferenceTime( MessageUtilities.getTimestamp( Instant.MIN ) )
                                           .setLatestReferenceTime( MessageUtilities.getTimestamp( Instant.MAX ) )
                                           .setEarliestLeadDuration( MessageUtilities.getDuration( durationOne ) )
                                           .setLatestLeadDuration( MessageUtilities.getDuration( durationTwo ) )
                                           .build();

        Set<TimeWindow> expected = Set.of( expectedOne, expectedTwo );

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateMetricThresholdsWithAllDataOverride()
    {
        Metric metric = new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR, MetricParametersBuilder.builder()
                                                                                                .build() );

        Metric anotherMetric =
                new Metric( MetricConstants.MEAN_ERROR,
                            MetricParametersBuilder.builder()
                                                   .thresholds( Set.of( DeclarationUtilities.ALL_DATA_THRESHOLD ) )
                                                   .build() );

        Threshold one = Threshold.newBuilder()
                                 .setObservedThresholdValue( 0.1 )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .build();
        wres.config.components.Threshold oneWrapped =
                ThresholdBuilder.builder()
                                .threshold( one )
                                .type( ThresholdType.PROBABILITY )
                                .build();

        // Preserve insertion order
        Set<wres.config.components.Threshold> thresholds = Set.of( oneWrapped );

        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( this.predictedDataset )
                                                                        .probabilityThresholds( thresholds )
                                                                        .metrics( Set.of( metric, anotherMetric ) )
                                                                        .build();

        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        DataTypes dataTypes = new DataTypes( null, null, null, null );
        TimeScale timeScale = TimeScale.newBuilder()
                                       .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 64 ) )
                                       .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                       .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( declaration,
                                                                                  dataTypes,
                                                                                  variableNames,
                                                                                  "foo",
                                                                                  timeScale,
                                                                                  false );

        Metric expectedOne =
                new Metric( MetricConstants.MEAN_ERROR,
                            MetricParametersBuilder.builder()
                                                   .thresholds( Set.of( DeclarationUtilities.ALL_DATA_THRESHOLD ) )
                                                   .build() );

        Metric expectedTwo =
                new Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                            MetricParametersBuilder.builder()
                                                   .thresholds( Set.of( DeclarationUtilities.GENERATED_ALL_DATA_THRESHOLD ) )
                                                   .probabilityThresholds( thresholds )
                                                   .build() );

        Set<Metric> expected = Set.of( expectedOne, expectedTwo );
        Set<Metric> actual = interpolated.metrics();

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateCovariatePurposeForEventDetection()
    {
        EventDetection detection = EventDetectionBuilder.builder()
                                                        .datasets( Set.of( EventDetectionDataset.COVARIATES ) )
                                                        .build();
        CovariateDataset covariate = CovariateDatasetBuilder.builder()
                                                            .dataset( this.observedDataset )
                                                            .build();
        EvaluationDeclaration declaration = EvaluationDeclarationBuilder.builder()
                                                                        .left( this.observedDataset )
                                                                        .right( this.predictedDataset )
                                                                        .covariates( List.of( covariate ) )
                                                                        .eventDetection( detection )
                                                                        .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( declaration,
                                                                                  false );
        assertTrue( interpolated.covariates()
                                .stream()
                                .anyMatch( n -> n.purposes()
                                                 .contains( CovariatePurpose.DETECT ) ) );

    }

    @Test
    void testInterpolateForecastDataTypeForWrdsSourceWhenForecastDeclarationIncluded()
    {
        Instant validOne = Instant.parse( "2033-12-01T09:15:23Z" );
        Instant validTwo = Instant.parse( "2083-12-01T09:15:23Z" );

        Dataset predicted = DatasetBuilder.builder( this.predictedDataset )
                                          .sources( List.of( SourceBuilder.builder()
                                                                          .sourceInterface( SourceInterface.WRDS_AHPS )
                                                                          .uri( URI.create( "http://foo.bar" ) )
                                                                          .build() ) )
                                          .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .left( this.observedDataset )
                                            .right( predicted )
                                            .referenceDates( new TimeInterval( validOne, validTwo ) )
                                            .build();

        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( declaration,
                                                                                  false );

        assertSame( DataType.SINGLE_VALUED_FORECASTS, interpolated.right()
                                                                  .type() );
    }

    @Test
    void testInterpolateMissingValueForSourceWithoutDatasetMissingValue()
    {
        // GitHub #160
        URI observedUri = URI.create( "file://foo/some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        List<Source> observedSources = List.of( observedSource );
        Dataset observedDatasetInner = DatasetBuilder.builder()
                                                     .sources( observedSources )
                                                     .build();

        URI predictedUri = URI.create( "file://foo/another_file.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .missingValue( List.of( -998.0 ) )
                                              .build();

        URI anotherPredictedUri = URI.create( "file://foo/yet_another_file.csv" );
        Source anotherPredictedSource = SourceBuilder.builder()
                                                     .uri( anotherPredictedUri )
                                                     .build();

        List<Source> predictedSources = List.of( predictedSource, anotherPredictedSource );
        Dataset predictedDatasetInner = DatasetBuilder.builder()
                                                      .sources( predictedSources )
                                                      .missingValue( List.of( -999.0 ) )
                                                      .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( observedDatasetInner )
                                                                       .right( predictedDatasetInner )
                                                                       .build();

        EvaluationDeclaration actualDeclaration = DeclarationInterpolator.interpolate( evaluation,
                                                                                       false );

        Source expectedPredictedSource = SourceBuilder.builder()
                                                      .uri( anotherPredictedUri )
                                                      .missingValue( List.of( -999.0 ) )
                                                      .build();

        List<Source> expectedPredictedSources = List.of( predictedSource, expectedPredictedSource );
        Dataset expected = DatasetBuilder.builder()
                                         .sources( expectedPredictedSources )
                                         .missingValue( List.of( -999.0 ) )
                                         .build();

        Dataset actual = actualDeclaration.right();

        assertEquals( expected, actual );
    }

    @Test
    void testInterpolateMissingValueForSourcesWithDatasetMissingValue()
    {
        // GitHub #160
        URI observedUri = URI.create( "file://foo/some_file.csv" );
        Source observedSource = SourceBuilder.builder()
                                             .uri( observedUri )
                                             .build();

        List<Source> observedSources = List.of( observedSource );
        Dataset observedDatasetInner = DatasetBuilder.builder()
                                                     .sources( observedSources )
                                                     .build();

        URI predictedUri = URI.create( "file://foo/another_file.csv" );
        Source predictedSource = SourceBuilder.builder()
                                              .uri( predictedUri )
                                              .build();

        URI anotherPredictedUri = URI.create( "file://foo/yet_another_file.csv" );
        Source anotherPredictedSource = SourceBuilder.builder()
                                                     .uri( anotherPredictedUri )
                                                     .build();

        List<Source> predictedSources = List.of( predictedSource, anotherPredictedSource );
        Dataset predictedDatasetInner = DatasetBuilder.builder()
                                                      .sources( predictedSources )
                                                      .missingValue( List.of( -999.0 ) )
                                                      .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( observedDatasetInner )
                                                                       .right( predictedDatasetInner )
                                                                       .build();

        EvaluationDeclaration actualDeclaration = DeclarationInterpolator.interpolate( evaluation,
                                                                                       false );

        Source expectedPredictedOne = SourceBuilder.builder()
                                                   .uri( predictedUri )
                                                   .missingValue( List.of( -999.0 ) )
                                                   .build();

        Source expectedPredictedTwo = SourceBuilder.builder()
                                                   .uri( anotherPredictedUri )
                                                   .missingValue( List.of( -999.0 ) )
                                                   .build();

        List<Source> expectedPredictedSources = List.of( expectedPredictedOne, expectedPredictedTwo );
        Dataset expected = DatasetBuilder.builder()
                                         .sources( expectedPredictedSources )
                                         .missingValue( List.of( -999.0 ) )
                                         .build();

        Dataset actual = actualDeclaration.right();

        assertEquals( expected, actual );
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
                thresholds: [23.0, 25.0]
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actual, false );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
        Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();

        Set<MetricConstants> expectedMetrics = new HashSet<>( singleValued );
        expectedMetrics.addAll( dichotomous );

        expectedMetrics = expectedMetrics
                .stream()
                .filter( m -> !m.isExplicitBaselineRequired() )
                .collect( Collectors.toSet() );

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
                thresholds: [23.0, 25.0]
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actual, false );

        Set<MetricConstants> actualMetrics = actualInterpolated.metrics()
                                                               .stream()
                                                               .map( Metric::name )
                                                               .collect( Collectors.toSet() );

        Set<MetricConstants> expectedMetrics = new HashSet<>( MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY.getMetrics() );
        expectedMetrics.addAll( MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics() );
        expectedMetrics.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );

        expectedMetrics = expectedMetrics
                .stream()
                .filter( m -> !m.isExplicitBaselineRequired() )
                .collect( Collectors.toSet() );

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
        DataTypes dataTypes = new DataTypes( null,
                                             null,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        EvaluationDeclaration actual = DeclarationFactory.from( declaration );
        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( actual,
                                                                                  dataTypes,
                                                                                  variableNames,
                                                                                  "foo",
                                                                                  TimeScale.getDefaultInstance(),
                                                                                  false );
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
        DataTypes dataTypes = new DataTypes( null,
                                             null,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );
        EvaluationDeclaration actual = DeclarationFactory.from( declaration );
        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( actual,
                                                                                  dataTypes,
                                                                                  variableNames,
                                                                                  "foo",
                                                                                  TimeScale.getDefaultInstance(),
                                                                                  false );
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
                analysis_times:
                  minimum: 0
                  maximum: 0
                  unit: hours
                """;

        DataTypes dataTypes = new DataTypes( null,
                                             null,
                                             null,
                                             null );
        VariableNames variableNames = new VariableNames( null,
                                                         null,
                                                         null,
                                                         null );


        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration interpolated = DeclarationInterpolator.interpolate( actual,
                                                                                  dataTypes,
                                                                                  variableNames,
                                                                                  "foo",
                                                                                  TimeScale.getDefaultInstance(),
                                                                                  false );
        DataType actualType = interpolated.left()
                                          .type();
        assertEquals( DataType.ANALYSES, actualType );
    }

    @Test
    void testDeserializeAndInterpolateWithShortAndLongFeatures() throws IOException
    {
        String yaml = """
                observed:
                  sources:
                    - some_file.csv
                  feature_authority: custom
                predicted:
                  sources:
                    - another_file.csv
                  feature_authority: custom
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
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actual, false );

        GeometryTuple first = GeometryTuple.newBuilder()
                                           .setLeft( Geometry.newBuilder()
                                                             .setName( "09165000" )
                                                             .setWkt( "POINT (-67.945 46.8886111)" ) )
                                           .setRight( Geometry.newBuilder()
                                                              .setName( DRRC2 ) )
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
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometries )
                                           .build();

        assertEquals( features, actualInterpolated.features() );
    }

    @Test
    void testDeserializeAndInterpolateAddsUnitTothresholds() throws IOException
    {
        String yaml = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                unit: foo
                thresholds: [26,9,73]
                threshold_sets:
                  - thresholds: [23,2,45]
                metrics:
                  - name: mean square error skill score
                    thresholds: [12, 24, 27]
                """;

        EvaluationDeclaration actual = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actual, false );

        assertAll( () -> assertTrue( actualInterpolated.thresholds()
                                                       .stream()
                                                       .filter( next -> next.threshold()
                                                                        != DeclarationUtilities.ALL_DATA_THRESHOLD.threshold() )
                                                       .allMatch( next -> "foo".equals( next.threshold()
                                                                                            .getThresholdValueUnits() ) ) ),
                   () -> assertTrue( actualInterpolated.thresholdSets()
                                                       .stream()
                                                       .filter( next -> next.threshold()
                                                                        != DeclarationUtilities.ALL_DATA_THRESHOLD.threshold() )
                                                       .allMatch( next -> "foo".equals( next.threshold()
                                                                                            .getThresholdValueUnits() ) ) ),
                   () -> assertTrue( actualInterpolated.metrics()
                                                       .stream()
                                                       .map( Metric::parameters )
                                                       .flatMap( next -> next.thresholds().stream() )
                                                       .filter( next -> next.threshold()
                                                                        != DeclarationUtilities.ALL_DATA_THRESHOLD.threshold() )
                                                       .allMatch( next -> "foo".equals( next.threshold()
                                                                                            .getThresholdValueUnits() ) ) )
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
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actual, false );

        assertAll( () -> assertEquals( Formats.PNG_FORMAT, actualInterpolated.formats()
                                                                             .outputs()
                                                                             .getPng() ),

                   () -> assertEquals( Formats.SVG_FORMAT, actualInterpolated.formats()
                                                                             .outputs()
                                                                             .getSvg() )
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
                thresholds: [1]
                classifier_thresholds: [0.1]
                threshold_sets:
                  - thresholds: [2]
                  - classifier_thresholds: [0.2]
                metrics:
                  - mean square error skill score
                  - probability of detection
                """;

        EvaluationDeclaration actualDeclaration = DeclarationFactory.from( yaml );
        EvaluationDeclaration actualInterpolated = DeclarationInterpolator.interpolate( actualDeclaration, false );

        Set<Metric> actual = actualInterpolated.metrics();

        Threshold one = Threshold.newBuilder()
                                 .setObservedThresholdValue( 1 )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .build();
        wres.config.components.Threshold oneWrapped =
                ThresholdBuilder.builder()
                                .threshold( one )
                                .type( ThresholdType.VALUE )
                                .build();

        Threshold two = Threshold.newBuilder()
                                 .setObservedThresholdValue( 2 )
                                 .setOperator( Threshold.ThresholdOperator.GREATER )
                                 .build();

        wres.config.components.Threshold twoWrapped =
                ThresholdBuilder.builder()
                                .threshold( two )
                                .type( ThresholdType.VALUE )
                                .build();

        // Preserve insertion order
        Set<wres.config.components.Threshold> thresholds = new LinkedHashSet<>();
        thresholds.add( oneWrapped );
        thresholds.add( twoWrapped );
        thresholds.add( ALL_DATA_THRESHOLD );

        MetricParameters firstParameters =
                MetricParametersBuilder.builder()
                                       .thresholds( thresholds )
                                       .build();
        Metric first = MetricBuilder.builder()
                                    .name( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE )
                                    .parameters( firstParameters )
                                    .build();

        Threshold three = Threshold.newBuilder()
                                   .setObservedThresholdProbability( 0.1 )
                                   .setOperator( Threshold.ThresholdOperator.GREATER )
                                   .build();
        wres.config.components.Threshold threeWrapped =
                ThresholdBuilder.builder()
                                .threshold( three )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        Threshold four = Threshold.newBuilder()
                                  .setObservedThresholdProbability( 0.2 )
                                  .setOperator( Threshold.ThresholdOperator.GREATER )
                                  .build();

        wres.config.components.Threshold fourWrapped =
                ThresholdBuilder.builder()
                                .threshold( four )
                                .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                .build();

        // Preserve insertion order
        Set<wres.config.components.Threshold> classifierThresholds = new LinkedHashSet<>();
        classifierThresholds.add( threeWrapped );
        classifierThresholds.add( fourWrapped );

        // Re-use the value thresholds without the all data threshold
        thresholds.remove( ALL_DATA_THRESHOLD );

        MetricParameters secondParameters =
                MetricParametersBuilder.builder()
                                       .thresholds( thresholds )
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

    /**
     * Generates a boilerplate evaluation declaration with the inputs.
     * @param features the features
     * @param featureGroups the feature groups
     * @param addBaseline is true to add a baseline
     * @return the evaluation declaration
     */

    private static EvaluationDeclaration getBoilerplateEvaluationWith( Set<GeometryTuple> features,
                                                                       Set<GeometryGroup> featureGroups,
                                                                       boolean addBaseline )
    {
        Dataset left =
                DatasetBuilder.builder()
                              .featureAuthority( FeatureAuthority.CUSTOM )
                              .build();

        Dataset right =
                DatasetBuilder.builder()
                              .featureAuthority(
                                      FeatureAuthority.CUSTOM )
                              .build();

        EvaluationDeclarationBuilder builder =
                EvaluationDeclarationBuilder.builder()
                                            .features( FeaturesBuilder.builder()
                                                                      .geometries( features )
                                                                      .build() )
                                            .featureGroups( FeatureGroupsBuilder.builder()
                                                                                .geometryGroups( featureGroups )
                                                                                .build() )
                                            .left( left )
                                            .right( right );

        if ( addBaseline )
        {
            BaselineDataset baseline =
                    BaselineDatasetBuilder.builder()
                                          .dataset( DatasetBuilder.builder()
                                                                  .featureAuthority( FeatureAuthority.CUSTOM )
                                                                  .build() )
                                          .build();
            builder.baseline( baseline );
        }

        return builder.build();
    }
}