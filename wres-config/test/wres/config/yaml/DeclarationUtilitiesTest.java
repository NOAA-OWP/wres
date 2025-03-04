package wres.config.yaml;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisTimes;
import wres.config.yaml.components.AnalysisTimesBuilder;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.FeaturesBuilder;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.config.yaml.components.GeneratedBaselines;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.SeasonBuilder;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdSource;
import wres.config.yaml.components.ThresholdSourceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.Variable;
import wres.config.yaml.components.VariableBuilder;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.ReferenceTime;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;
import wres.statistics.generated.TimeScale;

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

        Features features = FeaturesBuilder.builder()
                                           .geometries( Collections.singleton( singleton ) )
                                           .build();
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( Set.of( grouped ) )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
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
                                      .sourceInterface( SourceInterface.WRDS_AHPS )
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
        AnalysisTimes analysisTimes =
                AnalysisTimesBuilder.builder()
                                    .minimum( java.time.Duration.ofHours( 3 ) )
                                    .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();

        assertTrue( DeclarationUtilities.hasAnalysisTimes( evaluation ) );
    }

    @Test
    void testHasAnalysisDurationsReturnsFalse()
    {
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .build();

        assertFalse( DeclarationUtilities.hasAnalysisTimes( evaluation ) );
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
                                                               .setLeftThresholdValue( 23.0 )
                                                               .setOperator( Threshold.ThresholdOperator.GREATER )
                                                               .build() )
                                          .type( ThresholdType.VALUE )
                                          .featureNameFrom( DatasetOrientation.LEFT )
                                          .build() );
        Metric one = MetricBuilder.Metric( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( thresholdsOne )
                                                                  .build() );
        Metric two = MetricBuilder.Metric( MetricConstants.MEAN_ERROR,
                                           MetricParametersBuilder.builder()
                                                                  .ensembleAverageType( Pool.EnsembleAverageType.MEDIAN )
                                                                  .thresholds( thresholdsOne )
                                                                  .build() );

        Set<wres.config.yaml.components.Threshold> thresholdsTwo
                = Set.of( ThresholdBuilder.builder()
                                          .threshold( Threshold.newBuilder()
                                                               .setLeftThresholdValue( 0.3 )
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

    @Test
    void testGetMetricGroupsForProcessingWithTimingErrorSummaryStatistics()
    {
        Set<SummaryStatistic> summaryStatisticsOne = new LinkedHashSet<>();
        SummaryStatistic.Builder template = SummaryStatistic.newBuilder()
                                                            .setDimension( SummaryStatistic.StatisticDimension.TIMING_ERRORS );
        SummaryStatistic mean = template.setStatistic( SummaryStatistic.StatisticName.MEAN )
                                        .build();
        SummaryStatistic median = template.setStatistic( SummaryStatistic.StatisticName.MEDIAN )
                                          .build();
        SummaryStatistic meanAbsolute = template.setStatistic( SummaryStatistic.StatisticName.MEAN_ABSOLUTE )
                                                .build();
        summaryStatisticsOne.add( mean );
        summaryStatisticsOne.add( median );
        summaryStatisticsOne.add( meanAbsolute );

        MetricParameters parametersOne = MetricParametersBuilder.builder()
                                                                .summaryStatistics( summaryStatisticsOne )
                                                                .build();
        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.TIME_TO_PEAK_ERROR )
                                        .parameters( parametersOne )
                                        .build();


        Set<SummaryStatistic> summaryStatisticsTwo = new LinkedHashSet<>();
        SummaryStatistic sd = template.setStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION )
                                      .build();
        SummaryStatistic minimum = template.setStatistic( SummaryStatistic.StatisticName.MINIMUM )
                                           .build();
        SummaryStatistic maximum = template.setStatistic( SummaryStatistic.StatisticName.MAXIMUM )
                                           .build();
        summaryStatisticsTwo.add( sd );
        summaryStatisticsTwo.add( minimum );
        summaryStatisticsTwo.add( maximum );

        MetricParameters parametersTwo = MetricParametersBuilder.builder()
                                                                .summaryStatistics( summaryStatisticsTwo )
                                                                .build();
        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR )
                                        .parameters( parametersTwo )
                                        .build();
        Set<Set<Metric>> actual = DeclarationUtilities.getMetricGroupsForProcessing( Set.of( metricOne, metricTwo ) );

        Set<Set<Metric>> expected =
                Set.of( Set.of( metricOne,
                                metricTwo,
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEAN, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEDIAN, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_ERROR_MEAN_ABSOLUTE, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_MINIMUM, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_MAXIMUM, null ),
                                new Metric( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STANDARD_DEVIATION, null ) ) );

        assertEquals( expected, actual );
    }

    @Test
    void testAddDataSources()
    {
        // Create some sources with parameters to correlate
        Source leftOne = SourceBuilder.builder()
                                      .uri( URI.create( "foo.csv" ) )
                                      .timeZoneOffset( ZoneOffset.ofHours( 3 ) )
                                      .build();
        Source leftTwo = SourceBuilder.builder()
                                      .uri( URI.create( "qux.csv" ) )
                                      .timeZoneOffset( ZoneOffset.ofHours( 4 ) )
                                      .build();
        List<wres.config.yaml.components.Source> leftSources = List.of( leftOne, leftTwo );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .build();
        Source rightOne = SourceBuilder.builder()
                                       .uri( URI.create( "bar.csv" ) )
                                       .sourceInterface( SourceInterface.NWM_LONG_RANGE_CHANNEL_RT_CONUS )
                                       .build();
        List<wres.config.yaml.components.Source> rightSources = List.of( rightOne );
        Dataset right = DatasetBuilder.builder()
                                      .sources( rightSources )
                                      .build();
        Source baselineOne = SourceBuilder.builder()
                                          .uri( URI.create( "baz.csv" ) )
                                          .missingValue( List.of( -999.0 ) )
                                          .build();
        List<wres.config.yaml.components.Source> baselineSources = List.of( baselineOne );
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .build() )
                                      .build();

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .left( left )
                                                                                  .right( right )
                                                                                  .baseline( baseline )
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI leftCorrelated = URI.create( "foopath/foo.csv" );
        URI leftUncorrelated = URI.create( "foopath/fooest.csv" );

        URI rightCorrelated = URI.create( "barpath/bar.csv" );
        URI rightUncorrelated = URI.create( "barpath/barest.csv" );

        URI baselineCorrelated = URI.create( "bazpath/baz.csv" );
        URI baselineUncorrelated = URI.create( "bazpath/bazest.csv" );

        List<URI> newLeftSources = List.of( leftCorrelated, leftUncorrelated );
        List<URI> newRightSources = List.of( rightCorrelated, rightUncorrelated );
        List<URI> newBaselineSources = List.of( baselineCorrelated, baselineUncorrelated );

        EvaluationDeclaration actual = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                            newLeftSources,
                                                                            newRightSources,
                                                                            newBaselineSources );

        // Create the expectation
        Source leftSourceExpectedOne = SourceBuilder.builder( leftOne )
                                                    .uri( leftCorrelated )
                                                    .build();
        Source leftSourceExpectedTwo = SourceBuilder.builder()
                                                    .uri( leftUncorrelated )
                                                    .build();
        Source rightSourceExpectedOne = SourceBuilder.builder( rightOne )
                                                     .uri( rightCorrelated )
                                                     .build();
        Source rightSourceExpectedTwo = SourceBuilder.builder()
                                                     .uri( rightUncorrelated )
                                                     .build();
        Source baselineSourceExpectedOne = SourceBuilder.builder( baselineOne )
                                                        .uri( baselineCorrelated )
                                                        .build();
        Source baselineSourceExpectedTwo = SourceBuilder.builder()
                                                        .uri( baselineUncorrelated )
                                                        .build();
        List<Source> leftSourcesExpected = List.of( leftTwo, leftSourceExpectedOne, leftSourceExpectedTwo );
        Dataset leftExpected = DatasetBuilder.builder()
                                             .sources( leftSourcesExpected )
                                             .build();
        List<Source> rightSourcesExpected = List.of( rightSourceExpectedOne, rightSourceExpectedTwo );
        Dataset rightExpected = DatasetBuilder.builder()
                                              .sources( rightSourcesExpected )
                                              .build();
        List<Source> baselineSourcesExpected = List.of( baselineSourceExpectedOne, baselineSourceExpectedTwo );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .build();
        BaselineDataset baselineExpected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( leftExpected )
                                                                     .right( rightExpected )
                                                                     .baseline( baselineExpected )
                                                                     .build();
        assertEquals( expected, actual );
    }

    /**
     * See Redmine issue #116899
     */

    @Test
    void testAddDataSourcesRetainsExistingSources()
    {
        Source baselineOne =
                SourceBuilder.builder()
                             .uri( URI.create( "singleValuedEx_ABRFC_ARCFUL_OBS/FLTA4X.QINE.19951101.20170905.datacard" ) )
                             .build();
        Source baselineTwo =
                SourceBuilder.builder()
                             .uri( URI.create( "singleValuedEx_ABRFC_ARCFUL_OBS/FRSO2X.QINE.19951101.20170905.datacard" ) )
                             .build();

        List<wres.config.yaml.components.Source> baselineSources = List.of( baselineOne, baselineTwo );
        TimeScale timeScaleInner = TimeScale.newBuilder()
                                            .setPeriod( com.google.protobuf.Duration.newBuilder().setSeconds( 1 ) )
                                            .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                            .build();
        wres.config.yaml.components.TimeScale timeScale = new wres.config.yaml.components.TimeScale( timeScaleInner );
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baseline =
                BaselineDatasetBuilder.builder()
                                      .dataset( DatasetBuilder.builder()
                                                              .sources( baselineSources )
                                                              .variable( new Variable( "QINE", null, Set.of() ) )
                                                              .type( DataType.OBSERVATIONS )
                                                              .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                              .timeScale( timeScale )
                                                              .build() )
                                      .generatedBaseline( persistence )
                                      .build();

        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .baseline( baseline )
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI sourceOne = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_9291293271822018547" );
        URI sourceTwo = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_11687359385535593111" );
        URI sourceThree = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_16018604822676150580" );
        URI sourceFour = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_14964912810788706087" );
        URI sourceFive = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_4655376427529148367" );
        URI sourceSix = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_9748034963021086804" );
        URI sourceSeven = URI.create( "file:///mnt/wres_share/input_data/2831045288138671584_17342198904464396701" );

        List<URI> newBaselineSources = List.of( sourceOne,
                                                sourceTwo,
                                                sourceThree,
                                                sourceFour,
                                                sourceFive,
                                                sourceSix,
                                                sourceSeven );

        EvaluationDeclaration actualEvaluation = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                                      List.of(),
                                                                                      List.of(),
                                                                                      newBaselineSources );

        // Create the expectation
        Source baselineSourceExpectedOne = SourceBuilder.builder()
                                                        .uri( sourceOne )
                                                        .build();
        Source baselineSourceExpectedTwo = SourceBuilder.builder()
                                                        .uri( sourceTwo )
                                                        .build();
        Source baselineSourceExpectedThree = SourceBuilder.builder()
                                                          .uri( sourceThree )
                                                          .build();
        Source baselineSourceExpectedFour = SourceBuilder.builder()
                                                         .uri( sourceFour )
                                                         .build();
        Source baselineSourceExpectedFive = SourceBuilder.builder()
                                                         .uri( sourceFive )
                                                         .build();
        Source baselineSourceExpectedSix = SourceBuilder.builder()
                                                        .uri( sourceSix )
                                                        .build();
        Source baselineSourceExpectedSeven = SourceBuilder.builder()
                                                          .uri( sourceSeven )
                                                          .build();

        List<Source> baselineSourcesExpected = List.of( baselineOne,
                                                        baselineTwo,
                                                        baselineSourceExpectedOne,
                                                        baselineSourceExpectedTwo,
                                                        baselineSourceExpectedThree,
                                                        baselineSourceExpectedFour,
                                                        baselineSourceExpectedFive,
                                                        baselineSourceExpectedSix,
                                                        baselineSourceExpectedSeven );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .variable( new Variable( "QINE", null, Set.of() ) )
                                                        .type( DataType.OBSERVATIONS )
                                                        .timeZoneOffset( ZoneOffset.ofHours( -6 ) )
                                                        .timeScale( timeScale )
                                                        .build();
        BaselineDataset expected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .generatedBaseline( persistence )
                                      .build();

        BaselineDataset actual = actualEvaluation.baseline();

        assertEquals( expected, actual );
    }

    @Test
    void testAddDataSourcesWithMissingDatasets()
    {
        // Create some sources
        EvaluationDeclaration evaluationDeclaration = EvaluationDeclarationBuilder.builder()
                                                                                  .build();

        // Create some correlated and some uncorrelated URIs
        URI leftSource = URI.create( "foopath/foo.csv" );
        URI rightSource = URI.create( "barpath/bar.csv" );
        URI baselineSource = URI.create( "bazpath/baz.csv" );

        List<URI> newLeftSources = List.of( leftSource );
        List<URI> newRightSources = List.of( rightSource );
        List<URI> newBaselineSources = List.of( baselineSource );

        EvaluationDeclaration actual = DeclarationUtilities.addDataSources( evaluationDeclaration,
                                                                            newLeftSources,
                                                                            newRightSources,
                                                                            newBaselineSources );

        // Create the expectation
        Source leftSourceExpectedOne = SourceBuilder.builder()
                                                    .uri( leftSource )
                                                    .build();
        Source rightSourceExpectedOne = SourceBuilder.builder()
                                                     .uri( rightSource )
                                                     .build();
        Source baselineSourceExpectedOne = SourceBuilder.builder()
                                                        .uri( baselineSource )
                                                        .build();
        List<Source> leftSourcesExpected = List.of( leftSourceExpectedOne );
        Dataset leftExpected = DatasetBuilder.builder()
                                             .sources( leftSourcesExpected )
                                             .build();
        List<Source> rightSourcesExpected = List.of( rightSourceExpectedOne );
        Dataset rightExpected = DatasetBuilder.builder()
                                              .sources( rightSourcesExpected )
                                              .build();
        List<Source> baselineSourcesExpected = List.of( baselineSourceExpectedOne );
        Dataset baselineDatasetExpected = DatasetBuilder.builder()
                                                        .sources( baselineSourcesExpected )
                                                        .build();
        BaselineDataset baselineExpected =
                BaselineDatasetBuilder.builder()
                                      .dataset( baselineDatasetExpected )
                                      .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .left( leftExpected )
                                                                     .right( rightExpected )
                                                                     .baseline( baselineExpected )
                                                                     .build();
        assertEquals( expected, actual );
    }

    @Test
    void testIsReadableFileReturnsFalseForInvalidPath()
    {
        String path = """
                observed:
                  - some_file.csv
                predicted:
                  - another_file.csv
                """;

        FileSystem fileSystem = FileSystems.getDefault();
        assertFalse( DeclarationUtilities.isReadableFile( fileSystem, path ) );
    }

    @Test
    void testIsReadableFileReturnsTrueForReadableFile() throws IOException
    {
        try ( FileSystem fileSystem = Jimfs.newFileSystem( Configuration.unix() ) )
        {
            Path path = fileSystem.getPath( "foo.file" );
            Files.createFile( path );
            String pathString = path.toString();

            assertTrue( DeclarationUtilities.isReadableFile( fileSystem, pathString ) );
        }
    }

    @Test
    void testGetSourceTimeScales()
    {
        TimeScale scaleOne = TimeScale.newBuilder()
                                      .setFunction( TimeScale.TimeScaleFunction.MEAN )
                                      .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                              .setSeconds( 100 )
                                                                              .build() )
                                      .build();
        wres.config.yaml.components.TimeScale scaleOneWrapped = new wres.config.yaml.components.TimeScale( scaleOne );
        Source leftSourceOne = SourceBuilder.builder()
                                            .build();
        TimeScale scaleTwo = TimeScale.newBuilder()
                                      .setFunction( TimeScale.TimeScaleFunction.TOTAL )
                                      .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                              .setSeconds( 789 )
                                                                              .build() )
                                      .build();
        wres.config.yaml.components.TimeScale scaleTwoWrapped = new wres.config.yaml.components.TimeScale( scaleTwo );
        Source rightSourceOne = SourceBuilder.builder()
                                             .build();
        TimeScale scaleThree = TimeScale.newBuilder()
                                        .setFunction( TimeScale.TimeScaleFunction.MAXIMUM )
                                        .setPeriod( com.google.protobuf.Duration.newBuilder()
                                                                                .setSeconds( 1800 )
                                                                                .build() )
                                        .build();
        wres.config.yaml.components.TimeScale scaleThreeWrapped =
                new wres.config.yaml.components.TimeScale( scaleThree );
        Source baselineSourceOne = SourceBuilder.builder()
                                                .build();
        List<Source> leftSources = List.of( leftSourceOne );
        Dataset left = DatasetBuilder.builder()
                                     .sources( leftSources )
                                     .timeScale( scaleOneWrapped )
                                     .build();
        List<Source> rightSources = List.of( rightSourceOne );
        Dataset right = DatasetBuilder.builder()
                                      .sources( rightSources )
                                      .timeScale( scaleTwoWrapped )
                                      .build();
        List<Source> baselineSources = List.of( baselineSourceOne );
        Dataset baselineDataset = DatasetBuilder.builder()
                                                .sources( baselineSources )
                                                .timeScale( scaleThreeWrapped )
                                                .build();
        BaselineDataset baseline = BaselineDatasetBuilder.builder()
                                                         .dataset( baselineDataset )
                                                         .build();

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .left( left )
                                                                       .right( right )
                                                                       .baseline( baseline )
                                                                       .build();

        Set<TimeScale> actual = DeclarationUtilities.getSourceTimeScales( evaluation );
        Set<TimeScale> expected = Set.of( scaleOne, scaleTwo, scaleThree );
        assertEquals( expected, actual );
    }

    @Test
    void testGetEarliestAnalysisDuration()
    {
        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .minimum( Duration.ZERO )
                                                          .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();
        assertEquals( Duration.ZERO, DeclarationUtilities.getEarliestAnalysisDuration( evaluation ) );
    }

    @Test
    void testGetLatestAnalysisDuration()
    {
        AnalysisTimes analysisTimes = AnalysisTimesBuilder.builder()
                                                          .maximum( Duration.ZERO )
                                                          .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .analysisTimes( analysisTimes )
                                                                       .build();
        assertEquals( Duration.ZERO, DeclarationUtilities.getLatestAnalysisDuration( evaluation ) );
    }

    @Test
    void testGetStartOfSeason()
    {
        MonthDay startOfSeason = MonthDay.of( 1, 2 );
        wres.config.yaml.components.Season season = SeasonBuilder.builder()
                                                                 .minimum( startOfSeason )
                                                                 .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .season( season )
                                                                       .build();
        assertEquals( startOfSeason, DeclarationUtilities.getStartOfSeason( evaluation ) );
    }

    @Test
    void testGetEndOfSeason()
    {
        MonthDay endOfSeason = MonthDay.of( 1, 2 );
        wres.config.yaml.components.Season season = SeasonBuilder.builder()
                                                                 .maximum( endOfSeason )
                                                                 .build();
        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .season( season )
                                                                       .build();
        assertEquals( endOfSeason, DeclarationUtilities.getEndOfSeason( evaluation ) );
    }

    @Test
    void testHasProbabilityThresholds()
    {
        Threshold probability = Threshold.newBuilder()
                                         .build();
        wres.config.yaml.components.Threshold wrapped = ThresholdBuilder.builder()
                                                                        .threshold( probability )
                                                                        .type( ThresholdType.PROBABILITY )
                                                                        .build();
        EvaluationDeclaration topLevelEvaluation =
                EvaluationDeclarationBuilder.builder()
                                            .probabilityThresholds( Set.of( wrapped ) )
                                            .build();
        EvaluationDeclaration thresholdSetEvaluation = EvaluationDeclarationBuilder.builder()
                                                                                   .thresholdSets( Set.of( wrapped ) )
                                                                                   .build();
        ThresholdSource thresholdSource = ThresholdSourceBuilder.builder()
                                                                .type( ThresholdType.PROBABILITY )
                                                                .build();
        EvaluationDeclaration serviceEvaluation =
                EvaluationDeclarationBuilder.builder()
                                            .thresholdSources( Set.of( thresholdSource ) )
                                            .build();
        Metric metric = MetricBuilder.builder()
                                     .parameters( MetricParametersBuilder.builder()
                                                                         .probabilityThresholds( Set.of( wrapped ) )
                                                                         .build() )
                                     .build();
        EvaluationDeclaration metricEvaluation = EvaluationDeclarationBuilder.builder()
                                                                             .metrics( Set.of( metric ) )
                                                                             .build();
        EvaluationDeclaration emptyEvaluation = EvaluationDeclarationBuilder.builder()
                                                                            .build();
        assertAll( () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( topLevelEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( thresholdSetEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( serviceEvaluation ) ),
                   () -> assertTrue( DeclarationUtilities.hasProbabilityThresholds( metricEvaluation ) ),
                   () -> assertFalse( DeclarationUtilities.hasProbabilityThresholds( emptyEvaluation ) ) );
    }

    @Test
    void testHasGeneratedBaseline()
    {
        Dataset dataset = DatasetBuilder.builder()
                                        .build();
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .method( GeneratedBaselines.PERSISTENCE )
                                                                .build();
        BaselineDataset baselineDataset = BaselineDatasetBuilder.builder().dataset( dataset )
                                                                .generatedBaseline( persistence )
                                                                .build();
        BaselineDataset anotherBaselineDataset = BaselineDatasetBuilder.builder()
                                                                       .dataset( dataset )
                                                                       .build();
        assertAll( () -> assertTrue( DeclarationUtilities.hasGeneratedBaseline( baselineDataset ) ),
                   () -> assertFalse( DeclarationUtilities.hasGeneratedBaseline( anotherBaselineDataset ) ) );
    }

    @Test
    void testGetVariableName()
    {
        Dataset left = DatasetBuilder.builder()
                                     .variable( VariableBuilder.builder()
                                                               .name( "foo" )
                                                               .build() )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .variable( VariableBuilder.builder()
                                                                .name( "bar" )
                                                                .build() )
                                      .build();
        Dataset baseline = DatasetBuilder.builder()
                                         .variable( VariableBuilder.builder()
                                                                   .name( "baz" )
                                                                   .build() )
                                         .build();
        assertAll( () -> assertEquals( "foo", DeclarationUtilities.getVariableName( left ) ),
                   () -> assertEquals( "bar", DeclarationUtilities.getVariableName( right ) ),
                   () -> assertEquals( "baz", DeclarationUtilities.getVariableName( baseline ) ) );
    }

    @Test
    void testIsForecast()
    {
        Dataset left = DatasetBuilder.builder()
                                     .type( DataType.ENSEMBLE_FORECASTS )
                                     .build();
        Dataset right = DatasetBuilder.builder()
                                      .type( DataType.SINGLE_VALUED_FORECASTS )
                                      .build();
        Dataset baseline = DatasetBuilder.builder()
                                         .type( DataType.OBSERVATIONS )
                                         .build();
        assertAll( () -> assertTrue( DeclarationUtilities.isForecast( left ) ),
                   () -> assertTrue( DeclarationUtilities.isForecast( right ) ),
                   () -> assertFalse( DeclarationUtilities.isForecast( baseline ) ) );
    }

    @Test
    void testGetReferenceTimeType()
    {
        assertAll( () -> assertEquals( ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME,
                                       DeclarationUtilities.getReferenceTimeType( DataType.OBSERVATIONS ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.ANALYSIS_START_TIME,
                                       DeclarationUtilities.getReferenceTimeType( DataType.ANALYSES ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.T0,
                                       DeclarationUtilities.getReferenceTimeType( DataType.ENSEMBLE_FORECASTS ) ),
                   () -> assertEquals( ReferenceTime.ReferenceTimeType.T0,
                                       DeclarationUtilities.getReferenceTimeType( DataType.SINGLE_VALUED_FORECASTS ) ) );
    }

    @Test
    void testGetThresholds()
    {
        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( 1.0 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( 2.0 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdValue( 3.0 )
                                   .build();
        wres.config.yaml.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                             .threshold( three )
                                                                             .type( ThresholdType.VALUE )
                                                                             .build();

        Metric metric = MetricBuilder.builder()
                                     .parameters( MetricParametersBuilder.builder()
                                                                         .thresholds( Set.of( wrappedThree ) )
                                                                         .build() )
                                     .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .thresholds( Set.of( wrappedOne ) )
                                            .thresholdSets( Set.of( wrappedTwo ) )
                                            .metrics( Set.of( metric ) )
                                            .build();

        Set<wres.config.yaml.components.Threshold> actual = DeclarationUtilities.getInbandThresholds( evaluation );
        Set<wres.config.yaml.components.Threshold> expected = Set.of( wrappedOne, wrappedTwo, wrappedThree );

        assertEquals( expected, actual );
    }

    @Test
    void testHasMissingDataTypes()
    {
        Dataset missing = DatasetBuilder.builder()
                                        .build();
        Dataset present = DatasetBuilder.builder()
                                        .type( DataType.OBSERVATIONS )
                                        .build();

        EvaluationDeclaration withAllMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( missing )
                                              .right( missing )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( missing )
                                                                               .build() )
                                              .build();

        EvaluationDeclaration withOneMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( missing )
                                              .right( present )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( missing )
                                                                               .build() )
                                              .build();

        EvaluationDeclaration withNoneMissing
                = EvaluationDeclarationBuilder.builder()
                                              .left( present )
                                              .right( present )
                                              .baseline( BaselineDatasetBuilder.builder()
                                                                               .dataset( present )
                                                                               .build() )
                                              .build();

        assertAll( () -> assertTrue( DeclarationUtilities.hasMissingDataTypes( withOneMissing ) ),
                   () -> assertTrue( DeclarationUtilities.hasMissingDataTypes( withAllMissing ) ),
                   () -> assertFalse( DeclarationUtilities.hasMissingDataTypes( withNoneMissing ) ) );

    }

    @Test
    void testAddThresholdsToDeclaration()
    {
        Metric metricOne = MetricBuilder.builder()
                                        .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                                        .build();

        Metric metricTwo = MetricBuilder.builder()
                                        .name( MetricConstants.PROBABILITY_OF_DETECTION )
                                        .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .metrics( Set.of( metricOne, metricTwo ) )
                                            .build();

        Threshold one = Threshold.newBuilder()
                                 .setLeftThresholdValue( 0.1 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedOne = ThresholdBuilder.builder()
                                                                           .threshold( one )
                                                                           .type( ThresholdType.VALUE )
                                                                           .build();
        Threshold two = Threshold.newBuilder()
                                 .setLeftThresholdValue( 0.2 )
                                 .build();
        wres.config.yaml.components.Threshold wrappedTwo = ThresholdBuilder.builder()
                                                                           .threshold( two )
                                                                           .type( ThresholdType.PROBABILITY )
                                                                           .build();
        Threshold three = Threshold.newBuilder()
                                   .setLeftThresholdValue( 0.3 )
                                   .build();
        wres.config.yaml.components.Threshold wrappedThree = ThresholdBuilder.builder()
                                                                             .threshold( three )
                                                                             .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                                                             .build();
        Set<wres.config.yaml.components.Threshold> thresholds = Set.of( wrappedOne, wrappedTwo, wrappedThree );
        EvaluationDeclaration actual = DeclarationUtilities.addThresholds( evaluation, thresholds );

        Metric expectedMetricOne =
                MetricBuilder.builder()
                             .name( MetricConstants.MEAN_ABSOLUTE_ERROR )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                 .thresholds( Set.of( wrappedOne ) )
                                                                 .build() )
                             .build();

        Metric expectedMetricTwo =
                MetricBuilder.builder()
                             .name( MetricConstants.PROBABILITY_OF_DETECTION )
                             .parameters( MetricParametersBuilder.builder()
                                                                 .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                 .thresholds( Set.of( wrappedOne ) )
                                                                 .classifierThresholds( Set.of( wrappedThree ) )
                                                                 .build() )
                             .build();
        Set<Metric> expectedMetrics = Set.of( expectedMetricOne, expectedMetricTwo );
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .thresholds( Set.of( wrappedOne ) )
                                                                     .probabilityThresholds( Set.of( wrappedTwo ) )
                                                                     .classifierThresholds( Set.of( wrappedThree ) )
                                                                     .metrics( expectedMetrics )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testRemoveFeaturesWithoutThresholds()
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
        Geometry allDataGeometry = Geometry.newBuilder()
                                           .setName( "qux" )
                                           .build();

        // Tuple foo-bar-baz
        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();
        // Tuple baz-foo-bar
        GeometryTuple two = GeometryTuple.newBuilder()
                                         .setLeft( baseline )
                                         .setRight( left )
                                         .setBaseline( right )
                                         .build();
        // Tuple bar-baz-foo
        GeometryTuple three = GeometryTuple.newBuilder()
                                           .setLeft( right )
                                           .setRight( baseline )
                                           .setBaseline( left )
                                           .build();

        // Tuple qux
        GeometryTuple four = GeometryTuple.newBuilder()
                                          .setLeft( allDataGeometry )
                                          .setRight( allDataGeometry )
                                          .setBaseline( allDataGeometry )
                                          .build();

        Threshold threshold = Threshold.newBuilder()
                                       .setLeftThresholdValue( 1 )
                                       .build();
        wres.config.yaml.components.Threshold wrappedThresholdOne =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( left )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.yaml.components.Threshold wrappedThresholdTwo =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( right )
                                .featureNameFrom( DatasetOrientation.RIGHT )
                                .build();
        wres.config.yaml.components.Threshold wrappedThresholdThree =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( baseline )
                                .featureNameFrom( DatasetOrientation.BASELINE )
                                .build();
        wres.config.yaml.components.Threshold allDataThreshold =
                ThresholdBuilder.builder( DeclarationUtilities.GENERATED_ALL_DATA_THRESHOLD )
                                .feature( allDataGeometry )
                                .build();

        Set<GeometryTuple> geometryTuples = Set.of( one, two, three, four );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometryTuples )
                                           .build();
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .features( features )
                                            .featureGroups( featureGroups )
                                            .thresholds( Set.of( wrappedThresholdOne,
                                                                 wrappedThresholdTwo,
                                                                 wrappedThresholdThree,
                                                                 allDataThreshold ) )
                                            .build();

        EvaluationDeclaration actual = DeclarationUtilities.removeFeaturesWithoutThresholds( declaration );

        Features expectedFeatures = FeaturesBuilder.builder()
                                                   .geometries( Collections.singleton( one ) )
                                                   .build();
        GeometryGroup expectedGroup = GeometryGroup.newBuilder()
                                                   .addGeometryTuples( one )
                                                   .setRegionName( "foorbarbaz" )
                                                   .build();
        FeatureGroups expectedFeatureGroups = FeatureGroupsBuilder.builder()
                                                                  .geometryGroups( Set.of( expectedGroup ) )
                                                                  .build();
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .features( expectedFeatures )
                                                                     .featureGroups( expectedFeatureGroups )
                                                                     .thresholds( Set.of( wrappedThresholdOne,
                                                                                          wrappedThresholdTwo,
                                                                                          wrappedThresholdThree,
                                                                                          allDataThreshold ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testRemoveFeaturesWithoutThresholdsRemovesNoFeaturesWhenEachFeatureHasAThresholdWithADifferentFeatureNameOrientation()
    {
        // Tests GitHub issue #319
        Geometry oneObserved = Geometry.newBuilder()
                                       .setName( "one_observed" )
                                       .build();
        Geometry onePredicted = Geometry.newBuilder()
                                        .setName( "one_predicted" )
                                        .build();
        Geometry twoObserved = Geometry.newBuilder()
                                       .setName( "two_observed" )
                                       .build();
        Geometry twoPredicted = Geometry.newBuilder()
                                        .setName( "two_predicted" )
                                        .build();

        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( oneObserved )
                                         .setRight( onePredicted )
                                         .build();
        GeometryTuple two = GeometryTuple.newBuilder()
                                         .setLeft( twoObserved )
                                         .setRight( twoPredicted )
                                         .build();

        Threshold threshold = Threshold.newBuilder()
                                       .setLeftThresholdValue( 1 )
                                       .build();
        wres.config.yaml.components.Threshold wrappedThresholdOne =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( oneObserved )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();
        wres.config.yaml.components.Threshold wrappedThresholdTwo =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( twoPredicted )
                                .featureNameFrom( DatasetOrientation.RIGHT )
                                .build();

        Set<GeometryTuple> geometryTuples = Set.of( one, two );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometryTuples )
                                           .build();

        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .features( features )
                                            .thresholds( Set.of( wrappedThresholdOne,
                                                                 wrappedThresholdTwo ) )
                                            .build();

        EvaluationDeclaration actual = DeclarationUtilities.removeFeaturesWithoutThresholds( declaration );

        Features expectedFeatures = FeaturesBuilder.builder()
                                                   .geometries( geometryTuples )
                                                   .build();

        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .features( expectedFeatures )
                                                                     .thresholds( Set.of( wrappedThresholdOne,
                                                                                          wrappedThresholdTwo ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testRemoveFeaturesWithoutThresholdsWhenThresholdsContainSingleDataOrientation()
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

        // Tuple foo-bar-baz
        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();

        Threshold threshold = Threshold.newBuilder()
                                       .setLeftThresholdValue( 1 )
                                       .build();
        wres.config.yaml.components.Threshold wrappedThresholdOne =
                ThresholdBuilder.builder()
                                .threshold( threshold )
                                .feature( left )
                                .featureNameFrom( DatasetOrientation.LEFT )
                                .build();

        Set<GeometryTuple> geometryTuples = Set.of( one );
        Features features = FeaturesBuilder.builder()
                                           .geometries( geometryTuples )
                                           .build();
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .features( features )
                                            .featureGroups( featureGroups )
                                            .thresholds( Set.of( wrappedThresholdOne ) )
                                            .build();

        EvaluationDeclaration actual = DeclarationUtilities.removeFeaturesWithoutThresholds( declaration );

        Features expectedFeatures = FeaturesBuilder.builder()
                                                   .geometries( Collections.singleton( one ) )
                                                   .build();
        GeometryGroup expectedGroup = GeometryGroup.newBuilder()
                                                   .addGeometryTuples( one )
                                                   .setRegionName( "foorbarbaz" )
                                                   .build();
        FeatureGroups expectedFeatureGroups = FeatureGroupsBuilder.builder()
                                                                  .geometryGroups( Set.of( expectedGroup ) )
                                                                  .build();
        EvaluationDeclaration expected = EvaluationDeclarationBuilder.builder()
                                                                     .features( expectedFeatures )
                                                                     .featureGroups( expectedFeatureGroups )
                                                                     .thresholds( Set.of( wrappedThresholdOne ) )
                                                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    void testHasFeatureGroups()
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

        GeometryTuple one = GeometryTuple.newBuilder()
                                         .setLeft( left )
                                         .setRight( right )
                                         .setBaseline( baseline )
                                         .build();

        Set<GeometryTuple> geometryTuples = Set.of( one );
        GeometryGroup group = GeometryGroup.newBuilder()
                                           .addAllGeometryTuples( geometryTuples )
                                           .setRegionName( "foorbarbaz" )
                                           .build();
        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( Set.of( group ) )
                                                          .build();
        EvaluationDeclaration declaration =
                EvaluationDeclarationBuilder.builder()
                                            .featureGroups( featureGroups )
                                            .build();

        FeatureServiceGroup serviceGroup = new FeatureServiceGroup( "a", "b", true );
        FeatureService featureService = new FeatureService( URI.create( "http://foo.bar" ), Set.of( serviceGroup ) );
        EvaluationDeclaration declarationTwo =
                EvaluationDeclarationBuilder.builder()
                                            .featureService( featureService )
                                            .build();

        assertAll( () -> assertTrue( DeclarationUtilities.hasFeatureGroups( declaration ) ),
                   () -> assertTrue( DeclarationUtilities.hasFeatureGroups( declarationTwo ) ),
                   () -> assertFalse( DeclarationUtilities.hasFeatureGroups( EvaluationDeclarationBuilder.builder()
                                                                                                         .build() ) ) );
    }
}