package wres.pipeline;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.MetricConstants;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureGroupsBuilder;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimePoolsBuilder;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.thresholds.MetricsAndThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeWindowSlicer;
import wres.metrics.SummaryStatisticsCalculator;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.Threshold;

/**
 * Tests the {@link EvaluationUtilities}.
 * @author James Brown
 */

class EvaluationUtilitiesTest
{
    @Test
    void testGetSummaryStatisticsCalculatorsWithTwoTimeWindowsAndTwoThresholdsAcrossAllFeatures()
    {
        // Create the declaration
        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                                  java.time.Duration.ofHours( 40 ) );
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( java.time.Duration.ofHours( 23 ) )
                                                  .frequency( java.time.Duration.ofHours( 17 ) )
                                                  .build();

        Threshold pOneValue = Threshold.newBuilder()
                                       .setLeftThresholdValue( 5 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                       .build();

        wres.config.yaml.components.Threshold pOneValueWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pOneValue )
                                  .type( ThresholdType.VALUE )
                                  .build();

        Threshold pTwoValue = Threshold.newBuilder()
                                       .setLeftThresholdValue( 10 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                       .build();

        wres.config.yaml.components.Threshold pTwoValueWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pTwoValue )
                                  .type( ThresholdType.VALUE )
                                  .build();

        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( pOneValueWrapped );
        valueThresholds.add( pTwoValueWrapped );

        Threshold pOneClassifier = Threshold.newBuilder()
                                            .setLeftThresholdProbability( 0.05 )
                                            .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                            .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                            .build();

        wres.config.yaml.components.Threshold pOneClassifierWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pOneClassifier )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Threshold pTwoClassifier = Threshold.newBuilder()
                                            .setLeftThresholdProbability( 0.1 )
                                            .setOperator( Threshold.ThresholdOperator.LESS_EQUAL )
                                            .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                            .build();

        wres.config.yaml.components.Threshold pTwoClassifierWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pTwoClassifier )
                                  .type( ThresholdType.PROBABILITY_CLASSIFIER )
                                  .build();

        Set<wres.config.yaml.components.Threshold> classifierThresholds = new LinkedHashSet<>();
        classifierThresholds.add( pOneClassifierWrapped );
        classifierThresholds.add( pTwoClassifierWrapped );

        // Add some summary statistics
        SummaryStatistic first = SummaryStatistic.newBuilder()
                                                 .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                 .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                 .build();

        SummaryStatistic second = SummaryStatistic.newBuilder()
                                                  .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                  .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                  .setHistogramBins( 10 )
                                                  .build();

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( first );
        summaryStatistics.add( second );

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .leadTimes( leadTimeInterval )
                                            .leadTimePools( Collections.singleton( leadTimePools ) )
                                            .thresholds( valueThresholds )
                                            .classifierThresholds( classifierThresholds )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        Map<String, List<SummaryStatisticsCalculator>> calculators =
                EvaluationUtilities.getSummaryStatisticsCalculators( evaluation, 0, false );

        // Eight filters
        assertEquals( 8, calculators.values()
                                    .stream()
                                    .mapToInt( List::size )
                                    .sum() );
    }

    @Test
    void testGetSummaryStatisticsCalculatorsForNamedThresholdsAcrossAllFeatures()
    {
        // Create the declaration
        Threshold pOneValue = Threshold.newBuilder()
                                       .setLeftThresholdValue( 5 )
                                       .setOperator( Threshold.ThresholdOperator.GREATER )
                                       .setDataType( Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                       .setName( "FLOOD" )
                                       .build();

        wres.config.yaml.components.Threshold pOneValueWrapped
                = ThresholdBuilder.builder()
                                  .threshold( pOneValue )
                                  .type( ThresholdType.VALUE )
                                  .build();

        Set<wres.config.yaml.components.Threshold> valueThresholds = new LinkedHashSet<>();
        valueThresholds.add( pOneValueWrapped );

        // Add some summary statistics
        SummaryStatistic first = SummaryStatistic.newBuilder()
                                                 .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                 .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                 .build();

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( first );

        EvaluationDeclaration evaluation = EvaluationDeclarationBuilder.builder()
                                                                       .thresholds( valueThresholds )
                                                                       .summaryStatistics( summaryStatistics )
                                                                       .build();

        Map<String, List<SummaryStatisticsCalculator>> calculators =
                EvaluationUtilities.getSummaryStatisticsCalculators( evaluation, 0, false );

        // One filter
        assertEquals( 1, calculators.size() );

        // Filter accepts arbitrary statistics with "FLOOD" threshold
        Threshold eventThreshold = pOneValue.toBuilder()
                                            .setLeftThresholdValue( 23.0 )
                                            .build();

        TimeWindowOuter big = TimeWindowSlicer.getOneBigTimeWindow( evaluation );

        Statistics statistics =
                Statistics.newBuilder()
                          .setPool( Pool.newBuilder()
                                        .setEventThreshold( eventThreshold )
                                        .setTimeWindow( big.getTimeWindow() ) )
                          .build();

        assertTrue( calculators.values().stream()
                               .flatMap( Collection::stream )
                               .anyMatch( c -> c.test( statistics ) ) );
    }

    @Test
    void testGetSummaryStatisticsCalculatorsWithTwoTimeWindowsAcrossFeatureGroups()
    {
        // Create the declaration
        LeadTimeInterval leadTimeInterval = new LeadTimeInterval( java.time.Duration.ofHours( 0 ),
                                                                  java.time.Duration.ofHours( 40 ) );
        TimePools leadTimePools = TimePoolsBuilder.builder()
                                                  .period( java.time.Duration.ofHours( 23 ) )
                                                  .frequency( java.time.Duration.ofHours( 17 ) )
                                                  .build();

        // Add some summary statistics
        SummaryStatistic first = SummaryStatistic.newBuilder()
                                                 .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                 .setDimension( SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                                                 .build();

        SummaryStatistic second = SummaryStatistic.newBuilder()
                                                  .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                  .setDimension( SummaryStatistic.StatisticDimension.FEATURE_GROUP )
                                                  .setHistogramBins( 10 )
                                                  .build();

        Set<SummaryStatistic> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( first );
        summaryStatistics.add( second );

        GeometryTuple firstTuple = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "fooOne" ) )
                                                .setRight( Geometry.newBuilder()
                                                                   .setName( "fooOne" ) )
                                                .build();

        GeometryTuple secondTuple = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder()
                                                                   .setName( "fooTwo" ) )
                                                 .setRight( Geometry.newBuilder()
                                                                    .setName( "fooTwo" ) )
                                                 .build();

        GeometryGroup firstGroup = GeometryGroup.newBuilder()
                                                .addGeometryTuples( firstTuple )
                                                .addGeometryTuples( secondTuple )
                                                .setRegionName( "fooRegion" )
                                                .build();

        GeometryTuple thirdTuple = GeometryTuple.newBuilder()
                                                .setLeft( Geometry.newBuilder()
                                                                  .setName( "barOne" ) )
                                                .setRight( Geometry.newBuilder()
                                                                   .setName( "barOne" ) )
                                                .build();

        GeometryTuple fourthTuple = GeometryTuple.newBuilder()
                                                 .setLeft( Geometry.newBuilder()
                                                                   .setName( "barTwo" ) )
                                                 .setRight( Geometry.newBuilder()
                                                                    .setName( "barTwo" ) )
                                                 .build();

        GeometryGroup secondGroup = GeometryGroup.newBuilder()
                                                 .addGeometryTuples( thirdTuple )
                                                 .addGeometryTuples( fourthTuple )
                                                 .setRegionName( "barRegion" )
                                                 .build();

        Set<GeometryGroup> geometryGroups = Set.of( firstGroup, secondGroup );

        FeatureGroups featureGroups = FeatureGroupsBuilder.builder()
                                                          .geometryGroups( geometryGroups )
                                                          .build();

        EvaluationDeclaration evaluation =
                EvaluationDeclarationBuilder.builder()
                                            .leadTimes( leadTimeInterval )
                                            .leadTimePools( Collections.singleton( leadTimePools ) )
                                            .featureGroups( featureGroups )
                                            .summaryStatistics( summaryStatistics )
                                            .build();

        Map<String, List<SummaryStatisticsCalculator>> calculators =
                EvaluationUtilities.getSummaryStatisticsCalculators( evaluation, 0, false );

        // Eight filters
        assertEquals( 4, calculators.values()
                                    .stream()
                                    .mapToInt( List::size )
                                    .sum() );
    }

    @Test
    void testHasEventThresholdsThatVaryAcrossFeaturesReturnsTrue()
    {
        Map<FeatureTuple, Set<ThresholdOuter>> thresholds = new HashMap<>();

        Threshold one = Threshold.newBuilder()
                                 .setName( "flood" )
                                 .setLeftThresholdValue( 23.0 )
                                 .build();

        Threshold two = Threshold.newBuilder()
                                 .setName( "flood" )
                                 .setLeftThresholdValue( 22.0 )
                                 .build();

        Geometry oneFeature = Geometry.newBuilder()
                                      .setName( "foo" )
                                      .build();
        Geometry twoFeature = Geometry.newBuilder()
                                      .setName( "bar" )
                                      .build();

        FeatureTuple oneTuple = FeatureTuple.of( GeometryTuple.newBuilder()
                                                              .setLeft( oneFeature )
                                                              .setRight( oneFeature )
                                                              .build() );

        FeatureTuple twoTuple = FeatureTuple.of( GeometryTuple.newBuilder()
                                                              .setLeft( twoFeature )
                                                              .setRight( twoFeature )
                                                              .build() );

        thresholds.put( oneTuple, Set.of( ThresholdOuter.of( one ) ) );
        thresholds.put( twoTuple, Set.of( ThresholdOuter.of( two ) ) );

        MetricsAndThresholds metricsAndThresholds = new MetricsAndThresholds( Set.of( MetricConstants.MEAN_ERROR ),
                                                                              thresholds,
                                                                              0,
                                                                              Pool.EnsembleAverageType.MEAN );

        Set<MetricsAndThresholds> thresholdSet = Set.of( metricsAndThresholds );
        assertTrue( EvaluationUtilities.hasEventThresholdsThatVaryAcrossFeatures( thresholdSet ) );
    }

    @Test
    void testHasEventThresholdsThatVaryAcrossFeaturesReturnsFalse()
    {
        Map<FeatureTuple, Set<ThresholdOuter>> thresholds = new HashMap<>();

        Threshold one = Threshold.newBuilder()
                                 .setName( "flood" )
                                 .setLeftThresholdValue( 23.0 )
                                 .build();

        Threshold two = Threshold.newBuilder()
                                 .setName( "flood" )
                                 .setLeftThresholdValue( 23.0 )
                                 .build();

        Geometry oneFeature = Geometry.newBuilder()
                                      .setName( "foo" )
                                      .build();
        Geometry twoFeature = Geometry.newBuilder()
                                      .setName( "bar" )
                                      .build();

        FeatureTuple oneTuple = FeatureTuple.of( GeometryTuple.newBuilder()
                                                              .setLeft( oneFeature )
                                                              .setRight( oneFeature )
                                                              .build() );

        FeatureTuple twoTuple = FeatureTuple.of( GeometryTuple.newBuilder()
                                                              .setLeft( twoFeature )
                                                              .setRight( twoFeature )
                                                              .build() );

        thresholds.put( oneTuple, Set.of( ThresholdOuter.of( one ) ) );
        thresholds.put( twoTuple, Set.of( ThresholdOuter.of( two ) ) );

        MetricsAndThresholds metricsAndThresholds = new MetricsAndThresholds( Set.of( MetricConstants.MEAN_ERROR ),
                                                                              thresholds,
                                                                              0,
                                                                              Pool.EnsembleAverageType.MEAN );

        Set<MetricsAndThresholds> thresholdSet = Set.of( metricsAndThresholds );

        Map<FeatureTuple, Set<ThresholdOuter>> thresholdsTwo = new HashMap<>();
        thresholdsTwo.put( oneTuple, Set.of( ThresholdOuter.of( one ) ) );

        MetricsAndThresholds metricsAndThresholdsTwo = new MetricsAndThresholds( Set.of( MetricConstants.MEAN_ERROR ),
                                                                                 thresholdsTwo,
                                                                                 0,
                                                                                 Pool.EnsembleAverageType.MEAN );

        Set<MetricsAndThresholds> thresholdSetTwo = Set.of( metricsAndThresholdsTwo );

        assertAll( () -> assertFalse( EvaluationUtilities.hasEventThresholdsThatVaryAcrossFeatures( thresholdSet ) ),
                   () -> assertFalse( EvaluationUtilities.hasEventThresholdsThatVaryAcrossFeatures( thresholdSetTwo ) ) );
    }
}