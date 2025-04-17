package wres.metrics;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;

import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.datamodel.pools.MeasurementUnit;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.SummaryStatistic;

/**
 * Tests the {@link SummaryStatisticsCalculator}.
 *
 * @author James Brown
 */

class SummaryStatisticsCalculatorTest
{
    @Test
    void testGetQuantilesForDoubleScore()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0.5 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.1 );

        SummaryStatistic q2 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        SummaryStatistic q3 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.9 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );
        ScalarSummaryStatisticFunction q2f = FunctionFactory.ofScalarSummaryStatistic( q2 );
        ScalarSummaryStatisticFunction q3f = FunctionFactory.ofScalarSummaryStatistic( q3 );

        Set<ScalarSummaryStatisticFunction> quantiles = new LinkedHashSet<>();
        quantiles.add( q1f );
        quantiles.add( q2f );
        quantiles.add( q3f );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 1.1 );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder()
                                                          .setSummaryStatistic( q2 );
        expectedSecondBuilder.getScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( 5.5 );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q3 );
        expectedThirdBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 9.9 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(),
                                             expectedSecondBuilder.build(),
                                             expectedThirdBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetQuantilesForDiagram()
    {
        DiagramMetric.DiagramMetricComponent pod =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.PROBABILITY_OF_DETECTION )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( 1 )
                                                    .setUnits( "PROBABILITY" )
                                                    .build();

        DiagramMetric.DiagramMetricComponent pofd =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.PROBABILITY_OF_FALSE_DETECTION )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( 1 )
                                                    .setUnits( "PROBABILITY" )
                                                    .build();

        DiagramMetric roc = DiagramMetric.newBuilder()
                                         .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                         .setHasDiagonal( true )
                                         .build();

        DiagramStatistic.DiagramStatisticComponent podStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( pod )
                                                          .addAllValues( List.of( 1.0, 1.0, 1.0 ) )
                                                          .build();

        DiagramStatistic.DiagramStatisticComponent pofdStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( pofd )
                                                          .addAllValues( List.of( 1.0, 1.0, 1.0 ) )
                                                          .build();

        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .addStatistics( podStatistic )
                                                      .addStatistics( pofdStatistic )
                                                      .setMetric( roc )
                                                      .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addDiagrams( rocDiagram )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.1 );

        SummaryStatistic q2 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        SummaryStatistic q3 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.9 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );
        ScalarSummaryStatisticFunction q2f = FunctionFactory.ofScalarSummaryStatistic( q2 );
        ScalarSummaryStatisticFunction q3f = FunctionFactory.ofScalarSummaryStatistic( q3 );

        Set<ScalarSummaryStatisticFunction> quantiles = new LinkedHashSet<>();
        quantiles.add( q1f );
        quantiles.add( q2f );
        quantiles.add( q3f );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();

            List<Double> first = List.of( ( double ) i, ( double ) i, ( double ) i );
            List<Double> second = List.of( i + 5.0, i + 5.0, i + 5.0 );

            // Set the new diagram
            next.getDiagramsBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .clearValues()
                .addAllValues( first );
            next.getDiagramsBuilder( 0 )
                .getStatisticsBuilder( 1 )
                .clearValues()
                .addAllValues( second );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .clearValues()
                            .addAllValues( List.of( 1.1, 1.1, 1.1 ) );
        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .clearValues()
                            .addAllValues( List.of( 6.1, 6.1, 6.1 ) );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder()
                                                          .setSummaryStatistic( q2 );
        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .clearValues()
                             .addAllValues( List.of( 5.5, 5.5, 5.5 ) );
        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .clearValues()
                             .addAllValues( List.of( 10.5, 10.5, 10.5 ) );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q3 );
        expectedThirdBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .clearValues()
                            .addAllValues( List.of( 9.9, 9.9, 9.9 ) );
        expectedThirdBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .clearValues()
                            .addAllValues( List.of( 14.9, 14.9, 14.9 ) );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(),
                                             expectedSecondBuilder.build(),
                                             expectedThirdBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetQuantilesForDurationScore()
    {
        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();

        DurationScoreMetric.DurationScoreMetricComponent main =
                DurationScoreMetric.DurationScoreMetricComponent.newBuilder()
                                                                .setName( MetricName.MEAN )
                                                                .build();

        DurationScoreStatistic.DurationScoreStatisticComponent
                component = DurationScoreStatistic.DurationScoreStatisticComponent.newBuilder()
                                                                                  .setMetric( main )
                                                                                  .setValue( Duration.newBuilder()
                                                                                                     .setSeconds( 50 ) )
                                                                                  .build();

        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .addStatistics( component )
                                                             .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addDurationScores( score )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.1 );

        SummaryStatistic q2 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        SummaryStatistic q3 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.9 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );
        ScalarSummaryStatisticFunction q2f = FunctionFactory.ofScalarSummaryStatistic( q2 );
        ScalarSummaryStatisticFunction q3f = FunctionFactory.ofScalarSummaryStatistic( q3 );

        Set<ScalarSummaryStatisticFunction> quantiles = new LinkedHashSet<>();
        quantiles.add( q1f );
        quantiles.add( q2f );
        quantiles.add( q3f );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getDurationScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( Duration.newBuilder()
                                   .setSeconds( i * 60 * 60 ) );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getDurationScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( Duration.newBuilder()
                                               .setSeconds( 3960 ) );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder()
                                                          .setSummaryStatistic( q2 );
        expectedSecondBuilder.getDurationScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( Duration.newBuilder()
                                                .setSeconds( 19800 ) );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q3 );
        expectedThirdBuilder.getDurationScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( Duration.newBuilder()
                                               .setSeconds( 35640 ) );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(),
                                             expectedSecondBuilder.build(),
                                             expectedThirdBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetQuantilesForDurationDiagram()
    {
        DurationDiagramMetric ttpe =
                DurationDiagramMetric.newBuilder().setName( MetricName.TIME_TO_PEAK_ERROR )
                                     .build();

        Timestamp first = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T00:00:00Z" ) );
        Timestamp second = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T01:00:00Z" ) );
        Timestamp third = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T02:00:00Z" ) );
        Duration one = MessageUtilities.getDuration( java.time.Duration.ofHours( 1 ) );
        Duration two = MessageUtilities.getDuration( java.time.Duration.ofHours( 2 ) );
        Duration three = MessageUtilities.getDuration( java.time.Duration.ofHours( 3 ) );

        List<DurationDiagramStatistic.PairOfInstantAndDuration> errorPairs =
                List.of( DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( first )
                                                                          .setDuration( one )
                                                                          .build(),
                         DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( second )
                                                                          .setDuration( two )
                                                                          .build(),
                         DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( third )
                                                                          .setDuration( three )
                                                                          .build() );

        DurationDiagramStatistic ttpes =
                DurationDiagramStatistic.newBuilder()
                                        .setMetric( ttpe )
                                        .addAllStatistics( errorPairs )
                                        .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addDurationDiagrams( ttpes )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.1 );

        SummaryStatistic q2 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        SummaryStatistic q3 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.9 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );
        ScalarSummaryStatisticFunction q2f = FunctionFactory.ofScalarSummaryStatistic( q2 );
        ScalarSummaryStatisticFunction q3f = FunctionFactory.ofScalarSummaryStatistic( q3 );

        Set<ScalarSummaryStatisticFunction> quantiles = new LinkedHashSet<>();
        quantiles.add( q1f );
        quantiles.add( q2f );
        quantiles.add( q3f );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();

            Duration firstDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i ) );
            Duration secondDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i + 1 ) );
            Duration thirdDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i + 2 ) );

            // Set the new pairs
            next.getDurationDiagramsBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setDuration( firstDuration );
            next.getDurationDiagramsBuilder( 0 )
                .getStatisticsBuilder( 1 )
                .setDuration( secondDuration );
            next.getDurationDiagramsBuilder( 0 )
                .getStatisticsBuilder( 2 )
                .setDuration( thirdDuration );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Duration firstExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 3960 ) );
        Duration secondExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 7560 ) );
        Duration thirdExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 11160 ) );
        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setDuration( firstExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .setDuration( secondExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 2 )
                            .setDuration( thirdExpected );

        Duration fourthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 19800 ) );
        Duration fifthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 23400 ) );
        Duration sixthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 27000 ) );
        Statistics.Builder expectedSecondBuilder = nominal.toBuilder()
                                                          .setSummaryStatistic( q2 );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setDuration( fourthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .setDuration( fifthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 2 )
                             .setDuration( sixthExpected );

        Duration seventhExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 35640 ) );
        Duration eighthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 39240 ) );
        Duration ninthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 42840 ) );
        Statistics.Builder expectedThirdBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q3 );
        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setDuration( seventhExpected );
        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .setDuration( eighthExpected );
        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 2 )
                            .setDuration( ninthExpected );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(),
                                             expectedSecondBuilder.build(),
                                             expectedThirdBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetQuantilesForDoubleScoreWhenAddingNominalStatistics()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );

        Set<ScalarSummaryStatisticFunction> quantiles = Set.of( q1f );
        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        // Add nominal
        calculator.test( nominal );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 5.0 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build() );

        assertEquals( expected, actual );
    }

    @RepeatedTest( 100 )
    void testMultithreadedAddIsDeterministic() throws InterruptedException, ExecutionException
    {
        // Create the nominal double score
        DoubleScoreMetric doubleScoreMetric = DoubleScoreMetric.newBuilder()
                                                               .setName( MetricName.BIAS_FRACTION )
                                                               .build();

        DoubleScoreMetric.DoubleScoreMetricComponent doubleScoreMain =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                doubleScoreComponent = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                         .setMetric( doubleScoreMain )
                                                                                         .setValue( 0.5 )
                                                                                         .build();

        DoubleScoreStatistic doubleScore = DoubleScoreStatistic.newBuilder()
                                                               .setMetric( doubleScoreMetric )
                                                               .addStatistics( doubleScoreComponent )
                                                               .build();

        // Create the nominal duration score
        DurationScoreMetric durationScoreMetric = DurationScoreMetric.newBuilder()
                                                                     .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                                     .build();

        DurationScoreMetric.DurationScoreMetricComponent durationScoreMain =
                DurationScoreMetric.DurationScoreMetricComponent.newBuilder()
                                                                .setName( MetricName.MEAN )
                                                                .build();

        DurationScoreStatistic.DurationScoreStatisticComponent
                durationScoreComponent = DurationScoreStatistic.DurationScoreStatisticComponent.newBuilder()
                                                                                               .setMetric(
                                                                                                       durationScoreMain )
                                                                                               .setValue( Duration.newBuilder()
                                                                                                                  .setSeconds(
                                                                                                                          50 ) )
                                                                                               .build();

        DurationScoreStatistic durationScore = DurationScoreStatistic.newBuilder()
                                                                     .setMetric( durationScoreMetric )
                                                                     .addStatistics( durationScoreComponent )
                                                                     .build();

        // Create the nominal diagram
        DiagramMetric.DiagramMetricComponent pod =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.PROBABILITY_OF_DETECTION )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( 1 )
                                                    .setUnits( "PROBABILITY" )
                                                    .build();

        DiagramMetric.DiagramMetricComponent pofd =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.PROBABILITY_OF_FALSE_DETECTION )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( 1 )
                                                    .setUnits( "PROBABILITY" )
                                                    .build();

        DiagramMetric roc = DiagramMetric.newBuilder()
                                         .setName( MetricName.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM )
                                         .setHasDiagonal( true )
                                         .build();

        DiagramStatistic.DiagramStatisticComponent podStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( pod )
                                                          .addAllValues( List.of( 1.0, 1.0, 1.0 ) )
                                                          .build();

        DiagramStatistic.DiagramStatisticComponent pofdStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( pofd )
                                                          .addAllValues( List.of( 1.0, 1.0, 1.0 ) )
                                                          .build();

        DiagramStatistic rocDiagram = DiagramStatistic.newBuilder()
                                                      .addStatistics( podStatistic )
                                                      .addStatistics( pofdStatistic )
                                                      .setMetric( roc )
                                                      .build();

        // Create the nominal duration diagram
        DurationDiagramMetric ttpe =
                DurationDiagramMetric.newBuilder().setName( MetricName.TIME_TO_PEAK_ERROR )
                                     .build();

        Timestamp one = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T00:00:00Z" ) );
        Timestamp two = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T01:00:00Z" ) );
        Timestamp three = MessageUtilities.getTimestamp( Instant.parse( "2023-03-03T02:00:00Z" ) );
        Duration a = MessageUtilities.getDuration( java.time.Duration.ofHours( 1 ) );
        Duration b = MessageUtilities.getDuration( java.time.Duration.ofHours( 2 ) );
        Duration c = MessageUtilities.getDuration( java.time.Duration.ofHours( 3 ) );

        List<DurationDiagramStatistic.PairOfInstantAndDuration> errorPairs =
                List.of( DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( one )
                                                                          .setDuration( a )
                                                                          .build(),
                         DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( two )
                                                                          .setDuration( b )
                                                                          .build(),
                         DurationDiagramStatistic.PairOfInstantAndDuration.newBuilder()
                                                                          .setTime( three )
                                                                          .setDuration( c )
                                                                          .build() );

        DurationDiagramStatistic ttpes =
                DurationDiagramStatistic.newBuilder()
                                        .setMetric( ttpe )
                                        .addAllStatistics( errorPairs )
                                        .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addDiagrams( rocDiagram )
                                       .addScores( doubleScore )
                                       .addDurationScores( durationScore )
                                       .addDurationDiagrams( ttpes )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.1 );

        SummaryStatistic q2 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.5 );

        SummaryStatistic q3 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.QUANTILE,
                                                                    Set.of( SummaryStatistic.StatisticDimension.RESAMPLED ),
                                                                    0.9 );

        ScalarSummaryStatisticFunction q1f = FunctionFactory.ofScalarSummaryStatistic( q1 );
        ScalarSummaryStatisticFunction q2f = FunctionFactory.ofScalarSummaryStatistic( q2 );
        ScalarSummaryStatisticFunction q3f = FunctionFactory.ofScalarSummaryStatistic( q3 );

        Set<ScalarSummaryStatisticFunction> quantiles = new LinkedHashSet<>();
        quantiles.add( q1f );
        quantiles.add( q2f );
        quantiles.add( q3f );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( quantiles, Set.of(), Set.of(), null, ( x, y ) -> x, null );

        ExecutorService executor = Executors.newFixedThreadPool( 5 );

        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 1; i < 11; i++ )
        {
            // Create the double score
            Statistics.Builder nextBuilder = nominal.toBuilder();
            // Set the new score
            nextBuilder.getScoresBuilder( 0 )
                       .getStatisticsBuilder( 0 )
                       .setValue( i );

            // Create the diagram
            List<Double> first = List.of( ( double ) i, ( double ) i, ( double ) i );
            List<Double> second = List.of( i + 5.0, i + 5.0, i + 5.0 );
            // Set the new diagram
            nextBuilder.getDiagramsBuilder( 0 )
                       .getStatisticsBuilder( 0 )
                       .clearValues()
                       .addAllValues( first );
            nextBuilder.getDiagramsBuilder( 0 )
                       .getStatisticsBuilder( 1 )
                       .clearValues()
                       .addAllValues( second );

            // Create the duration score
            nextBuilder.getDurationScoresBuilder( 0 )
                       .getStatisticsBuilder( 0 )
                       .setValue( Duration.newBuilder()
                                          .setSeconds( i * 60 * 60 ) );

            // Create the duration diagram
            Duration firstDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i ) );
            Duration secondDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i + 1 ) );
            Duration thirdDuration = MessageUtilities.getDuration( java.time.Duration.ofHours( i + 2 ) );

            // Set the new pairs
            nextBuilder.getDurationDiagramsBuilder( 0 )
                       .getStatisticsBuilder( 0 )
                       .setDuration( firstDuration );
            nextBuilder.getDurationDiagramsBuilder( 0 )
                       .getStatisticsBuilder( 1 )
                       .setDuration( secondDuration );
            nextBuilder.getDurationDiagramsBuilder( 0 )
                       .getStatisticsBuilder( 2 )
                       .setDuration( thirdDuration );

            // Multithreaded add
            Runnable statisticAddTask = () -> calculator.test( nextBuilder.build() );
            Future<?> statisticAddFuture = executor.submit( statisticAddTask );
            futures.add( statisticAddFuture );
        }

        // Execute
        for ( Future<?> next : futures )
        {
            next.get();
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q1 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 1.1 );

        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .clearValues()
                            .addAllValues( List.of( 1.1, 1.1, 1.1 ) );
        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .clearValues()
                            .addAllValues( List.of( 6.1, 6.1, 6.1 ) );

        expectedFirstBuilder.getDurationScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( Duration.newBuilder()
                                               .setSeconds( 3960 ) );

        Duration firstExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 3960 ) );
        Duration secondExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 7560 ) );
        Duration thirdExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 11160 ) );

        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setDuration( firstExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .setDuration( secondExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 2 )
                            .setDuration( thirdExpected );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder()
                                                          .setSummaryStatistic( q2 );
        expectedSecondBuilder.getScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( 5.5 );

        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .clearValues()
                             .addAllValues( List.of( 5.5, 5.5, 5.5 ) );
        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .clearValues()
                             .addAllValues( List.of( 10.5, 10.5, 10.5 ) );

        expectedSecondBuilder.getDurationScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( Duration.newBuilder()
                                                .setSeconds( 19800 ) );

        Duration fourthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 19800 ) );
        Duration fifthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 23400 ) );
        Duration sixthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 27000 ) );

        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setDuration( fourthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .setDuration( fifthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 2 )
                             .setDuration( sixthExpected );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( q3 );
        expectedThirdBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 9.9 );

        expectedThirdBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .clearValues()
                            .addAllValues( List.of( 9.9, 9.9, 9.9 ) );
        expectedThirdBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .clearValues()
                            .addAllValues( List.of( 14.9, 14.9, 14.9 ) );

        expectedThirdBuilder.getDurationScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( Duration.newBuilder()
                                               .setSeconds( 35640 ) );

        Duration seventhExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 35640 ) );
        Duration eighthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 39240 ) );
        Duration ninthExpected = MessageUtilities.getDuration( java.time.Duration.ofSeconds( 42840 ) );

        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setDuration( seventhExpected );
        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .setDuration( eighthExpected );
        expectedThirdBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 2 )
                            .setDuration( ninthExpected );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(),
                                             expectedSecondBuilder.build(),
                                             expectedThirdBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMeanOverFeaturesForDoubleScore()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0.5 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic mean = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.MEAN,
                                                                      Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                      null );

        ScalarSummaryStatisticFunction meanFunction = FunctionFactory.ofScalarSummaryStatistic( mean );

        Set<ScalarSummaryStatisticFunction> summaryStatistics = Set.of( meanFunction );
        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( summaryStatistics, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( mean );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 5.5 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetStandardDeviationOverFeaturesWithFilterForDoubleScore()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0.5 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic sd = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION,
                                                                    Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                    null );

        ScalarSummaryStatisticFunction sdFunction = FunctionFactory.ofScalarSummaryStatistic( sd );

        Set<ScalarSummaryStatisticFunction> summaryStatistics = Set.of( sdFunction );

        // Create a filter to eliminate a score of "5"
        Predicate<Statistics> include = s -> !FunctionFactory.doubleEquals()
                                                             .test( s.getScores( 0 )
                                                                     .getStatistics( 0 )
                                                                     .getValue(), 5.0 );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( summaryStatistics, Set.of(), Set.of(), include, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .setSummaryStatistic( sd );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 3.2058973436118907 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testAddDifferentScoreAcrossTwoUpdates()
    {
        DoubleScoreMetric biasMetric = DoubleScoreMetric.newBuilder()
                                                        .setName( MetricName.BIAS_FRACTION )
                                                        .build();

        DoubleScoreMetric.DoubleScoreMetricComponent biasMain =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                biasComponent = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                  .setMetric( biasMain )
                                                                                  .setValue( 0.5 )
                                                                                  .build();

        DoubleScoreStatistic biasScore = DoubleScoreStatistic.newBuilder()
                                                             .setMetric( biasMetric )
                                                             .addStatistics( biasComponent )
                                                             .build();

        Statistics one = Statistics.newBuilder()
                                   .addScores( biasScore )
                                   .build();

        DoubleScoreMetric correlationMetric = DoubleScoreMetric.newBuilder()
                                                               .setName( MetricName.PEARSON_CORRELATION_COEFFICIENT )
                                                               .build();

        DoubleScoreMetric.DoubleScoreMetricComponent correlationMain =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( -1 )
                                                            .setMaximum( 1 )
                                                            .setOptimum( 1 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                correlationComponent = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                                         .setMetric( correlationMain )
                                                                                         .setValue( 0.8 )
                                                                                         .build();

        DoubleScoreStatistic correlationScore = DoubleScoreStatistic.newBuilder()
                                                                    .setMetric( correlationMetric )
                                                                    .addStatistics( correlationComponent )
                                                                    .build();

        Statistics two = Statistics.newBuilder()
                                   .addScores( correlationScore )
                                   .build();

        SummaryStatistic mean = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.MEAN,
                                                                      Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                      null );

        ScalarSummaryStatisticFunction meanFunction = FunctionFactory.ofScalarSummaryStatistic( mean );

        Set<ScalarSummaryStatisticFunction> summaryStatistics = Set.of( meanFunction );

        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( summaryStatistics, Set.of(), Set.of(), null, ( a, b ) -> a, null );

        // Accept the two statistics
        calculator.test( one );
        calculator.test( two );

        // Cannot guarantee order of statistics, so compare sets
        List<Statistics> actual = calculator.get();

        Statistics expectedOne = Statistics.newBuilder()
                                           .setSummaryStatistic( mean )
                                           .addScores( biasScore )
                                           .addScores( correlationScore )
                                           .build();

        List<Statistics> expected = List.of( expectedOne );

        assertEquals( expected, actual );
    }

    @Test
    void testGetMeanAndStandardDeviationOverFeaturesForDoubleScore()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0.5 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic mean = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.MEAN,
                                                                      Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                      null );

        ScalarSummaryStatisticFunction meanFunction = FunctionFactory.ofScalarSummaryStatistic( mean );

        SummaryStatistic sd = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.STANDARD_DEVIATION,
                                                                    Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                    null );

        ScalarSummaryStatisticFunction sdFunction = FunctionFactory.ofScalarSummaryStatistic( sd );

        Set<ScalarSummaryStatisticFunction> summaryStatistics = new LinkedHashSet<>();
        summaryStatistics.add( meanFunction );
        summaryStatistics.add( sdFunction );

        GeometryGroup group = GeometryGroup.newBuilder()
                                           .setRegionName( "ALL FEATURES" )
                                           .build();

        BinaryOperator<Statistics> transformer = ( x, y ) -> x.toBuilder()
                                                              .setPool( x.getPool()
                                                                         .toBuilder()
                                                                         .setGeometryGroup( group ) )
                                                              .build();
        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( summaryStatistics, Set.of(), Set.of(), null, transformer, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        // Root builder with adapted geometry information
        Statistics root = nominal.toBuilder()
                                 .setPool( nominal.getPool()
                                                  .toBuilder()
                                                  .setGeometryGroup( group ) )
                                 .build();

        Statistics.Builder expectedFirstBuilder = root.toBuilder()
                                                      .setSummaryStatistic( mean );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 5.5 );

        Statistics.Builder expectedSecondBuilder = root.toBuilder()
                                                       .setSummaryStatistic( sd );
        expectedSecondBuilder.getScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( 3.0276503540974917 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build(), expectedSecondBuilder.build() );

        assertEquals( expected, actual );
    }

    @Test
    void testGetBoxplotsForDoubleScore()
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( MetricName.MAIN )
                                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                            .build();

        DoubleScoreStatistic.DoubleScoreStatisticComponent
                component = DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                              .setMetric( main )
                                                                              .setValue( 0.5 )
                                                                              .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( metric )
                                                         .addStatistics( component )
                                                         .build();

        Statistics nominal = Statistics.newBuilder()
                                       .addScores( score )
                                       .build();

        SummaryStatistic q1 = MessageUtilities.getSummaryStatistic( SummaryStatistic.StatisticName.BOX_PLOT,
                                                                    Set.of( SummaryStatistic.StatisticDimension.FEATURES ),
                                                                    null );

        BoxplotSummaryStatisticFunction q1f = FunctionFactory.ofBoxplotSummaryStatistic( q1 );

        Set<BoxplotSummaryStatisticFunction> boxplot = Set.of( q1f );
        SummaryStatisticsCalculator calculator =
                SummaryStatisticsCalculator.of( Set.of(), Set.of(), boxplot, null, ( a, b ) -> a, null );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.test( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder()
                                                         .clearScores()
                                                         .setSummaryStatistic( q1 );
        BoxplotStatistic.Box box = BoxplotStatistic.Box.newBuilder()
                                                       .addAllQuantiles( List.of( 1.0, 2.75, 5.5, 8.25, 10.0 ) )
                                                       .build();
        BoxplotMetric boxplotMetric = BoxplotMetric.newBuilder()
                                                   .setName( MetricName.BOX_PLOT )
                                                   .setStatisticName( MetricName.BIAS_FRACTION )
                                                   .setStatisticComponentName( MetricName.MAIN )
                                                   .setUnits( "DIMENSIONLESS" )
                                                   .setMinimum( Double.NEGATIVE_INFINITY )
                                                   .setMaximum( Double.POSITIVE_INFINITY )
                                                   .setQuantileValueType( BoxplotMetric.QuantileValueType.STATISTIC )
                                                   .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                                   .build();
        BoxplotStatistic boxStatistic = BoxplotStatistic.newBuilder()
                                                        .setMetric( boxplotMetric )
                                                        .addStatistics( box )
                                                        .build();
        expectedFirstBuilder.addOneBoxPerPool( boxStatistic );

        List<Statistics> expected = List.of( expectedFirstBuilder.build() );

        assertEquals( expected, actual );
    }
}
