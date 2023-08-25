package wres.datamodel.bootstrap;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import wres.datamodel.pools.MeasurementUnit;
import wres.statistics.MessageFactory;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramMetric;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;

/**
 * Tests the {@link QuantileCalculator}.
 * @author James Brown
 */
class QuantileCalculatorTest
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
                                                            .setName( DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName.MAIN )
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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.1, 0.5, 0.9 ) ),
                                                               false );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.add( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.1 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 1.1 );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder();
        expectedSecondBuilder.setSampleQuantile( 0.5 );
        expectedSecondBuilder.getScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( 5.5 );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder();
        expectedThirdBuilder.setSampleQuantile( 0.9 );
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
                                                    .setName( DiagramMetric.DiagramMetricComponent.DiagramComponentName.PROBABILITY_OF_DETECTION )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( 1 )
                                                    .setUnits( "PROBABILITY" )
                                                    .build();

        DiagramMetric.DiagramMetricComponent pofd =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( DiagramMetric.DiagramMetricComponent.DiagramComponentName.PROBABILITY_OF_FALSE_DETECTION )
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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.1, 0.5, 0.9 ) ),
                                                               false );

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
            calculator.add( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.1 );
        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .clearValues()
                            .addAllValues( List.of( 1.1, 1.1, 1.1 ) );
        expectedFirstBuilder.getDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .clearValues()
                            .addAllValues( List.of( 6.1, 6.1, 6.1 ) );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder();
        expectedSecondBuilder.setSampleQuantile( 0.5 );
        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .clearValues()
                             .addAllValues( List.of( 5.5, 5.5, 5.5 ) );
        expectedSecondBuilder.getDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .clearValues()
                             .addAllValues( List.of( 10.5, 10.5, 10.5 ) );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder();
        expectedThirdBuilder.setSampleQuantile( 0.9 );
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
                                                                .setName( DurationScoreMetric.DurationScoreMetricComponent.ComponentName.MEAN )
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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.1, 0.5, 0.9 ) ),
                                                               false );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getDurationScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( Duration.newBuilder()
                                   .setSeconds( i * 60 * 60 ) );
            calculator.add( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.1 );
        expectedFirstBuilder.getDurationScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( Duration.newBuilder()
                                               .setSeconds( 3960 ) );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder();
        expectedSecondBuilder.setSampleQuantile( 0.5 );
        expectedSecondBuilder.getDurationScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( Duration.newBuilder()
                                                .setSeconds( 19800 ) );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder();
        expectedThirdBuilder.setSampleQuantile( 0.9 );
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

        Timestamp first = MessageFactory.parse( Instant.parse( "2023-03-03T00:00:00Z" ) );
        Timestamp second = MessageFactory.parse( Instant.parse( "2023-03-03T01:00:00Z" ) );
        Timestamp third = MessageFactory.parse( Instant.parse( "2023-03-03T02:00:00Z" ) );
        Duration one = MessageFactory.parse( java.time.Duration.ofHours( 1 ) );
        Duration two = MessageFactory.parse( java.time.Duration.ofHours( 2 ) );
        Duration three = MessageFactory.parse( java.time.Duration.ofHours( 3 ) );

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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.1, 0.5, 0.9 ) ),
                                                               false );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();

            Duration firstDuration = MessageFactory.parse( java.time.Duration.ofHours( i ) );
            Duration secondDuration = MessageFactory.parse( java.time.Duration.ofHours( i + 1 ) );
            Duration thirdDuration = MessageFactory.parse( java.time.Duration.ofHours( i + 2 ) );

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
            calculator.add( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Duration firstExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 3960 ) );
        Duration secondExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 7560 ) );
        Duration thirdExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 11160 ) );
        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.1 );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setDuration( firstExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 1 )
                            .setDuration( secondExpected );
        expectedFirstBuilder.getDurationDiagramsBuilder( 0 )
                            .getStatisticsBuilder( 2 )
                            .setDuration( thirdExpected );

        Duration fourthExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 19800 ) );
        Duration fifthExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 23400 ) );
        Duration sixthExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 27000 ) );
        Statistics.Builder expectedSecondBuilder = nominal.toBuilder();
        expectedSecondBuilder.setSampleQuantile( 0.5 );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setDuration( fourthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 1 )
                             .setDuration( fifthExpected );
        expectedSecondBuilder.getDurationDiagramsBuilder( 0 )
                             .getStatisticsBuilder( 2 )
                             .setDuration( sixthExpected );

        Duration seventhExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 35640 ) );
        Duration eighthExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 39240 ) );
        Duration ninthExpected = MessageFactory.parse( java.time.Duration.ofSeconds( 42840 ) );
        Statistics.Builder expectedThirdBuilder = nominal.toBuilder();
        expectedThirdBuilder.setSampleQuantile( 0.9 );
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

    @RepeatedTest( 100 )
    void testMultithreadedAddIsDeterministic() throws InterruptedException, ExecutionException
    {
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.BIAS_FRACTION )
                                                    .build();

        DoubleScoreMetric.DoubleScoreMetricComponent main =
                DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                                            .setMaximum( Double.POSITIVE_INFINITY )
                                                            .setOptimum( 0 )
                                                            .setName( DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName.MAIN )
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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.1, 0.5, 0.9 ) ),
                                                               false );

        ExecutorService executor = Executors.newFixedThreadPool( 5 );

        List<Future<?>> futures = new ArrayList<>();
        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );

            // Multithreaded add
            Runnable runnable = () -> calculator.add( next.build() );
            Future<?> future = executor.submit( runnable );
            futures.add( future );
        }

        // Execute
        for ( Future<?> next : futures )
        {
            next.get();
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.1 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 1.1 );

        Statistics.Builder expectedSecondBuilder = nominal.toBuilder();
        expectedSecondBuilder.setSampleQuantile( 0.5 );
        expectedSecondBuilder.getScoresBuilder( 0 )
                             .getStatisticsBuilder( 0 )
                             .setValue( 5.5 );

        Statistics.Builder expectedThirdBuilder = nominal.toBuilder();
        expectedThirdBuilder.setSampleQuantile( 0.9 );
        expectedThirdBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 9.9 );

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
                                                            .setName( DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName.MAIN )
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

        QuantileCalculator calculator = QuantileCalculator.of( nominal,
                                                               10,
                                                               new TreeSet<>( Set.of( 0.5 ) ),
                                                               true );

        for ( int i = 1; i < 11; i++ )
        {
            Statistics.Builder next = nominal.toBuilder();
            // Set the new score
            next.getScoresBuilder( 0 )
                .getStatisticsBuilder( 0 )
                .setValue( i );
            calculator.add( next.build() );
        }

        List<Statistics> actual = calculator.get();

        Statistics.Builder expectedFirstBuilder = nominal.toBuilder();
        expectedFirstBuilder.setSampleQuantile( 0.5 );
        expectedFirstBuilder.getScoresBuilder( 0 )
                            .getStatisticsBuilder( 0 )
                            .setValue( 5.0 );

        List<Statistics> expected = List.of( expectedFirstBuilder.build() );

        assertEquals( expected, actual );
    }
}
