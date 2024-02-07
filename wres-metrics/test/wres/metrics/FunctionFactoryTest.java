package wres.metrics;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.datamodel.units.Units;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.SummaryStatistic;

/**
 * Tests the {@link FunctionFactory}.
 *
 * @author James Brown
 */
class FunctionFactoryTest
{
    /** Tests for double equality. */
    private final BiPredicate<Double, Double> doubleTester = FunctionFactory.doubleEquals();

    /**
     * Tests the {@link FunctionFactory#error()}.
     */

    @Test
    void testError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.error().applyAsDouble( Pair.of( -1.0, 1.0 ) ), 2.0 ) );
    }

    @Test
    void testAbsError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.absError().applyAsDouble( Pair.of( -1.0, 1.0 ) ),
                                            2.0 ) );
    }

    @Test
    void testSquareError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.squareError().applyAsDouble( Pair.of( -5.0, 5.0 ) ),
                                            100.0 ) );
    }

    @Test
    void testSkill()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.skill().applyAsDouble( 1.0, 2.0 ), 0.5 ) );
    }

    @Test
    void testDoubleEquals()
    {
        assertTrue( this.doubleTester.test( 1.0000131, 1.0000131 ) );

        assertFalse( this.doubleTester.test( 13.13131, 13.13132 ) );

        assertTrue( this.doubleTester.test( Double.NaN, Double.NaN ) );

        assertTrue( this.doubleTester.test( Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY ) );

        assertTrue( this.doubleTester.test( Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY ) );

        assertFalse( this.doubleTester.test( Double.NaN, Double.NEGATIVE_INFINITY ) );

        assertFalse( this.doubleTester.test( 1.0, Double.NaN ) );

        assertFalse( this.doubleTester.test( Double.POSITIVE_INFINITY, 1.0 ) );

    }

    @Test
    void testFiniteOrMissing()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( 1.13 ), 1.13 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NEGATIVE_INFINITY ),
                                            MissingValues.DOUBLE ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.POSITIVE_INFINITY ),
                                            MissingValues.DOUBLE ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NaN ),
                                            MissingValues.DOUBLE ) );
    }

    @Test
    void testMean()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.mean()
                                                           .applyAsDouble( new double[] { 1.0, 2.0, 3.0 } ),
                                            2.0 ) );
    }

    @Test
    void testMedian()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.median()
                                                           .applyAsDouble( new double[] { 4.0, 7.0, 6.3, 5.1723 } ),
                                            5.73615 ) );
    }

    @Test
    void testMeanAbsolute()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.meanAbsolute()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            6.8328275 ) );
    }

    @Test
    void testMinimum()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.minimum()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            -2.9 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.minimum()
                                                           .applyAsDouble( new double[] { 4.3,
                                                                   Double.NEGATIVE_INFINITY } ),
                                            Double.NEGATIVE_INFINITY ) );
    }

    @Test
    void testMaximum()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.maximum()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            13.13131 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.maximum()
                                                           .applyAsDouble( new double[] { Double.POSITIVE_INFINITY,
                                                                   4.3,
                                                                   Double.NEGATIVE_INFINITY } ),
                                            Double.POSITIVE_INFINITY ) );
    }

    @Test
    void testStandardDeviation()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.standardDeviation()
                                                           .applyAsDouble( new double[] { 7, 9, 11, 13, 123.883 } ),
                                            50.97908922 ) );
    }

    @Test
    void testSampleSize()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.sampleSize()
                                                           .applyAsDouble( new double[] { 7, 9, 11, 13, 123.883 } ),
                                            5.0 ) );
    }

    @Test
    void testQuantile()
    {
        double[] unsorted = new double[] { 4.9, 1.5, 6.3, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2 };
        ToDoubleFunction<double[]> qFA = FunctionFactory.quantile( 7.0 / 11.0 );
        assertEquals( 1647.1818181818185, qFA.applyAsDouble( unsorted ), 7 );
    }

    @Test
    void testOfScalarSummaryStatistic()
    {
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.MEAN ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.SAMPLE_SIZE ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.MINIMUM ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.MAXIMUM ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.MEDIAN ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.MEAN_ABSOLUTE ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofScalarSummaryStatistic( MetricConstants.STANDARD_DEVIATION ) ) );
    }

    @Test
    void testOfScalarSummaryStatisticWithWrongInput()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class,
                                                           () -> FunctionFactory.ofScalarSummaryStatistic(
                                                                   MetricConstants.MAIN ) );

        assertEquals( "The statistic 'MAIN' is not a recognized statistic in this context.", exception.getMessage() );
    }

    @Test
    void testOfDurationFromUnivariateFunction()
    {
        ToDoubleFunction<double[]> mean = FunctionFactory.mean();

        Duration[] durations = new Duration[3];

        durations[0] = Duration.ofSeconds( 23 );
        durations[1] = Duration.ofSeconds( 27 );
        durations[2] = Duration.ofMillis( 38800 );

        Function<Duration[], Duration> meanDuration = FunctionFactory.ofDurationFromUnivariateFunction( mean );
        Duration actual = meanDuration.apply( durations );
        Duration expected = Duration.ofMillis( 29600 );

        assertEquals( expected, actual );
    }

    @Test
    void testHistogram()
    {
        SummaryStatistic parameters = SummaryStatistic.newBuilder()
                                                      .setHistogramBins( 5 )
                                                      .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                      .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                      .build();
        DiagramSummaryStatisticFunction histogram = FunctionFactory.histogram( parameters );

        double[] data = new double[] { 1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 6 };
        Map<SummaryStatisticComponentName, String> p =
                Map.of( SummaryStatisticComponentName.METRIC_NAME, "MEAN",
                        SummaryStatisticComponentName.METRIC_UNIT, "bar" );

        DiagramStatistic actual = histogram.apply( p, data );

        // Build the expectation
        DiagramMetric.DiagramMetricComponent domainMetric =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.BIN_UPPER_BOUND )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                    .setMinimum( Double.NEGATIVE_INFINITY )
                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                    .setUnits( "bar" )
                                                    .build();

        DiagramMetric.DiagramMetricComponent rangeMetric =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.COUNT )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                    .setUnits( Units.COUNT )
                                                    .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( domainMetric )
                                            .addComponents( rangeMetric )
                                            .setName( MetricName.HISTOGRAM )
                                            .setStatisticName( MetricName.MEAN )
                                            .setStatisticUnits( "bar" )
                                            .setStatisticMinimum( Double.NEGATIVE_INFINITY )
                                            .setStatisticMaximum( Double.POSITIVE_INFINITY )
                                            .setStatisticOptimum( Double.NaN )
                                            .build();

        DiagramStatistic.DiagramStatisticComponent domainStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( domainMetric )
                                                          .addAllValues( List.of( 2.0, 3.0, 4.0, 5.0, 6.0 ) )
                                                          .build();

        DiagramStatistic.DiagramStatisticComponent rangeStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( rangeMetric )
                                                          .addAllValues( List.of( 3.0, 3.0, 4.0, 5.0, 6.0 ) )
                                                          .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( domainStatistic )
                                                    .addStatistics( rangeStatistic )
                                                    .setMetric( metric )
                                                    .build();

        assertEquals( expected, actual );
    }

    @Test
    void testHistogramForDurations()
    {
        SummaryStatistic parameters = SummaryStatistic.newBuilder()
                                                      .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                      .setHistogramBins( 5 )
                                                      .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                      .build();
        DiagramSummaryStatisticFunction histogram = FunctionFactory.ofDiagramSummaryStatistic( parameters );
        BiFunction<Map<SummaryStatisticComponentName, String>, Duration[],
                DiagramStatistic> durationHistogram =
                FunctionFactory.ofDurationDiagramFromUnivariateFunction( histogram, ChronoUnit.SECONDS );

        Duration one = Duration.ofHours( 1 );
        Duration two = Duration.ofHours( 2 );
        Duration three = Duration.ofHours( 3 );
        Duration four = Duration.ofHours( 4 );
        Duration five = Duration.ofHours( 5 );
        Duration six = Duration.ofHours( 6 );

        Duration[] data =
                new Duration[] { one, two, two, three, three, three, four, four, four, four, five, five, five, five,
                        five, six, six, six, six, six, six };
        Map<SummaryStatisticComponentName, String> p =
                Map.of( SummaryStatisticComponentName.METRIC_NAME, "MEAN",
                        SummaryStatisticComponentName.METRIC_UNIT, "bar" );

        DiagramStatistic actual = durationHistogram.apply( p, data );

        // Build the expectation
        DiagramMetric.DiagramMetricComponent domainMetric =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.BIN_UPPER_BOUND )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                    .setMinimum( Double.NEGATIVE_INFINITY )
                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                    .setUnits( "SECONDS" )
                                                    .build();

        DiagramMetric.DiagramMetricComponent rangeMetric =
                DiagramMetric.DiagramMetricComponent.newBuilder()
                                                    .setName( MetricName.COUNT )
                                                    .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                    .setMinimum( 0 )
                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                    .setUnits( Units.COUNT )
                                                    .build();

        DiagramMetric metric = DiagramMetric.newBuilder()
                                            .addComponents( domainMetric )
                                            .addComponents( rangeMetric )
                                            .setName( MetricName.HISTOGRAM )
                                            .setStatisticName( MetricName.MEAN )
                                            .setStatisticUnits( "bar" )
                                            .setStatisticMinimum( Double.NEGATIVE_INFINITY )
                                            .setStatisticMaximum( Double.POSITIVE_INFINITY )
                                            .setStatisticOptimum( Double.NaN )
                                            .build();

        DiagramStatistic.DiagramStatisticComponent domainStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( domainMetric )
                                                          .addAllValues( List.of( 7200.0,
                                                                                  10800.0,
                                                                                  14400.0,
                                                                                  18000.0,
                                                                                  21600.0 ) )
                                                          .build();

        DiagramStatistic.DiagramStatisticComponent rangeStatistic =
                DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                          .setMetric( rangeMetric )
                                                          .addAllValues( List.of( 3.0, 3.0, 4.0, 5.0, 6.0 ) )
                                                          .build();

        DiagramStatistic expected = DiagramStatistic.newBuilder()
                                                    .addStatistics( domainStatistic )
                                                    .addStatistics( rangeStatistic )
                                                    .setMetric( metric )
                                                    .build();

        assertEquals( expected, actual );
    }

    @Test
    void testBoxplot()
    {
        SummaryStatistic parameters = SummaryStatistic.newBuilder()
                                                      .setStatistic( SummaryStatistic.StatisticName.BOX_PLOT )
                                                      .setDimension( SummaryStatistic.StatisticDimension.FEATURES )
                                                      .build();
        BoxplotSummaryStatisticFunction boxplot = FunctionFactory.boxplot( parameters );

        double[] data = new double[] { 1, 2, 2, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 6, 7, 7, 7 };
        Map<SummaryStatisticComponentName, String> p =
                Map.of( SummaryStatisticComponentName.METRIC_NAME, "MEAN",
                        SummaryStatisticComponentName.METRIC_UNIT, "bar" );

        BoxplotStatistic actual = boxplot.apply( p, data );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT )
                                            .setLinkedValueType( BoxplotMetric.LinkedValueType.NONE )
                                            .setQuantileValueType( BoxplotMetric.QuantileValueType.STATISTIC )
                                            .setStatisticName( MetricName.MEAN )
                                            .setUnits( "bar" )
                                            .addAllQuantiles( FunctionFactory.DEFAULT_PROBABILITIES )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setOptimum( Double.NaN )
                                            .build();

        // Compute the quantiles at a rounded precision
        List<Double> box = List.of( 1.0, 3.0, 5.0, 6.0, 7.0 );

        BoxplotStatistic expected = BoxplotStatistic.newBuilder()
                                                    .setMetric( metric )
                                                    .addStatistics( BoxplotStatistic.Box.newBuilder()
                                                                                        .addAllQuantiles( box ) )
                                                    .build();
        assertEquals( expected, actual );
    }

    @Test
    void testOfScalarSummaryStatisticWithMissingValuesAndMinimumSampleSizeProducesMissing()
    {
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                            .build();
        ScalarSummaryStatisticFunction f = FunctionFactory.ofScalarSummaryStatistic( summaryStatistic,
                                                                                     3 );

        double[] data = new double[] { 1.0, 2.0, MissingValues.DOUBLE };

        assertEquals( MissingValues.DOUBLE, f.applyAsDouble( data ) );
    }

    @Test
    void testOfScalarSummaryStatisticWithMissingValuesAndMinimumSampleSizeProducesNonMissing()
    {
        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setStatistic( SummaryStatistic.StatisticName.MEAN )
                                                            .build();
        ScalarSummaryStatisticFunction f = FunctionFactory.ofScalarSummaryStatistic( summaryStatistic,
                                                                                     3 );

        double[] data = new double[] { 1.0, 2.0, 3.0, MissingValues.DOUBLE };

        assertEquals( 2.0, f.applyAsDouble( data ) );
    }
}