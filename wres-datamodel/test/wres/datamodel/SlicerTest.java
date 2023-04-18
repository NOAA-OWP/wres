package wres.datamodel;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.config.yaml.components.ThresholdOperator;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.messages.MessageFactory;
import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.Feature;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.config.yaml.components.ThresholdOrientation;
import wres.datamodel.time.TimeWindowOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link Slicer}.
 *
 * @author James Brown
 */
class SlicerTest
{

    @Test
    void testGetLeftSideSingleValued()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };

        assertArrayEquals( expected, Slicer.getLeftSide( Pool.of( values,
                                                                  PoolMetadata.of() ) ), 0.0 );
    }

    @Test
    void testGetRightSide()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );

        double[] expected = new double[] { 3.0 / 5.0, 1.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0, 0.0 / 5.0, 1.0 / 5.0 };
        assertArrayEquals( expected, Slicer.getRightSide( Pool.of( values,
                                                                   PoolMetadata.of() ) ), 0.0 );
    }

    @Test
    void testRound()
    {
        DoubleUnaryOperator rounder = Slicer.rounder( 1 );
        assertEquals( 2.0, rounder.applyAsDouble( 2.04 ), 0.001 );
    }

    @Test
    void testTransformEnsemblePairToDiscreteProbabilityPair()
    {
        Pair<Double, Ensemble> a = Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5 ) );
        Pair<Double, Ensemble> b = Pair.of( 0.0, Ensemble.of( 1, 2, 2, 3, 3 ) );
        Pair<Double, Ensemble> c = Pair.of( 3.0, Ensemble.of( 3, 3, 3, 3, 3 ) );
        Pair<Double, Ensemble> d = Pair.of( 4.0, Ensemble.of( 4, 4, 4, 4, 4 ) );
        Pair<Double, Ensemble> e = Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) );
        Pair<Double, Ensemble> f = Pair.of( 5.0, Ensemble.of( 1, 1, 6, 6, 50 ) );
        ThresholdOuter threshold = ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                      ThresholdOperator.GREATER,
                                                      ThresholdOrientation.LEFT );

        Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> mapper =
                pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        assertEquals( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ), mapper.apply( a ) );
        assertEquals( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ), mapper.apply( b ) );
        assertEquals( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ), mapper.apply( c ) );
        assertEquals( Pair.of( Probability.ONE, Probability.of( 1.0 ) ), mapper.apply( d ) );
        assertEquals( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ), mapper.apply( e ) );
        assertEquals( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ), mapper.apply( f ) );
    }

    @Test
    void testGetQuantileFromProbability()
    {
        double[] sorted = new double[] { 1.5, 4.9, 6.3, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2 };
        double[] sortedB = new double[] { -50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50 };
        double testA = 0.0;
        double testB = 1.0;
        double testC = 7.0 / 11.0;
        double testD = ( 8.0 + ( ( 5005.0 - 2009.8 ) / ( 7001.4 - 2009.8 ) ) ) / 11.0;
        double testE = 0.5;
        double expectedA = 1.5;
        double expectedB = 17897.2;
        double expectedC = 1647.1818181818185;
        double expectedD = 8924.920568373052;
        double expectedE = 0.0;

        //Test for equality
        DoubleUnaryOperator qFA = Slicer.getQuantileFunction( sorted );
        DoubleUnaryOperator qFB = Slicer.getQuantileFunction( sortedB );

        assertEquals( expectedA, qFA.applyAsDouble( testA ), 7 );
        assertEquals( expectedB, qFA.applyAsDouble( testB ), 7 );
        assertEquals( expectedC, qFA.applyAsDouble( testC ), 7 );
        assertEquals( expectedD, qFA.applyAsDouble( testD ), 7 );
        assertEquals( expectedE, qFB.applyAsDouble( testE ), 7 );

        //Check exceptional cases
        assertThrows( IllegalArgumentException.class, () -> qFA.applyAsDouble( -0.1 ) );
        assertThrows( IllegalArgumentException.class, () -> qFA.applyAsDouble( 1.1 ) );
    }

    /**
     * Tests the {@link Slicer#filterByRightSize(List)}.
     */

    @Test
    void testFilterByRight()
    {
        List<Pair<Double, Ensemble>> input = new ArrayList<>();
        input.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        input.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        input.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        input.add( Pair.of( 2.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        input.add( Pair.of( 2.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        input.add( Pair.of( 2.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        input.add( Pair.of( 2.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        input.add( Pair.of( 2.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        input.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5, 6 ) ) );
        input.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5, 6 ) ) );
        input.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5, 6 ) ) );
        input.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5, 6 ) ) );

        //Slice
        Map<Integer, List<Pair<Double, Ensemble>>> sliced = Slicer.filterByRightSize( input );

        //Check the results
        assertEquals( 3, sliced.size(), "Expected three slices of data." );
        assertEquals( 3, sliced.get( 3 ).size(), "Expected the first slice to contain three pairs." );
        assertEquals( 5, sliced.get( 5 ).size(), "Expected the second slice to contain five pairs." );
        assertEquals( 4, sliced.get( 6 ).size(), "Expected the third slice to contain four pairs." );
    }

    @Test
    void testFilterByMetricComponent()
    {
        //Obtain input and slice
        List<DoubleScoreStatisticOuter> toSlice = new ArrayList<>();

        PoolMetadata meta = PoolMetadata.of();


        DoubleScoreStatisticComponent reliability = DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.5 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.RELIABILITY ) )
                                                                                 .build();
        DoubleScoreStatisticComponent resolution = DoubleScoreStatisticComponent.newBuilder()
                                                                                .setValue( 0.2 )
                                                                                .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                      .setName(
                                                                                                                              ComponentName.RESOLUTION ) )
                                                                                .build();

        DoubleScoreStatisticComponent sharpness = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setValue( 0.1 )
                                                                               .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                     .setName(
                                                                                                                             ComponentName.SHARPNESS ) )
                                                                               .build();

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BRIER_SCORE ) )
                                    .addStatistics( reliability )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BRIER_SCORE ) )
                                    .addStatistics( resolution )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.BRIER_SCORE ) )
                                    .addStatistics( sharpness )
                                    .build();

        DoubleScoreStatisticOuter oneOuter = DoubleScoreStatisticOuter.of( one, meta );

        DoubleScoreStatisticOuter twoOuter = DoubleScoreStatisticOuter.of( two, meta );

        DoubleScoreStatisticOuter threeOuter = DoubleScoreStatisticOuter.of( three, meta );

        toSlice.add( oneOuter );
        toSlice.add( twoOuter );
        toSlice.add( threeOuter );

        Map<MetricConstants, List<DoubleScoreComponentOuter>> sliced = Slicer.filterByMetricComponent( toSlice );

        //Check the results
        assertEquals( 3, sliced.size() );
        assertEquals( List.of( DoubleScoreComponentOuter.of( reliability, meta ) ),
                      sliced.get( MetricConstants.RELIABILITY ) );
        assertEquals( List.of( DoubleScoreComponentOuter.of( resolution, meta ) ),
                      sliced.get( MetricConstants.RESOLUTION ) );
        assertEquals( List.of( DoubleScoreComponentOuter.of( sharpness, meta ) ),
                      sliced.get( MetricConstants.SHARPNESS ) );
    }

    @Test
    void testFilterSingleValuedPairs()
    {
        List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        values.add( Pair.of( Double.NaN, Double.NaN ) );
        values.add( Pair.of( 0.0, Double.NaN ) );
        values.add( Pair.of( Double.NaN, 0.0 ) );

        List<Pair<Double, Double>> expectedValues = new ArrayList<>();
        expectedValues.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        expectedValues.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        expectedValues.add( Pair.of( 1.0, 1.0 / 5.0 ) );

        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        Climatology climatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5, Double.NaN } )
                                         .build();

        Climatology climatologyExpected =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5 } )
                                         .build();
        PoolMetadata meta =
                PoolMetadata.of( Evaluation.newBuilder()
                                           .setMeasurementUnit( "foo" )
                                           .build(),
                                 wres.statistics.generated.Pool.newBuilder()
                                                               .setGeometryGroup( GeometryGroup.newBuilder()
                                                                                               .addGeometryTuples(
                                                                                                       GeometryTuple.newBuilder()
                                                                                                                    .setLeft(
                                                                                                                            geometry )
                                                                                                                    .setRight(
                                                                                                                            geometry ) ) )
                                                               .build() );
        Pool<Pair<Double, Double>> pairs = Pool.of( values, meta, values, PoolMetadata.of( true ), climatology );
        Pool<Pair<Double, Double>> sliced =
                PoolSlicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertEquals( expectedValues, sliced.get() );
        assertEquals( expectedValues, sliced.getBaselineData().get() );
        assertEquals( climatologyExpected, sliced.getClimatology() );
        assertNotEquals( values, sliced.get() );

        //Test without baseline or climatology
        Pool<Pair<Double, Double>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Double>> slicedNoBase =
                PoolSlicer.filter( pairsNoBase, Slicer.leftAndRight( Double::isFinite ), null );

        assertEquals( expectedValues, slicedNoBase.get() );
    }

    @Test
    void testTransformEnsemblePairs()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( Double.NaN, Ensemble.of( Double.NaN, Double.NaN, Double.NaN ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( Double.NaN, Double.NaN, Double.NaN ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( Double.NaN, 2, 3, Double.NaN ) ) );

        List<Pair<Double, Ensemble>> expectedValues = new ArrayList<>();
        expectedValues.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        expectedValues.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        expectedValues.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        expectedValues.add( Pair.of( 0.0, Ensemble.of( 2, 3 ) ) );

        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        Climatology climatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5 } )
                                         .build();

        PoolMetadata meta = PoolMetadata.of();
        Pool<Pair<Double, Ensemble>> pairs = Pool.of( values, meta, values, PoolMetadata.of( true ), climatology );
        Pool<Pair<Double, Ensemble>> sliced =
                PoolSlicer.transform( pairs, Slicer.leftAndEachOfRight( Double::isFinite ) );

        //Test with baseline
        assertEquals( expectedValues, sliced.get() );
        assertEquals( expectedValues, sliced.getBaselineData().get() );
        assertNotEquals( values, sliced.get() );

        //Test without baseline
        Pool<Pair<Double, Ensemble>> pairsNoBase = Pool.of( values, meta );
        Pool<Pair<Double, Ensemble>> slicedNoBase =
                PoolSlicer.transform( pairsNoBase, Slicer.leftAndEachOfRight( Double::isFinite ) );

        assertEquals( expectedValues, slicedNoBase.get() );
    }

    @Test
    void testFilterListOfMetricOutputs()
    {
        // Populate a list of outputs
        PoolMetadata metadata = PoolMetadata.of();

        TimeWindowOuter windowOne = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 1 ) ) );

        TimeWindowOuter windowTwo = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 2 ) ) );

        TimeWindowOuter windowThree = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                        Instant.MAX,
                                                                                        Duration.ofHours( 3 ) ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.1 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.2 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.3 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        List<DoubleScoreStatisticOuter> listOfOutputs =
                Arrays.asList( DoubleScoreStatisticOuter.of( one,
                                                             PoolMetadata.of( metadata,
                                                                              windowOne,
                                                                              thresholdOne ) ),
                               DoubleScoreStatisticOuter.of( two,
                                                             PoolMetadata.of( metadata,
                                                                              windowTwo,
                                                                              thresholdTwo ) ),
                               DoubleScoreStatisticOuter.of( three,
                                                             PoolMetadata.of( metadata,
                                                                              windowThree,
                                                                              thresholdThree ) ) );

        // Filter by the first lead time and the last lead time and threshold
        Predicate<DoubleScoreStatisticOuter> filter = meta -> meta.getMetadata().getTimeWindow().equals( windowOne )
                                                              || ( meta.getMetadata()
                                                                       .getTimeWindow()
                                                                       .equals( windowThree )
                                                                   && meta.getMetadata()
                                                                          .getThresholds()
                                                                          .equals( thresholdThree ) );

        List<DoubleScoreStatisticOuter> actualOutput = Slicer.filter( listOfOutputs, filter );

        List<DoubleScoreStatisticOuter> expectedOutput =
                Arrays.asList( DoubleScoreStatisticOuter.of( one,
                                                             PoolMetadata.of( metadata,
                                                                              windowOne,
                                                                              thresholdOne ) ),
                               DoubleScoreStatisticOuter.of( three,
                                                             PoolMetadata.of( metadata,
                                                                              windowThree,
                                                                              thresholdThree ) ) );

        assertEquals( actualOutput, expectedOutput );
    }

    @Test
    void testDiscoverListOfMetricOutputs()
    {
        // Populate a list of outputs
        PoolMetadata metadata = PoolMetadata.of();

        TimeWindowOuter windowOne = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 1 ) ) );

        TimeWindowOuter windowTwo = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 2 ) ) );

        TimeWindowOuter windowThree = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                        Instant.MAX,
                                                                                        Duration.ofHours( 2 ) ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.1 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.2 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.3 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        List<DoubleScoreStatisticOuter> listOfOutputs =
                Arrays.asList( DoubleScoreStatisticOuter.of( one,
                                                             PoolMetadata.of( metadata,
                                                                              windowOne,
                                                                              thresholdOne ) ),
                               DoubleScoreStatisticOuter.of( two,
                                                             PoolMetadata.of( metadata,
                                                                              windowTwo,
                                                                              thresholdTwo ) ),
                               DoubleScoreStatisticOuter.of( three,
                                                             PoolMetadata.of( metadata,
                                                                              windowThree,
                                                                              thresholdThree ) ) );

        // Discover the metrics available
        Set<MetricConstants> actualOutputOne =
                Slicer.discover( listOfOutputs, DoubleScoreStatisticOuter::getMetricName );
        Set<MetricConstants> expectedOutputOne = Collections.singleton( MetricConstants.BIAS_FRACTION );

        assertEquals( actualOutputOne, expectedOutputOne );

        // Discover the unique time windows available
        Set<TimeWindowOuter> actualOutputTwo =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getTimeWindow() );
        Set<TimeWindowOuter> expectedOutputTwo = new TreeSet<>( Arrays.asList( windowOne, windowTwo, windowThree ) );

        assertEquals( actualOutputTwo, expectedOutputTwo );

        // Discover the thresholds available
        Set<OneOrTwoThresholds> actualOutputThree =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getThresholds() );
        Set<OneOrTwoThresholds> expectedOutputThree =
                new TreeSet<>( Arrays.asList( thresholdOne, thresholdTwo, thresholdThree ) );

        assertEquals( actualOutputThree, expectedOutputThree );

        // Discover the unique lead times available
        Set<Pair<Duration, Duration>> actualOutputFour =
                Slicer.discover( listOfOutputs,
                                 next -> Pair.of( next.getMetadata()
                                                      .getTimeWindow()
                                                      .getEarliestLeadDuration(),
                                                  next.getMetadata()
                                                      .getTimeWindow()
                                                      .getLatestLeadDuration() ) );

        Set<Pair<Duration, Duration>> expectedOutputFour =
                new TreeSet<>( Arrays.asList( Pair.of( Duration.ofHours( 1 ), Duration.ofHours( 1 ) ),
                                              Pair.of( Duration.ofHours( 2 ), Duration.ofHours( 2 ) ) ) );

        assertEquals( actualOutputFour, expectedOutputFour );

        // Discover the second thresholds, which are not available
        assertTrue( Slicer.discover( listOfOutputs,
                                     next -> next.getMetadata().getThresholds().second() )
                          .isEmpty() );

    }

    @Test
    void testFilter()
    {
        Geometry geometry = Geometry.newBuilder()
                                    .setName( "feature" )
                                    .build();
        Feature feature = Feature.of( geometry );

        Climatology climatology =
                new Climatology.Builder().addClimatology( feature, new double[] { 1, 2, 3, 4, 5, 6, 7 } )
                                         .build();

        Climatology expectedOutput = new Climatology.Builder().addClimatology( feature, new double[] { 1, 3, 5, 7 } )
                                                              .build();
        DoublePredicate predicate = d -> ( d == 1 || d == 3 || d == 5 || d == 7 );
        Climatology actualOutput = Slicer.filter( climatology, predicate );

        assertEquals( expectedOutput, actualOutput );
    }

    @Test
    void testConcatenate()
    {
        assertEquals( VectorOfDoubles.of(), Slicer.concatenate() );

        VectorOfDoubles input = VectorOfDoubles.of( 1, 2, 3 );

        VectorOfDoubles actualOutputOne = Slicer.concatenate( input );
        VectorOfDoubles expectedOutputOne = VectorOfDoubles.of( 1, 2, 3 );

        assertEquals( expectedOutputOne, actualOutputOne );

        VectorOfDoubles anotherInput = VectorOfDoubles.of( 4, 5, 6 );
        VectorOfDoubles expectedOutputTwo = VectorOfDoubles.of( 1, 2, 3, 4, 5, 6 );
        VectorOfDoubles actualOutputTwo = Slicer.concatenate( input, anotherInput );

        assertEquals( expectedOutputTwo, actualOutputTwo );
    }

    @Test
    void testSortStatisticsByTimeWindowAndThreshold()
    {
        // Populate a list of outputs
        PoolMetadata metadata = PoolMetadata.of();

        TimeWindowOuter windowOne = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 1 ) ) );

        TimeWindowOuter windowTwo = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                      Instant.MAX,
                                                                                      Duration.ofHours( 2 ) ) );

        TimeWindowOuter windowThree = TimeWindowOuter.of( MessageFactory.getTimeWindow( Instant.MIN,
                                                                                        Instant.MAX,
                                                                                        Duration.ofHours( 2 ) ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 1.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 2.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( ThresholdOuter.of( OneOrTwoDoubles.of( 3.0 ),
                                                          ThresholdOperator.GREATER,
                                                          ThresholdOrientation.LEFT ) );

        DoubleScoreStatistic one =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.1 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic two =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder().setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.2 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        DoubleScoreStatistic three =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( DoubleScoreMetric.newBuilder()
                                                                 .setName( MetricName.BIAS_FRACTION ) )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setValue( 0.3 )
                                                                                 .setMetric( DoubleScoreMetricComponent.newBuilder()
                                                                                                                       .setName(
                                                                                                                               ComponentName.MAIN ) ) )
                                    .build();

        List<DoubleScoreStatisticOuter> unorderedStatistics =
                List.of( DoubleScoreStatisticOuter.of( three,
                                                       PoolMetadata.of( metadata,
                                                                        windowThree,
                                                                        thresholdThree ) ),
                         DoubleScoreStatisticOuter.of( one,
                                                       PoolMetadata.of( metadata,
                                                                        windowOne,
                                                                        thresholdOne ) ),
                         DoubleScoreStatisticOuter.of( two,
                                                       PoolMetadata.of( metadata,
                                                                        windowTwo,
                                                                        thresholdTwo ) ) );

        List<DoubleScoreStatisticOuter> actual = Slicer.sortByTimeWindowAndThreshold( unorderedStatistics );

        List<DoubleScoreStatisticOuter> expected = List.of( unorderedStatistics.get( 1 ),
                                                            unorderedStatistics.get( 2 ),
                                                            unorderedStatistics.get( 0 ) );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterEnsemble()
    {
        double[] members = new double[] { 1, 2, 3, 4, 5 };
        Labels labels = Labels.of( "6", "7", "8", "9", "10" );
        Ensemble toFilter = Ensemble.of( members, labels );

        Ensemble actual = Slicer.filter( toFilter, "6", "7", "9", "10" );
        Ensemble expected = Ensemble.of( new double[] { 3 }, Labels.of( "8" ) );

        assertEquals( expected, actual );
    }

    @Test
    void testFilterEnsembleThrowsIllegalArgumentExceptionWhenLabelsAreMissing()
    {
        Ensemble ensemble = Ensemble.of( 1.0 );
        assertThrows( IllegalArgumentException.class,
                      () -> Slicer.filter( ensemble, "aLabel" ) );
    }

    @Test
    void testFilterThrowsNullPointerExceptionWhenEnsembleIsNull()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( null, "aLabel" ) );
    }

    @Test
    void testFilterThrowsNullPointerExceptionWhenLabelsIsNull()
    {
        Ensemble ensemble = Ensemble.of( 1.0 );
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( ensemble, ( String[] ) null ) );
    }

    @Test
    void testFilterListOfMetricOutputsWithNullListProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( null, ( Predicate<Statistic<?>> ) null ) );
    }

    @Test
    void testFilterListOfMetricOutputsWithNullPredicateProducesNPE()
    {
        List<Statistic<?>> list = List.of();
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( list, ( Predicate<Statistic<?>> ) null ) );
    }

    @Test
    void testDiscoverListOfMetricOutputsWithNullListProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.discover( null, ( Function<Statistic<?>, ?> ) null ) );
    }

    @Test
    void testDiscoverListOfMetricOutputsWithNullFunctionProducesNPE()
    {
        List<Statistic<?>> list = List.of();
        assertThrows( NullPointerException.class,
                      () -> Slicer.discover( list, ( Function<Statistic<?>, ?> ) null ) );
    }
    
    @Test
    void testRounderProducesInputValueWhenInputIsNotFinite()
    {
        // See issue #115230
        DoubleUnaryOperator slicer = Slicer.rounder( 5 );

        assertEquals( Double.NaN, slicer.applyAsDouble( Double.NaN ) );
    }
}
