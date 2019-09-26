package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.statistics.DataModelTestDataFactory;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindow;

/**
 * Tests the {@link Slicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SlicerTest
{

    private static final String TWELFTH_TIME = "1985-01-03T03:00:00Z";
    private static final String ELEVENTH_TIME = "1985-01-03T02:00:00Z";
    private static final String TENTH_TIME = "1985-01-03T01:00:00Z";
    private static final String NINTH_TIME = "1985-01-03T00:00:00Z";
    private static final String EIGHTH_TIME = "1985-01-02T03:00:00Z";
    private static final String SEVENTH_TIME = "1985-01-02T02:00:00Z";
    private static final String SIXTH_TIME = "1985-01-02T01:00:00Z";
    private static final String FIFTH_TIME = "1985-01-02T00:00:00Z";
    private static final String FOURTH_TIME = "1985-01-01T03:00:00Z";
    private static final String THIRD_TIME = "1985-01-01T02:00:00Z";
    private static final String SECOND_TIME = "1985-01-01T01:00:00Z";
    private static final String FIRST_TIME = "1985-01-01T00:00:00Z";

    @Test
    public void testGetLeftSideSingleValued()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };

        assertTrue( Arrays.equals( Slicer.getLeftSide( SampleDataBasic.of( values,
                                                                           SampleMetadata.of() ) ),
                                   expected ) );
    }

    @Test
    public void testGetRightSide()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );

        double[] expected = new double[] { 3.0 / 5.0, 1.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0, 0.0 / 5.0, 1.0 / 5.0 };
        assertTrue( Arrays.equals( Slicer.getRightSide( SampleDataBasic.of( values,
                                                                            SampleMetadata.of() ) ),
                                   expected ) );
    }

    @Test
    public void testFilterSingleValuedPairsByLeft()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Double, Double>> pairs = SampleDataBasic.of( values, meta, values, meta, null );
        SampleData<Pair<Double, Double>> sliced =
                Slicer.filter( pairs, Slicer.left( threshold::test ), threshold::test );
        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        SampleData<Pair<Double, Double>> pairsNoBase = SampleDataBasic.of( values, meta );
        SampleData<Pair<Double, Double>> slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.left( threshold::test ), threshold::test );
        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    @Test
    public void testFilterEnsemblePairsByLeft()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 1, 2, 3 ) ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Double, Ensemble>> pairs = SampleDataBasic.of( values, meta, values, meta, null );
        SampleData<Pair<Double, Ensemble>> sliced =
                Slicer.filter( pairs, Slicer.leftVector( threshold::test ), threshold::test );

        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );

        //Test without baseline
        SampleData<Pair<Double, Ensemble>> pairsNoBase = SampleDataBasic.of( values, meta );
        SampleData<Pair<Double, Ensemble>> slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.leftVector( threshold::test ), threshold::test );

        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    @Test
    public void testTransformEnsemblePairsToSingleValuedPairs()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 6, 7, 8, 9, 10 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 11, 12, 13, 14, 15 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 16, 17, 18, 19, 20 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 21, 22, 23, 24, 25 ) ) );
        values.add( Pair.of( 1.0, Ensemble.of( 26, 27, 28, 29, 30 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Double, Ensemble>> input = SampleDataBasic.of( values, meta, values, meta, null );
        Function<Pair<Double, Ensemble>, Pair<Double, Double>> mapper =
                in -> Pair.of( in.getLeft(),
                               Arrays.stream( in.getRight().getMembers() ).average().getAsDouble() );
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                Slicer.getRightSide( Slicer.transform( SampleDataBasic.of( values, meta ), mapper ) );
        assertTrue( Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = Slicer.getRightSide( Slicer.transform( input, mapper ).getBaselineData() );
        assertTrue( Arrays.equals( actualBase, expected ) );
    }

    @Test
    public void testTransformSingleValuedPairsToDichotomousPairs()
    {
        final List<Pair<Double, Double>> values = new ArrayList<>();
        values.add( Pair.of( 0.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 1.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 2.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 3.0 / 5.0 ) );
        values.add( Pair.of( 0.0, 0.0 / 5.0 ) );
        values.add( Pair.of( 1.0, 1.0 / 5.0 ) );
        SampleMetadata meta = SampleMetadata.of();
        Function<Pair<Double, Double>, Pair<Boolean, Boolean>> mapper =
                in -> Pair.of( in.getLeft() > 0, in.getRight() > 0 );
        final List<Pair<Boolean, Boolean>> expectedValues = new ArrayList<>();
        expectedValues.add( Pair.of( false, true ) );
        expectedValues.add( Pair.of( false, true ) );
        expectedValues.add( Pair.of( true, true ) );
        expectedValues.add( Pair.of( true, true ) );
        expectedValues.add( Pair.of( false, false ) );
        expectedValues.add( Pair.of( true, true ) );

        SampleData<Pair<Boolean, Boolean>> expectedNoBase = SampleDataBasic.of( expectedValues, meta );
        SampleData<Pair<Boolean, Boolean>> expectedBase = SampleDataBasic.of( expectedValues,
                                                                              meta,
                                                                              expectedValues,
                                                                              meta,
                                                                              null );

        //Test without baseline
        SampleData<Pair<Boolean, Boolean>> actualNoBase =
                Slicer.transform( SampleDataBasic.of( values, meta ), mapper );
        assertTrue( actualNoBase.getRawData().equals( expectedNoBase.getRawData() ) );

        //Test baseline
        SampleData<Pair<Boolean, Boolean>> actualBase =
                Slicer.transform( SampleDataBasic.of( values, meta, values, meta, null ),
                                  mapper );
        assertTrue( actualBase.getBaselineData().getRawData().equals( expectedBase.getBaselineData().getRawData() ) );
    }

    @Test
    public void testTransformEnsemblePairsToDiscreteProbabilityPairs()
    {
        final List<Pair<Double, Ensemble>> values = new ArrayList<>();
        values.add( Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 2, 3, 3 ) ) );
        values.add( Pair.of( 3.0, Ensemble.of( 3, 3, 3, 3, 3 ) ) );
        values.add( Pair.of( 4.0, Ensemble.of( 4, 4, 4, 4, 4 ) ) );
        values.add( Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) ) );
        values.add( Pair.of( 5.0, Ensemble.of( 1, 1, 6, 6, 50 ) ) );
        SampleMetadata meta = SampleMetadata.of();
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );

        List<Pair<Probability, Probability>> expectedPairs = new ArrayList<>();
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) );
        expectedPairs.add( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) );

        //Test without baseline
        SampleData<Pair<Double, Ensemble>> pairs = SampleDataBasic.of( values, meta );

        Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> mapper =
                pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        SampleData<Pair<Probability, Probability>> sliced =
                Slicer.transform( pairs, mapper );

        assertTrue( sliced.getRawData().equals( expectedPairs ) );

        //Test baseline
        SampleData<Pair<Probability, Probability>> slicedWithBaseline =
                Slicer.transform( SampleDataBasic.of( values, meta, values, meta, null ), mapper );
        assertTrue( slicedWithBaseline.getRawData().equals( expectedPairs ) );
        assertTrue( slicedWithBaseline.getBaselineData().getRawData().equals( expectedPairs ) );
    }

    @Test
    public void testTransformEnsemblePairToDiscreteProbabilityPair()
    {
        Pair<Double, Ensemble> a = Pair.of( 3.0, Ensemble.of( 1, 2, 3, 4, 5 ) );
        Pair<Double, Ensemble> b = Pair.of( 0.0, Ensemble.of( 1, 2, 2, 3, 3 ) );
        Pair<Double, Ensemble> c = Pair.of( 3.0, Ensemble.of( 3, 3, 3, 3, 3 ) );
        Pair<Double, Ensemble> d = Pair.of( 4.0, Ensemble.of( 4, 4, 4, 4, 4 ) );
        Pair<Double, Ensemble> e = Pair.of( 0.0, Ensemble.of( 1, 2, 3, 4, 5 ) );
        Pair<Double, Ensemble> f = Pair.of( 5.0, Ensemble.of( 1, 1, 6, 6, 50 ) );
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );

        Function<Pair<Double, Ensemble>, Pair<Probability, Probability>> mapper =
                pair -> Slicer.toDiscreteProbabilityPair( pair, threshold );

        assertTrue( mapper.apply( a ).equals( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) ) );
        assertTrue( mapper.apply( b ).equals( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) ) );
        assertTrue( mapper.apply( c ).equals( Pair.of( Probability.ZERO, Probability.of( 0.0 / 5.0 ) ) ) );
        assertTrue( mapper.apply( d ).equals( Pair.of( Probability.ONE, Probability.of( 1.0 ) ) ) );
        assertTrue( mapper.apply( e ).equals( Pair.of( Probability.ZERO, Probability.of( 2.0 / 5.0 ) ) ) );
        assertTrue( mapper.apply( f ).equals( Pair.of( Probability.ONE, Probability.of( 3.0 / 5.0 ) ) ) );
    }

    @Test
    public void testGetInverseCumulativeProbability()
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

        assertTrue( DataFactory.doubleEquals( qFA.applyAsDouble( testA ), expectedA, 7 ) );
        assertTrue( DataFactory.doubleEquals( qFA.applyAsDouble( testB ), expectedB, 7 ) );
        assertTrue( DataFactory.doubleEquals( qFA.applyAsDouble( testC ), expectedC, 7 ) );
        assertTrue( DataFactory.doubleEquals( qFA.applyAsDouble( testD ), expectedD, 7 ) );
        assertTrue( DataFactory.doubleEquals( qFB.applyAsDouble( testE ), expectedE, 7 ) );

        //Check exceptional cases
        assertThrows( IllegalArgumentException.class, () -> qFA.applyAsDouble( -0.1 ) );
        assertThrows( IllegalArgumentException.class, () -> qFA.applyAsDouble( 1.1 ) );
    }

    /**
     * Tests the {@link Slicer#filterByRightSize(List)}.
     */

    @Test
    public void testFilterByRight()
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
        assertTrue( "Expected three slices of data.", sliced.size() == 3 );
        assertTrue( "Expected the first slice to contain three pairs.", sliced.get( 3 ).size() == 3 );
        assertTrue( "Expected the second slice to contain five pairs.", sliced.get( 5 ).size() == 5 );
        assertTrue( "Expected the third slice to contain four pairs.", sliced.get( 6 ).size() == 4 );
    }

    @Test
    public void testFilterByMetricComponent()
    {
        //Obtain input and slice
        ListOfStatistics<DoubleScoreStatistic> toSlice =
                DataModelTestDataFactory.getVectorMetricOutputOne();
        Map<MetricConstants, ListOfStatistics<DoubleScoreStatistic>> sliced =
                Slicer.filterByMetricComponent( toSlice );

        //Check the results
        assertTrue( "Expected five slices of data.", sliced.size() == 5 );

        sliced.forEach( ( key, value ) -> assertTrue( "Expected 638 elements in each slice.",
                                                      value.getData().size() == 638 ) );
    }

    @Test
    public void testFilterSingleValuedPairs()
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

        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Double, Double>> pairs = SampleDataBasic.of( values, meta, values, meta, climatology );
        SampleData<Pair<Double, Double>> sliced =
                Slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( sliced.getRawData().equals( expectedValues ) );
        assertTrue( sliced.getBaselineData().getRawData().equals( expectedValues ) );
        assertTrue( Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( !Arrays.equals( Slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        SampleData<Pair<Double, Double>> pairsNoBase = SampleDataBasic.of( values, meta );
        SampleData<Pair<Double, Double>> slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.leftAndRight( Double::isFinite ), null );

        assertTrue( slicedNoBase.getRawData().equals( expectedValues ) );
    }

    @Test
    public void testTransformEnsemblePairs()
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

        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        SampleMetadata meta = SampleMetadata.of();
        SampleData<Pair<Double, Ensemble>> pairs = SampleDataBasic.of( values, meta, values, meta, climatology );
        SampleData<Pair<Double, Ensemble>> sliced =
                Slicer.transform( pairs, Slicer.leftAndEachOfRight( Double::isFinite ) );

        //Test with baseline
        assertTrue( sliced.getRawData().equals( expectedValues ) );
        assertTrue( sliced.getBaselineData().getRawData().equals( expectedValues ) );
        assertTrue( !sliced.getRawData().equals( values ) );

        //Test without baseline or climatology
        SampleData<Pair<Double, Ensemble>> pairsNoBase = SampleDataBasic.of( values, meta );
        SampleData<Pair<Double, Ensemble>> slicedNoBase =
                Slicer.transform( pairsNoBase, Slicer.leftAndEachOfRight( Double::isFinite ) );

        assertTrue( slicedNoBase.getRawData().equals( expectedValues ) );
    }

    @Test
    public void testFilterTimeSeriesOfSingleValuedPairs()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<Pair<Double, Double>>> first = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> second = new TreeSet<>();
        SortedSet<Event<Pair<Double, Double>>> third = new TreeSet<>();
        PoolOfPairsBuilder<Double, Double> b = new PoolOfPairsBuilder<>();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), Pair.of( 1.0, 10.0 ) ) );
        first.add( Event.of( Instant.parse( THIRD_TIME ), Pair.of( 2.0, 11.0 ) ) );
        first.add( Event.of( Instant.parse( FOURTH_TIME ), Pair.of( 3.0, 12.0 ) ) );

        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( Instant.parse( SIXTH_TIME ),
                              Pair.of( 4.0, 13.0 ) ) );
        second.add( Event.of( Instant.parse( SEVENTH_TIME ),
                              Pair.of( 5.0, 14.0 ) ) );
        second.add( Event.of( Instant.parse( EIGHTH_TIME ),
                              Pair.of( 6.0, 15.0 ) ) );

        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( Instant.parse( TENTH_TIME ), Pair.of( 7.0, 16.0 ) ) );
        third.add( Event.of( Instant.parse( ELEVENTH_TIME ), Pair.of( 8.0, 17.0 ) ) );
        third.add( Event.of( Instant.parse( TWELFTH_TIME ), Pair.of( 9.0, 18.0 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        //Add the time-series
        PoolOfPairs<Double, Double> firstSeries = b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                        first ) )
                                                         .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                        second ) )
                                                         .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                        third ) )
                                                         .setMetadata( meta )
                                                         .build();

        // Filter all values where the left side is greater than 0
        PoolOfPairs<Double, Double> firstResult =
                Slicer.filter( firstSeries,
                               Slicer.left( value -> value > 0 ),
                               null );

        assertTrue( firstResult.getRawData().equals( firstSeries.getRawData() ) );

        // Filter all values where the left side is greater than 3
        PoolOfPairs<Double, Double> secondResult =
                Slicer.filter( firstSeries,
                               Slicer.left( value -> value > 3 ),
                               clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> secondData = new ArrayList<>();
        secondResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( secondData::add ) );
        List<Event<Pair<Double, Double>>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertTrue( secondData.equals( secondBenchmark ) );

        // Add climatology for later
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than or equal to 7
        PoolOfPairs<Double, Double> thirdResult =
                Slicer.filter( firstSeries,
                               Slicer.leftAndRight( value -> value >= 7 ),
                               null );

        List<Event<Pair<Double, Double>>> thirdData = new ArrayList<>();
        thirdResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( thirdData::add ) );
        List<Event<Pair<Double, Double>>> thirdBenchmark = new ArrayList<>();
        thirdBenchmark.addAll( third );

        assertTrue( thirdData.equals( thirdBenchmark ) );

        // Filter on climatology simultaneously
        PoolOfPairs<Double, Double> fourthResult =
                Slicer.filter( b.build(),
                               Slicer.leftAndRight( value -> value > 7 ),
                               Double::isFinite );
        
        assertTrue( fourthResult.getClimatology().equals( climatologyExpected ) );

        // Also filter baseline data
        b.addTimeSeriesForBaseline( TimeSeries.of( firstBasisTime, first ) )
         .addTimeSeriesForBaseline( TimeSeries.of( secondBasisTime, second ) )
         .setMetadataForBaseline( meta );

        // Filter all values where both sides are greater than or equal to 4
        PoolOfPairs<Double, Double> fifthResult =
                Slicer.filter( b.build(),
                               Slicer.left( value -> value >= 4 ),
                               clim -> clim > 0 );

        List<Event<Pair<Double, Double>>> fifthData = new ArrayList<>();
        fifthResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( fifthData::add ) );

        // Same as second benchmark for main data
        assertTrue( fifthData.equals( secondBenchmark ) );

        // Baseline data
        List<Event<Pair<Double, Double>>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData()
                   .get()
                   .forEach( nextSeries -> nextSeries.getEvents().forEach( fifthDataBase::add ) );
        List<Event<Pair<Double, Double>>> fifthBenchmarkBase = new ArrayList<>();
        fifthBenchmarkBase.addAll( second );

        assertTrue( fifthDataBase.equals( fifthBenchmarkBase ) );

    }

    @Test
    public void testFilterListOfMetricOutputs()
    {
        // Populate a list of outputs
        SampleMetadata metadata = SampleMetadata.of( MeasurementUnit.of() );

        TimeWindow windowOne =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        TimeWindow windowTwo =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        TimeWindow windowThree =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 3 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 2.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        ListOfStatistics<DoubleScoreStatistic> listOfOutputs =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowOne,
                                                                                                                      thresholdOne ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.2,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowTwo,
                                                                                                                      thresholdTwo ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.3,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowThree,
                                                                                                                      thresholdThree ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ) ) );

        // Filter by the first lead time and the last lead time and threshold
        Predicate<StatisticMetadata> filter = meta -> meta.getSampleMetadata().getTimeWindow().equals( windowOne )
                                                      || ( meta.getSampleMetadata()
                                                               .getTimeWindow()
                                                               .equals( windowThree )
                                                           && meta.getSampleMetadata()
                                                                  .getThresholds()
                                                                  .equals( thresholdThree ) );

        ListOfStatistics<DoubleScoreStatistic> actualOutput = Slicer.filter( listOfOutputs, filter );

        ListOfStatistics<DoubleScoreStatistic> expectedOutput =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowOne,
                                                                                                                      thresholdOne ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.3,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowThree,
                                                                                                                      thresholdThree ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ) ) );

        assertEquals( actualOutput, expectedOutput );
    }

    @Test
    public void testDiscoverListOfMetricOutputs()
    {
        // Populate a list of outputs
        SampleMetadata metadata = SampleMetadata.of( MeasurementUnit.of() );

        TimeWindow windowOne =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 1 ) );

        TimeWindow windowTwo =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        TimeWindow windowThree =
                TimeWindow.of( Instant.MIN, Instant.MAX, Duration.ofHours( 2 ) );

        OneOrTwoThresholds thresholdOne =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 1.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdTwo =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 2.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );
        OneOrTwoThresholds thresholdThree =
                OneOrTwoThresholds.of( Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                                     Operator.GREATER,
                                                     ThresholdDataType.LEFT ) );

        ListOfStatistics<DoubleScoreStatistic> listOfOutputs =
                ListOfStatistics.of( Arrays.asList( DoubleScoreStatistic.of( 0.1,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowOne,
                                                                                                                      thresholdOne ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.2,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowTwo,
                                                                                                                      thresholdTwo ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ),
                                                    DoubleScoreStatistic.of( 0.3,
                                                                             StatisticMetadata.of( SampleMetadata.of( metadata,
                                                                                                                      windowThree,
                                                                                                                      thresholdThree ),
                                                                                                   0,
                                                                                                   MeasurementUnit.of(),
                                                                                                   MetricConstants.BIAS_FRACTION,
                                                                                                   MetricConstants.MAIN ) ) ) );

        // Discover the metrics available
        Set<MetricConstants> actualOutputOne =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getMetricID() );
        Set<MetricConstants> expectedOutputOne = Collections.singleton( MetricConstants.BIAS_FRACTION );

        assertEquals( actualOutputOne, expectedOutputOne );

        // Discover the unique time windows available
        Set<TimeWindow> actualOutputTwo =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getSampleMetadata().getTimeWindow() );
        Set<TimeWindow> expectedOutputTwo = new TreeSet<>( Arrays.asList( windowOne, windowTwo, windowThree ) );

        assertEquals( actualOutputTwo, expectedOutputTwo );

        // Discover the thresholds available
        Set<OneOrTwoThresholds> actualOutputThree =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getSampleMetadata().getThresholds() );
        Set<OneOrTwoThresholds> expectedOutputThree =
                new TreeSet<>( Arrays.asList( thresholdOne, thresholdTwo, thresholdThree ) );

        assertEquals( actualOutputThree, expectedOutputThree );

        // Discover the unique lead times available
        Set<Pair<Duration, Duration>> actualOutputFour =
                Slicer.discover( listOfOutputs,
                                 next -> Pair.of( next.getMetadata()
                                                      .getSampleMetadata()
                                                      .getTimeWindow()
                                                      .getEarliestLeadDuration(),
                                                  next.getMetadata()
                                                      .getSampleMetadata()
                                                      .getTimeWindow()
                                                      .getLatestLeadDuration() ) );

        Set<Pair<Duration, Duration>> expectedOutputFour =
                new TreeSet<>( Arrays.asList( Pair.of( Duration.ofHours( 1 ), Duration.ofHours( 1 ) ),
                                              Pair.of( Duration.ofHours( 2 ), Duration.ofHours( 2 ) ) ) );

        assertEquals( actualOutputFour, expectedOutputFour );

        // Discover the second thresholds, which are not available
        assertTrue( Slicer.discover( listOfOutputs,
                                     next -> next.getMetadata().getSampleMetadata().getThresholds().second() )
                          .isEmpty() );

    }

    @Test
    public void testFilter()
    {
        VectorOfDoubles input = VectorOfDoubles.of( 1, 2, 3, 4, 5, 6, 7 );
        VectorOfDoubles expectedOutput = VectorOfDoubles.of( 1, 3, 5, 7 );
        DoublePredicate predicate = d -> ( d == 1 || d == 3 || d == 5 || d == 7 );
        VectorOfDoubles actualOutput = Slicer.filter( input, predicate );

        assertEquals( expectedOutput, actualOutput );
    }

    @Test
    public void testFilterListOfMetricOutputsWithNullListProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( (ListOfStatistics<Statistic<?>>) null,
                                           (Predicate<StatisticMetadata>) null ) );
    }

    @Test
    public void testFilterListOfMetricOutputsWithNullPredicateProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.filter( ListOfStatistics.of( Arrays.asList() ),
                                           (Predicate<StatisticMetadata>) null ) );
    }

    @Test
    public void testDiscoverListOfMetricOutputsWithNullListProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.discover( (ListOfStatistics<Statistic<?>>) null,
                                             (Function<Statistic<?>, ?>) null ) );
    }

    @Test
    public void testDiscoverListOfMetricOutputsWithNullFunctionProducesNPE()
    {
        assertThrows( NullPointerException.class,
                      () -> Slicer.discover( ListOfStatistics.of( Arrays.asList() ),
                                             (Function<Statistic<?>, ?>) null ) );
    }

}
