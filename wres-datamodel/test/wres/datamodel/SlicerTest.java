package wres.datamodel;

import static org.junit.Assert.assertEquals;
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
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.DichotomousPair;
import wres.datamodel.sampledata.pairs.DichotomousPairs;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPair;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
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
import wres.datamodel.time.TimeSeriesSlicer;
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
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link Slicer#getLeftSide(SingleValuedPairs)}.
     */

    @Test
    public void testGetLeftSideSingleValued()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 0, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 1.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 0.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };
        assertTrue( Arrays.equals( Slicer.getLeftSide( SingleValuedPairs.of( values,
                                                                             SampleMetadata.of() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#getLeftSide(EnsemblePairs)}.
     */

    @Test
    public void testGetLeftSideEnsemble()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };
        assertTrue( Arrays.equals( Slicer.getLeftSide( EnsemblePairs.of( values,
                                                                         SampleMetadata.of() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#getRightSide(SingleValuedPairs)}.
     */

    @Test
    public void testGetRightSide()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 0, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 1.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 0.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 3.0 / 5.0, 1.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0, 0.0 / 5.0, 1.0 / 5.0 };
        assertTrue( Arrays.equals( Slicer.getRightSide( SingleValuedPairs.of( values,
                                                                              SampleMetadata.of() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#filter(SingleValuedPairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterSingleValuedPairsByLeft()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 0, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 1.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 0.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        SampleMetadata meta = SampleMetadata.of();
        SingleValuedPairs pairs = SingleValuedPairs.of( values, values, meta, meta, null );
        SingleValuedPairs sliced =
                Slicer.filter( pairs, Slicer.left( threshold::test ), threshold::test );
        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        SingleValuedPairs pairsNoBase = SingleValuedPairs.of( values, meta );
        SingleValuedPairs slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.left( threshold::test ), threshold::test );
        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    /**
     * Tests the {@link Slicer#filter(EnsemblePairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterEnsemblePairsByLeft()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 0.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        SampleMetadata meta = SampleMetadata.of();
        EnsemblePairs pairs = EnsemblePairs.of( values, values, meta, meta, null );
        EnsemblePairs sliced =
                Slicer.filter( pairs, Slicer.leftVector( threshold::test ), threshold::test );
        //Test with baseline
        assertTrue( Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        EnsemblePairs pairsNoBase = EnsemblePairs.of( values, meta );
        EnsemblePairs slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.leftVector( threshold::test ), threshold::test );
        assertTrue( Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    /**
     * Tests the {@link Slicer#toSingleValuedPairs(EnsemblePairs, Function)}.
     */

    @Test
    public void testTransformEnsemblePairsToSingleValuedPairs()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 6, 7, 8, 9, 10 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 11, 12, 13, 14, 15 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 16, 17, 18, 19, 20 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 21, 22, 23, 24, 25 } ) );
        values.add( EnsemblePair.of( 1, new double[] { 26, 27, 28, 29, 30 } ) );
        SampleMetadata meta = SampleMetadata.of();
        EnsemblePairs input = EnsemblePairs.of( values, values, meta, meta, null );
        Function<EnsemblePair, SingleValuedPair> mapper =
                in -> SingleValuedPair.of( in.getLeft(), Arrays.stream( in.getRight() ).average().getAsDouble() );
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                Slicer.getRightSide( Slicer.toSingleValuedPairs( EnsemblePairs.of( values, meta ),
                                                                 mapper ) );
        assertTrue( Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = Slicer.getRightSide( Slicer.toSingleValuedPairs( input, mapper ).getBaselineData() );
        assertTrue( Arrays.equals( actualBase, expected ) );
    }

    /**
     * Tests the {@link Slicer#toDichotomousPairs(SingleValuedPairs, Function)}. 
     */

    @Test
    public void testTransformSingleValuedPairsToDichotomousPairs()
    {
        final List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 0, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 1.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 0, 0.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );
        SampleMetadata meta = SampleMetadata.of();
        Function<SingleValuedPair, DichotomousPair> mapper =
                in -> DichotomousPair.of( in.getLeft() > 0, in.getRight() > 0 );
        final List<DichotomousPair> expectedValues = new ArrayList<>();
        expectedValues.add( DichotomousPair.of( false, true ) );
        expectedValues.add( DichotomousPair.of( false, true ) );
        expectedValues.add( DichotomousPair.of( true, true ) );
        expectedValues.add( DichotomousPair.of( true, true ) );
        expectedValues.add( DichotomousPair.of( false, false ) );
        expectedValues.add( DichotomousPair.of( true, true ) );
        DichotomousPairs expectedNoBase = DichotomousPairs.ofDichotomousPairs( expectedValues, meta );
        DichotomousPairs expectedBase = DichotomousPairs.ofDichotomousPairs( expectedValues,
                                                                             expectedValues,
                                                                             meta,
                                                                             meta,
                                                                             null );

        //Test without baseline
        DichotomousPairs actualNoBase =
                Slicer.toDichotomousPairs( SingleValuedPairs.of( values, meta ), mapper );
        assertTrue( actualNoBase.getRawData().equals( expectedNoBase.getRawData() ) );
        //Test baseline
        DichotomousPairs actualBase =
                Slicer.toDichotomousPairs( SingleValuedPairs.of( values, values, meta, meta, null ),
                                           mapper );
        assertTrue( actualBase.getBaselineData().getRawData().equals( expectedBase.getBaselineData().getRawData() ) );
    }

    /**
     * Tests the {@link Slicer#toDiscreteProbabilityPairs(EnsemblePairs, Threshold, BiFunction)}.
     */

    @Test
    public void testTransformEnsemblePairsToDiscreteProbabilityPairs()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 2, 3, 3 } ) );
        values.add( EnsemblePair.of( 3, new double[] { 3, 3, 3, 3, 3 } ) );
        values.add( EnsemblePair.of( 4, new double[] { 4, 4, 4, 4, 4 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( EnsemblePair.of( 5, new double[] { 1, 1, 6, 6, 50 } ) );
        SampleMetadata meta = SampleMetadata.of();
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> mapper =
                Slicer::toDiscreteProbabilityPair;

        List<DiscreteProbabilityPair> expectedPairs = new ArrayList<>();
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 1.0, 1.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 1.0, 3.0 / 5.0 ) );

        //Test without baseline
        DiscreteProbabilityPairs sliced =
                Slicer.toDiscreteProbabilityPairs( EnsemblePairs.of( values, meta ),
                                                   threshold,
                                                   mapper );

        assertTrue( sliced.getRawData().equals( expectedPairs ) );

        //Test baseline
        DiscreteProbabilityPairs slicedWithBaseline =
                Slicer.toDiscreteProbabilityPairs( EnsemblePairs.of( values, values, meta, meta ),
                                                   threshold,
                                                   mapper );
        assertTrue( slicedWithBaseline.getRawData().equals( expectedPairs ) );
        assertTrue( slicedWithBaseline.getBaselineData().getRawData().equals( expectedPairs ) );
    }

    /**
     * Tests the {@link Slicer#toDiscreteProbabilityPair(EnsemblePair, Threshold)}.
     */

    @Test
    public void testTransformEnsemblePairToDiscreteProbabilityPair()
    {
        EnsemblePair a = EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5 } );
        EnsemblePair b = EnsemblePair.of( 0, new double[] { 1, 2, 2, 3, 3 } );
        EnsemblePair c = EnsemblePair.of( 3, new double[] { 3, 3, 3, 3, 3 } );
        EnsemblePair d = EnsemblePair.of( 4, new double[] { 4, 4, 4, 4, 4 } );
        EnsemblePair e = EnsemblePair.of( 0, new double[] { 1, 2, 3, 4, 5 } );
        EnsemblePair f = EnsemblePair.of( 5, new double[] { 1, 1, 6, 6, 50 } );
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> mapper =
                Slicer::toDiscreteProbabilityPair;
        assertTrue( mapper.apply( a, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( mapper.apply( b, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( mapper.apply( c, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( mapper.apply( d, threshold ).equals( DiscreteProbabilityPair.of( 1.0, 1.0 ) ) );
        assertTrue( mapper.apply( e, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( mapper.apply( f, threshold ).equals( DiscreteProbabilityPair.of( 1.0, 3.0 / 5.0 ) ) );
    }

    /**
     * Tests the {@link Slicer#getQuantileFunction(double[])}.
     */

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
        exception.expect( IllegalArgumentException.class );
        qFA.applyAsDouble( -0.1 );
        qFA.applyAsDouble( 1.1 );
    }

    /**
     * Tests the {@link Slicer#getQuantileFromProbability(Threshold, double[], Integer)}.
     */

    @Test
    public void testGetQuantileFromProbability()
    {
        double[] sorted = new double[] { 1.5, 4.9, 6.3, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2 };
        double[] sortedSecond = new double[] { 1.5 };
        double tA = 0.0;
        double tB = 1.0;
        double tC = 7.0 / 11.0;
        double tD = ( 8.0 + ( ( 5005.0 - 2009.8 ) / ( 7001.4 - 2009.8 ) ) ) / 11.0;
        double[] tE = new double[] { 0.25, 0.5 };
        double tF = 8.0 / 11.0;
        double tG = 0.01;

        Threshold testA = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tA ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Threshold testB = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tB ),
                                                            Operator.LESS,
                                                            ThresholdDataType.LEFT );
        Threshold testC = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tC ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Threshold testD = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tD ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Threshold testE = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tE[0], tE[1] ),
                                                            Operator.BETWEEN,
                                                            ThresholdDataType.LEFT );
        Threshold testF = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tF ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Threshold testG = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( tG ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        Threshold expectedA = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1.5 ),
                                                             OneOrTwoDoubles.of( tA ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Threshold expectedB = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 17897.2 ),
                                                             OneOrTwoDoubles.of( tB ),
                                                             Operator.LESS,
                                                             ThresholdDataType.LEFT );
        Threshold expectedC = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1647.1818181818185 ),
                                                             OneOrTwoDoubles.of( tC ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Threshold expectedD = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 8924.920568373052 ),
                                                             OneOrTwoDoubles.of( tD ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Threshold expectedE = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 6.3,
                                                                                 433.9 ),
                                                             OneOrTwoDoubles.of( tE[0],
                                                                                 tE[1] ),
                                                             Operator.BETWEEN,
                                                             ThresholdDataType.LEFT );
        Threshold expectedF = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1.5 ),
                                                             OneOrTwoDoubles.of( tF ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );
        Threshold expectedG = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( 1.5 ),
                                                             OneOrTwoDoubles.of( tG ),
                                                             Operator.GREATER,
                                                             ThresholdDataType.LEFT );

        //Test for equality
        assertTrue( Slicer.getQuantileFromProbability( testA, sorted ).equals( expectedA ) );
        assertTrue( Slicer.getQuantileFromProbability( testB, sorted ).equals( expectedB ) );
        assertTrue( Slicer.getQuantileFromProbability( testC, sorted ).equals( expectedC ) );
        assertTrue( Slicer.getQuantileFromProbability( testD, sorted ).equals( expectedD ) );
        assertTrue( Slicer.getQuantileFromProbability( testE, sorted ).equals( expectedE ) );
        assertTrue( Slicer.getQuantileFromProbability( testF, sortedSecond ).equals( expectedF ) );
        assertTrue( Slicer.getQuantileFromProbability( testG, sorted ).equals( expectedG ) );

        //Check exceptional cases
        exception.expect( NullPointerException.class );

        Slicer.getQuantileFromProbability( null, sorted );
        Slicer.getQuantileFromProbability( testA, null );
        Slicer.getQuantileFromProbability( testA, new double[] {} );
    }

    /**
     * Tests the {@link Slicer#ofSingleValuedPairMapper(EnsemblePair)}.
     */

    @Test
    public void testTransformPair()
    {
        EnsemblePair a = EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5 } );
        EnsemblePair b = EnsemblePair.of( 0, new double[] { 1, 2, 2, 3, 3 } );
        EnsemblePair c = EnsemblePair.of( 3, new double[] { 3, 3, 3, 3, 3 } );
        EnsemblePair d = EnsemblePair.of( 4, new double[] { 4, 4, 4, 4, 4 } );
        EnsemblePair e = EnsemblePair.of( 0, new double[] { 1, 2, 3, 4, 5 } );
        EnsemblePair f = EnsemblePair.of( 5, new double[] { 1, 1, 6, 6, 50 } );
        Function<EnsemblePair, SingleValuedPair> mapper =
                Slicer.ofSingleValuedPairMapper( vector -> vector[0] );
        assertTrue( mapper.apply( a ).equals( SingleValuedPair.of( 3, 1 ) ) );
        assertTrue( mapper.apply( b ).equals( SingleValuedPair.of( 0, 1 ) ) );
        assertTrue( mapper.apply( c ).equals( SingleValuedPair.of( 3, 3 ) ) );
        assertTrue( mapper.apply( d ).equals( SingleValuedPair.of( 4, 4 ) ) );
        assertTrue( mapper.apply( e ).equals( SingleValuedPair.of( 0, 1 ) ) );
        assertTrue( mapper.apply( f ).equals( SingleValuedPair.of( 5, 1 ) ) );
    }

    /**
     * Tests the {@link Slicer#filterByRightSize(List)}.
     */

    @Test
    public void testFilterByRight()
    {
        List<EnsemblePair> input = new ArrayList<>();
        input.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        input.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        input.add( EnsemblePair.of( 1, new double[] { 1, 2, 3 } ) );
        input.add( EnsemblePair.of( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( EnsemblePair.of( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( EnsemblePair.of( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( EnsemblePair.of( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( EnsemblePair.of( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( EnsemblePair.of( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        //Slice
        Map<Integer, List<EnsemblePair>> sliced = Slicer.filterByRightSize( input );
        //Check the results
        assertTrue( "Expected three slices of data.", sliced.size() == 3 );
        assertTrue( "Expected the first slice to contain three pairs.", sliced.get( 3 ).size() == 3 );
        assertTrue( "Expected the second slice to contain five pairs.", sliced.get( 5 ).size() == 5 );
        assertTrue( "Expected the third slice to contain four pairs.", sliced.get( 6 ).size() == 4 );
    }

    /**
     * Tests the {@link Slicer#filterByMetricComponent(MetricOutputMapByTimeAndThreshold)}.
     */

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

    /**
     * Tests the {@link Slicer#filter(SingleValuedPairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterSingleValuedPairs()
    {
        List<SingleValuedPair> values = new ArrayList<>();
        values.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        values.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );
        values.add( SingleValuedPair.of( Double.NaN, Double.NaN ) );
        values.add( SingleValuedPair.of( 0, Double.NaN ) );
        values.add( SingleValuedPair.of( Double.NaN, 0 ) );

        List<SingleValuedPair> expectedValues = new ArrayList<>();
        expectedValues.add( SingleValuedPair.of( 1, 2.0 / 5.0 ) );
        expectedValues.add( SingleValuedPair.of( 1, 3.0 / 5.0 ) );
        expectedValues.add( SingleValuedPair.of( 1, 1.0 / 5.0 ) );

        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        SampleMetadata meta = SampleMetadata.of();
        SingleValuedPairs pairs = SingleValuedPairs.of( values, values, meta, meta, climatology );
        SingleValuedPairs sliced = Slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), Double::isFinite );

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
        SingleValuedPairs pairsNoBase = SingleValuedPairs.of( values, meta );
        SingleValuedPairs slicedNoBase = Slicer.filter( pairsNoBase, Slicer.leftAndRight( Double::isFinite ), null );

        assertTrue( slicedNoBase.getRawData().equals( expectedValues ) );
    }

    /**
     * Tests the {@link Slicer#filter(EnsemblePairs, Function)}.
     */

    @Test
    public void testFilterEnsemblePairs()
    {
        final List<EnsemblePair> values = new ArrayList<>();
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        values.add( EnsemblePair.of( Double.NaN, new double[] { Double.NaN, Double.NaN, Double.NaN } ) );
        values.add( EnsemblePair.of( 0, new double[] { Double.NaN, Double.NaN, Double.NaN } ) );
        values.add( EnsemblePair.of( 0, new double[] { Double.NaN, 2, 3, Double.NaN } ) );

        List<EnsemblePair> expectedValues = new ArrayList<>();
        expectedValues.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( EnsemblePair.of( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( EnsemblePair.of( 0, new double[] { 2, 3 } ) );

        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        SampleMetadata meta = SampleMetadata.of();
        EnsemblePairs pairs = EnsemblePairs.of( values, values, meta, meta, climatology );
        EnsemblePairs sliced = Slicer.filter( pairs, Slicer.leftAndEachOfRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( sliced.getRawData().equals( expectedValues ) );
        assertTrue( sliced.getBaselineData().getRawData().equals( expectedValues ) );
        assertTrue( Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( !Arrays.equals( Slicer.filter( pairs, Slicer.leftAndEachOfRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        EnsemblePairs pairsNoBase = EnsemblePairs.of( values, meta );
        EnsemblePairs slicedNoBase = Slicer.filter( pairsNoBase, Slicer.leftAndEachOfRight( Double::isFinite ), null );

        assertTrue( slicedNoBase.getRawData().equals( expectedValues ) );
    }

    /**
     * Tests the {@link Slicer#filter(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterTimeSeriesOfSingleValuedPairs()
    {
        //Build a time-series with three basis times 
        SortedSet<Event<SingleValuedPair>> first = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> second = new TreeSet<>();
        SortedSet<Event<SingleValuedPair>> third = new TreeSet<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( FIRST_TIME );
        first.add( Event.of( Instant.parse( SECOND_TIME ), SingleValuedPair.of( 1, 10 ) ) );
        first.add( Event.of( Instant.parse( THIRD_TIME ), SingleValuedPair.of( 2, 11 ) ) );
        first.add( Event.of( Instant.parse( FOURTH_TIME ), SingleValuedPair.of( 3, 12 ) ) );

        Instant secondBasisTime = Instant.parse( FIFTH_TIME );
        second.add( Event.of( Instant.parse( SIXTH_TIME ),
                              SingleValuedPair.of( 4, 13 ) ) );
        second.add( Event.of( Instant.parse( SEVENTH_TIME ),
                              SingleValuedPair.of( 5, 14 ) ) );
        second.add( Event.of( Instant.parse( EIGHTH_TIME ),
                              SingleValuedPair.of( 6, 15 ) ) );

        Instant thirdBasisTime = Instant.parse( NINTH_TIME );
        third.add( Event.of( Instant.parse( TENTH_TIME ), SingleValuedPair.of( 7, 16 ) ) );
        third.add( Event.of( Instant.parse( ELEVENTH_TIME ), SingleValuedPair.of( 8, 17 ) ) );
        third.add( Event.of( Instant.parse( TWELFTH_TIME ), SingleValuedPair.of( 9, 18 ) ) );
        SampleMetadata meta = SampleMetadata.of();

        //Add the time-series
        TimeSeriesOfSingleValuedPairs firstSeries =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeries( TimeSeries.of( firstBasisTime,
                                                                                first ) )
                                                 .addTimeSeries( TimeSeries.of( secondBasisTime,
                                                                                second ) )
                                                 .addTimeSeries( TimeSeries.of( thirdBasisTime,
                                                                                third ) )
                                                 .setMetadata( meta )
                                                 .build();

        // Filter all values where the left side is greater than 0
        TimeSeriesOfSingleValuedPairs firstResult =
                Slicer.filter( firstSeries,
                               TimeSeriesSlicer.anyOfLeftInTimeSeries( value -> value > 0 ),
                               null );

        assertTrue( firstResult.getRawData().equals( firstSeries.getRawData() ) );

        // Filter all values where the left side is greater than 3
        TimeSeriesOfSingleValuedPairs secondResult =
                Slicer.filter( firstSeries,
                               TimeSeriesSlicer.anyOfLeftInTimeSeries( value -> value > 3 ),
                               clim -> clim > 0 );

        List<Event<SingleValuedPair>> secondData = new ArrayList<>();
        secondResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( secondData::add ) );
        List<Event<SingleValuedPair>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertTrue( secondData.equals( secondBenchmark ) );

        // Add climatology for later
        VectorOfDoubles climatology = VectorOfDoubles.of( 1, 2, 3, 4, 5, Double.NaN );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( 1, 2, 3, 4, 5 );

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than 7
        TimeSeriesOfSingleValuedPairs thirdResult =
                Slicer.filter( firstSeries,
                               TimeSeriesSlicer.anyOfLeftAndAnyOfRightInTimeSeries( value -> value > 7 ),
                               null );

        List<Event<SingleValuedPair>> thirdData = new ArrayList<>();
        thirdResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( thirdData::add ) );
        List<Event<SingleValuedPair>> thirdBenchmark = new ArrayList<>();
        thirdBenchmark.addAll( third );

        assertTrue( thirdData.equals( thirdBenchmark ) );

        // Filter on climatology simultaneously
        TimeSeriesOfSingleValuedPairs fourthResult =
                Slicer.filter( b.build(),
                               TimeSeriesSlicer.anyOfLeftAndAnyOfRightInTimeSeries( value -> value > 7 ),
                               Double::isFinite );
        assertTrue( fourthResult.getClimatology().equals( climatologyExpected ) );

        // Also filter baseline data
        b.addTimeSeriesForBaseline( TimeSeries.of( firstBasisTime, first ) )
         .addTimeSeriesForBaseline( TimeSeries.of( secondBasisTime, second ) )
         .setMetadataForBaseline( meta );

        // Filter all values where both sides are greater than 4
        TimeSeriesOfSingleValuedPairs fifthResult =
                Slicer.filter( b.build(),
                               TimeSeriesSlicer.anyOfLeftInTimeSeries( value -> value > 4 ),
                               clim -> clim > 0 );

        List<Event<SingleValuedPair>> fifthData = new ArrayList<>();
        fifthResult.get().forEach( nextSeries -> nextSeries.getEvents().forEach( fifthData::add ) );

        // Same as second benchmark for main data
        assertTrue( fifthData.equals( secondBenchmark ) );

        // Baseline data
        List<Event<SingleValuedPair>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData()
                   .get()
                   .forEach( nextSeries -> nextSeries.getEvents().forEach( fifthDataBase::add ) );
        List<Event<SingleValuedPair>> fifthBenchmarkBase = new ArrayList<>();
        fifthBenchmarkBase.addAll( second );

        assertTrue( fifthDataBase.equals( fifthBenchmarkBase ) );

    }

    /**
     * Tests the {@link Slicer#filter(ListOfStatistics, java.util.function.Predicate)}.
     */

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

    /**
     * Tests the {@link Slicer#discover(ListOfStatistics, Function)}.
     */

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

    /**
     * Checks that a default quantile is returned by 
     * {@link Slicer#getQuantileFromProbability(Threshold, double[], Integer)} empty input.
     */

    @Test
    public void testGetQuantileFromProbabilityReturnsDefaultQuantileForEmptyInput()
    {
        double[] sorted = new double[0];
        Threshold testA = Threshold.ofProbabilityThreshold( OneOrTwoDoubles.of( 0.0 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );

        //Test for equality
        Threshold actual = Slicer.getQuantileFromProbability( testA, sorted );

        Threshold expected = Threshold.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NaN ),
                                                            OneOrTwoDoubles.of( 0.0 ),
                                                            Operator.GREATER,
                                                            ThresholdDataType.LEFT );
        assertEquals( expected, actual );
    }

    /**
     * Tests the {@link Slicer#filter( VectorOfDoubles DoublePredicate )}.
     */

    @Test
    public void testFilter()
    {
        VectorOfDoubles input = VectorOfDoubles.of( 1, 2, 3, 4, 5, 6, 7 );
        VectorOfDoubles expectedOutput = VectorOfDoubles.of( 1, 3, 5, 7 );
        DoublePredicate predicate = d -> ( d == 1 || d == 3 || d == 5 || d == 7 );
        VectorOfDoubles actualOutput = Slicer.filter( input, predicate );

        assertEquals( expectedOutput, actualOutput );
    }

    /**
     * Tests the {@link Slicer#filter(ListOfStatistics, java.util.function.Predicate)} produces an expected 
     * {@link NullPointerException} when the input list is null.
     */

    @Test
    public void testFilterListOfMetricOutputsWithNullListProducesNPE()
    {
        exception.expect( NullPointerException.class );

        Slicer.filter( (ListOfStatistics<Statistic<?>>) null, (Predicate<StatisticMetadata>) null );
    }

    /**
     * Tests the {@link Slicer#filter(ListOfStatistics, java.util.function.Predicate)} produces an expected 
     * {@link NullPointerException} when the input predicate is null.
     */

    @Test
    public void testFilterListOfMetricOutputsWithNullPredicateProducesNPE()
    {
        exception.expect( NullPointerException.class );

        Slicer.filter( ListOfStatistics.of( Arrays.asList() ), (Predicate<StatisticMetadata>) null );
    }

    /**
     * Tests the {@link Slicer#discover(ListOfStatistics, Function)} produces an expected 
     * {@link NullPointerException} when the input list is null.
     */

    @Test
    public void testDiscoverListOfMetricOutputsWithNullListProducesNPE()
    {
        exception.expect( NullPointerException.class );

        Slicer.discover( (ListOfStatistics<Statistic<?>>) null, (Function<Statistic<?>, ?>) null );
    }

    /**
     * Tests the {@link Slicer#discover(ListOfStatistics, Function)} produces an expected 
     * {@link NullPointerException} when the input predicate is null.
     */

    @Test
    public void testDiscoverListOfMetricOutputsWithNullFunctionProducesNPE()
    {
        exception.expect( NullPointerException.class );

        Slicer.discover( ListOfStatistics.of( Arrays.asList() ), (Function<Statistic<?>, ?>) null );
    }

}
