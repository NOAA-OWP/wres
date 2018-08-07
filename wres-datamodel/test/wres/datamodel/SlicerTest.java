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
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.inputs.pairs.DichotomousPair;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPair;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPair;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs.TimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs.TimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DataModelTestDataFactory;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.ListOfMetricOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link Slicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SlicerTest
{

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
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( SingleValuedPairs.of( values,
                                                                             Metadata.of() ) ),
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
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( EnsemblePairs.of( values,
                                                                         Metadata.of() ) ),
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
        assertTrue( "The right side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getRightSide( SingleValuedPairs.of( values,
                                                                              Metadata.of() ) ),
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
        Metadata meta = Metadata.of();
        SingleValuedPairs pairs = SingleValuedPairs.of( values, values, meta, meta, null );
        SingleValuedPairs sliced =
                Slicer.filter( pairs, Slicer.left( threshold::test ), clim -> threshold.test( clim ) );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        SingleValuedPairs pairsNoBase = SingleValuedPairs.of( values, meta );
        SingleValuedPairs slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.left( threshold::test ), clim -> threshold.test( clim ) );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
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
        Metadata meta = Metadata.of();
        EnsemblePairs pairs = EnsemblePairs.of( values, values, meta, meta, null );
        EnsemblePairs sliced =
                Slicer.filter( pairs, Slicer.leftVector( threshold::test ), clim -> threshold.test( clim ) );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        EnsemblePairs pairsNoBase = EnsemblePairs.of( values, meta );
        EnsemblePairs slicedNoBase =
                Slicer.filter( pairsNoBase, Slicer.leftVector( threshold::test ), clim -> threshold.test( clim ) );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( Slicer.getLeftSide( slicedNoBase ), expected ) );
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
        Metadata meta = Metadata.of();
        EnsemblePairs input = EnsemblePairs.of( values, values, meta, meta, null );
        Function<EnsemblePair, SingleValuedPair> mapper = ( in ) -> {
            return SingleValuedPair.of( in.getLeft(), Arrays.stream( in.getRight() ).average().getAsDouble() );
        };
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                Slicer.getRightSide( Slicer.toSingleValuedPairs( EnsemblePairs.of( values, meta ),
                                                                 mapper ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = Slicer.getRightSide( Slicer.toSingleValuedPairs( input, mapper ).getBaselineData() );
        assertTrue( "The transformed test data does not match the benchmark.", Arrays.equals( actualBase, expected ) );
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
        Metadata meta = Metadata.of();
        Function<SingleValuedPair, DichotomousPair> mapper = ( in ) -> {
            return DichotomousPair.of( in.getLeft() > 0, in.getRight() > 0 );
        };
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
        assertTrue( "The transformed test data does not match the benchmark.",
                    actualNoBase.getRawData().equals( expectedNoBase.getRawData() ) );
        //Test baseline
        DichotomousPairs actualBase =
                Slicer.toDichotomousPairs( SingleValuedPairs.of( values, values, meta, meta, null ),
                                           mapper );
        assertTrue( "The transformed test data does not match the benchmark.",
                    actualBase.getRawDataForBaseline().equals( expectedBase.getRawDataForBaseline() ) );
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
        Metadata meta = Metadata.of();
        Threshold threshold = Threshold.of( OneOrTwoDoubles.of( 3.0 ),
                                            Operator.GREATER,
                                            ThresholdDataType.LEFT );
        BiFunction<EnsemblePair, Threshold, DiscreteProbabilityPair> mapper =
                Slicer::toDiscreteProbabilityPair;

        List<DiscreteProbabilityPair> expectedPairs = new ArrayList<>();
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 1.0, 5.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) );
        expectedPairs.add( DiscreteProbabilityPair.of( 1.0, 3.0 / 5.0 ) );

        //Test without baseline
        DiscreteProbabilityPairs sliced =
                Slicer.toDiscreteProbabilityPairs( EnsemblePairs.of( values, meta ),
                                                   threshold,
                                                   mapper );

        assertTrue( "The transformed test data does not match the benchmark.",
                    sliced.getRawData().equals( expectedPairs ) );

        //Test baseline
        DiscreteProbabilityPairs slicedWithBaseline =
                Slicer.toDiscreteProbabilityPairs( EnsemblePairs.of( values, values, meta, meta ),
                                                   threshold,
                                                   mapper );
        assertTrue( "The transformed test data does not match the benchmark.",
                    slicedWithBaseline.getRawData().equals( expectedPairs ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    slicedWithBaseline.getRawDataForBaseline().equals( expectedPairs ) );
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
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( a, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( b, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( "The transfored pair does not match the benchmark",
                    mapper.apply( c, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( d, threshold ).equals( DiscreteProbabilityPair.of( 1.0, 5.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( e, threshold ).equals( DiscreteProbabilityPair.of( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( f, threshold ).equals( DiscreteProbabilityPair.of( 1.0, 3.0 / 5.0 ) ) );
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

        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    DataFactory.doubleEquals( qFA.applyAsDouble( testA ), expectedA, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    DataFactory.doubleEquals( qFA.applyAsDouble( testB ), expectedB, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    DataFactory.doubleEquals( qFA.applyAsDouble( testC ), expectedC, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    DataFactory.doubleEquals( qFA.applyAsDouble( testD ), expectedD, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    DataFactory.doubleEquals( qFB.applyAsDouble( testE ), expectedE, 7 ) );

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
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testA, sorted ).equals( expectedA ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testB, sorted ).equals( expectedB ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testC, sorted ).equals( expectedC ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testD, sorted ).equals( expectedD ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testE, sorted ).equals( expectedE ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testF, sortedSecond ).equals( expectedF ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    Slicer.getQuantileFromProbability( testG, sorted ).equals( expectedG ) );

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
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( a ).equals( SingleValuedPair.of( 3, 1 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( b ).equals( SingleValuedPair.of( 0, 1 ) ) );
        assertTrue( "The transfored pair does not match the benchmark",
                    mapper.apply( c ).equals( SingleValuedPair.of( 3, 3 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( d ).equals( SingleValuedPair.of( 4, 4 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( e ).equals( SingleValuedPair.of( 0, 1 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( f ).equals( SingleValuedPair.of( 5, 1 ) ) );
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
        MetricOutputMapByTimeAndThreshold<DoubleScoreOutput> toSlice =
                DataModelTestDataFactory.getVectorMetricOutputMapByLeadThresholdOne();
        Map<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>> sliced =
                Slicer.filterByMetricComponent( toSlice );

        //Check the results
        assertTrue( "Expected five slices of data.",
                    sliced.size() == toSlice.getMetadata().getMetricComponentID().getAllComponents().size() );
        sliced.forEach( ( key, value ) -> assertTrue( "Expected 638 elements in each slice.", value.size() == 638 ) );
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

        VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5 } );

        Metadata meta = Metadata.of();
        SingleValuedPairs pairs = SingleValuedPairs.of( values, values, meta, meta, climatology );
        SingleValuedPairs sliced = Slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getRawData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getRawDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( Slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        SingleValuedPairs pairsNoBase = SingleValuedPairs.of( values, meta );
        SingleValuedPairs slicedNoBase = Slicer.filter( pairsNoBase, Slicer.leftAndRight( Double::isFinite ), null );

        assertTrue( "The sliced data without a baseline does not match the benchmark.",
                    slicedNoBase.getRawData().equals( expectedValues ) );
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

        VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5 } );

        Metadata meta = Metadata.of();
        EnsemblePairs pairs = EnsemblePairs.of( values, values, meta, meta, climatology );
        EnsemblePairs sliced = Slicer.filter( pairs, Slicer.leftAndEachOfRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getRawData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getRawDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( Slicer.filter( pairs, Slicer.leftAndEachOfRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        EnsemblePairs pairsNoBase = EnsemblePairs.of( values, meta );
        EnsemblePairs slicedNoBase = Slicer.filter( pairsNoBase, Slicer.leftAndEachOfRight( Double::isFinite ), null );

        assertTrue( "The sliced data without a baseline does not match the benchmark.",
                    slicedNoBase.getRawData().equals( expectedValues ) );
    }

    /**
     * Tests the {@link Slicer#filter(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterTimeSeriesOfSingleValuedPairs()
    {
        //Build a time-series with three basis times 
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 10 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 11 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 12 ) ) );

        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), SingleValuedPair.of( 4, 13 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 5, 14 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), SingleValuedPair.of( 6, 15 ) ) );

        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 7, 16 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), SingleValuedPair.of( 8, 17 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 9, 18 ) ) );
        Metadata meta = Metadata.of();

        //Add the time-series
        TimeSeriesOfSingleValuedPairs firstSeries =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();

        // Filter all values where the left side is greater than 0
        TimeSeriesOfSingleValuedPairs firstResult =
                Slicer.filter( firstSeries,
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 0 ),
                               null );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    firstResult.getRawData().equals( firstSeries.getRawData() ) );

        // Filter all values where the left side is greater than 3
        TimeSeriesOfSingleValuedPairs secondResult =
                Slicer.filter( firstSeries,
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 3 ),
                               clim -> clim > 0 );

        List<Event<SingleValuedPair>> secondData = new ArrayList<>();
        secondResult.timeIterator().forEach( secondData::add );
        List<Event<SingleValuedPair>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    secondData.equals( secondBenchmark ) );

        // Add climatology for later
        VectorOfDoubles climatology = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = VectorOfDoubles.of( new double[] { 1, 2, 3, 4, 5 } );

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than 7
        TimeSeriesOfSingleValuedPairs thirdResult =
                Slicer.filter( firstSeries,
                               Slicer.anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( value -> value > 7 ),
                               null );

        List<Event<SingleValuedPair>> thirdData = new ArrayList<>();
        thirdResult.timeIterator().forEach( thirdData::add );
        List<Event<SingleValuedPair>> thirdBenchmark = new ArrayList<>();
        thirdBenchmark.addAll( third );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    thirdData.equals( thirdBenchmark ) );

        // Filter on climatology simultaneously
        TimeSeriesOfSingleValuedPairs fourthResult =
                Slicer.filter( b.build(),
                               Slicer.anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( value -> value > 7 ),
                               Double::isFinite );
        assertTrue( "The climatology in the fitlered time-series does not match the benchmark.",
                    fourthResult.getClimatology().equals( climatologyExpected ) );

        // Also filter baseline data
        b.addTimeSeriesDataForBaseline( firstBasisTime, first )
         .addTimeSeriesDataForBaseline( secondBasisTime, second )
         .setMetadataForBaseline( meta );

        // Filter all values where both sides are greater than 4
        TimeSeriesOfSingleValuedPairs fifthResult =
                Slicer.filter( b.build(),
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 4 ),
                               clim -> clim > 0 );

        List<Event<SingleValuedPair>> fifthData = new ArrayList<>();
        fifthResult.timeIterator().forEach( fifthData::add );

        // Same as second benchmark for main data
        assertTrue( "The filtered time-series does not match the benchmark.",
                    fifthData.equals( secondBenchmark ) );

        // Baseline data
        List<Event<SingleValuedPair>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData().timeIterator().forEach( fifthDataBase::add );
        List<Event<SingleValuedPair>> fifthBenchmarkBase = new ArrayList<>();
        fifthBenchmarkBase.addAll( second );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    fifthDataBase.equals( fifthBenchmarkBase ) );

    }

    /**
     * Tests the {@link Slicer#filterByBasisTime(TimeSeriesOfEnsemblePairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterEnsembleTimeSeriesByBasisTime()
    {
        //Build a time-series with three basis times 
        List<Event<EnsemblePair>> first = new ArrayList<>();
        List<Event<EnsemblePair>> second = new ArrayList<>();
        List<Event<EnsemblePair>> third = new ArrayList<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), EnsemblePair.of( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), EnsemblePair.of( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), EnsemblePair.of( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), EnsemblePair.of( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), EnsemblePair.of( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), EnsemblePair.of( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), EnsemblePair.of( 9, new double[] { 9 } ) ) );
        Metadata meta = Metadata.of();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeries<EnsemblePair> filtered =
                Slicer.filterByBasisTime( ts, a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getValue()
                            .equals( EnsemblePair.of( 4, new double[] { 4 } ) ) );

        //Check for empty output on none filter
        List<Instant> sliced = Slicer.filterByBasisTime( ts, a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) )
                                     .getBasisTimes();
        assertTrue( "Expected nullity on filtering basis times.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        Slicer.filterByBasisTime( (TimeSeriesOfEnsemblePairs) null, null );
        Slicer.filterByBasisTime( ts, null );
    }

    /**
     * Tests the {@link Slicer#filterByDuration(TimeSeriesOfEnsemblePairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterEnsembleTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<EnsemblePair>> first = new ArrayList<>();
        List<Event<EnsemblePair>> second = new ArrayList<>();
        List<Event<EnsemblePair>> third = new ArrayList<>();
        TimeSeriesOfEnsemblePairsBuilder b = new TimeSeriesOfEnsemblePairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), EnsemblePair.of( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), EnsemblePair.of( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), EnsemblePair.of( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), EnsemblePair.of( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), EnsemblePair.of( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), EnsemblePair.of( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), EnsemblePair.of( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), EnsemblePair.of( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), EnsemblePair.of( 9, new double[] { 9 } ) ) );
        Metadata meta = Metadata.of();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeriesOfEnsemblePairs filtered = Slicer.filterByBasisTime( ts, p -> p.equals( secondBasisTime ) );
        filtered = Slicer.filterByDuration( filtered, q -> q.equals( Duration.ofHours( 3 ) ) );

        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getValue()
                            .equals( EnsemblePair.of( 6, new double[] { 6 } ) ) );

        //Check for empty output on none filter
        @SuppressWarnings( "unlikely-arg-type" )
        Set<Duration> sliced = Slicer.filterByBasisTime( ts, p -> p.equals( Duration.ofHours( 4 ) ) ).getDurations();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        Slicer.filterByDuration( (TimeSeriesOfEnsemblePairs) null, null );
        Slicer.filterByDuration( ts, null );

    }


    /**
     * Tests the {@link Slicer#filterByBasisTime(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterSingleValuedTimeSeriesByBasisTime()
    {
        //Build a time-series with three basis times 
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
        Metadata meta = Metadata.of();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<SingleValuedPair> filtered = Slicer.filterByBasisTime( ts, a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( SingleValuedPair.of( 4, 4 ) ) );

        //Check for empty output on none filter
        List<Instant> sliced = Slicer.filterByBasisTime( ts, p -> p.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) )
                                     .getBasisTimes();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        Slicer.filterByDuration( (TimeSeriesOfSingleValuedPairs) null, null );
        Slicer.filterByDuration( ts, null );
    }

    /**
     * Tests the {@link Slicer#filterByDuration(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testSingleValuedTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<SingleValuedPair>> first = new ArrayList<>();
        List<Event<SingleValuedPair>> second = new ArrayList<>();
        List<Event<SingleValuedPair>> third = new ArrayList<>();
        TimeSeriesOfSingleValuedPairsBuilder b = new TimeSeriesOfSingleValuedPairsBuilder();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), SingleValuedPair.of( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), SingleValuedPair.of( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), SingleValuedPair.of( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), SingleValuedPair.of( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), SingleValuedPair.of( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), SingleValuedPair.of( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), SingleValuedPair.of( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), SingleValuedPair.of( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), SingleValuedPair.of( 9, 9 ) ) );
        Metadata meta = Metadata.of();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();

        //Iterate and test
        TimeSeriesOfSingleValuedPairs filtered = Slicer.filterByBasisTime( ts, p -> p.equals( secondBasisTime ) );
        filtered = Slicer.filterByDuration( filtered, q -> q.equals( Duration.ofHours( 3 ) ) );

        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( SingleValuedPair.of( 6, 6 ) ) );

        //Check for empty output on none filter
        Set<Duration> sliced = Slicer.filterByBasisTime( ts, p -> p.equals( Duration.ofHours( 4 ) ) ).getDurations();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        Slicer.filterByDuration( (TimeSeriesOfEnsemblePairs) null, null );
        Slicer.filterByDuration( ts, null );

    }

    /**
     * Tests the {@link Slicer#filter(ListOfMetricOutput, java.util.function.Predicate)}.
     */

    @Test
    public void testFilterListOfMetricOutputs()
    {
        // Populate a list of outputs
        MetricOutputMetadata metadata = MetricOutputMetadata.of( 0,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of(),
                                                                 MetricConstants.BIAS_FRACTION );

        TimeWindow windowOne =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 1 ) );

        TimeWindow windowTwo =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 2 ) );

        TimeWindow windowThree =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 3 ) );

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

        ListOfMetricOutput<DoubleScoreOutput> listOfOutputs =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowOne,
                                                                                                     thresholdOne ) ),
                                                      DoubleScoreOutput.of( 0.2,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowTwo,
                                                                                                     thresholdTwo ) ),
                                                      DoubleScoreOutput.of( 0.3,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowThree,
                                                                                                     thresholdThree ) ) ),
                                       metadata );

        // Filter by the first lead time and the last lead time and threshold
        Predicate<MetricOutputMetadata> filter = meta -> meta.getTimeWindow().equals( windowOne )
                                                         || ( meta.getTimeWindow().equals( windowThree )
                                                              && meta.getThresholds().equals( thresholdThree ) );

        ListOfMetricOutput<DoubleScoreOutput> actualOutput = Slicer.filter( listOfOutputs, filter );

        ListOfMetricOutput<DoubleScoreOutput> expectedOutput =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowOne,
                                                                                                     thresholdOne ) ),
                                                      DoubleScoreOutput.of( 0.3,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowThree,
                                                                                                     thresholdThree ) ) ),
                                       metadata );

        assertEquals( actualOutput, expectedOutput );

    }

    /**
     * Tests the {@link Slicer#discover(ListOfMetricOutput, Function)}.
     */

    @Test
    public void testDiscoverListOfMetricOutputs()
    {
        // Populate a list of outputs
        MetricOutputMetadata metadata = MetricOutputMetadata.of( 0,
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of(),
                                                                 MetricConstants.BIAS_FRACTION );

        TimeWindow windowOne =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 1 ) );

        TimeWindow windowTwo =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.VALID_TIME, Duration.ofHours( 2 ) );

        TimeWindow windowThree =
                TimeWindow.of( Instant.MIN, Instant.MAX, ReferenceTime.ISSUE_TIME, Duration.ofHours( 2 ) );

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

        ListOfMetricOutput<DoubleScoreOutput> listOfOutputs =
                ListOfMetricOutput.of( Arrays.asList( DoubleScoreOutput.of( 0.1,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowOne,
                                                                                                     thresholdOne ) ),
                                                      DoubleScoreOutput.of( 0.2,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowTwo,
                                                                                                     thresholdTwo ) ),
                                                      DoubleScoreOutput.of( 0.3,
                                                                            MetricOutputMetadata.of( metadata,
                                                                                                     windowThree,
                                                                                                     thresholdThree ) ) ),
                                       metadata );

        // Discover the metrics available
        Set<MetricConstants> actualOutputOne =
                Slicer.discover( listOfOutputs, next -> next.getMetadata().getMetricID() );
        Set<MetricConstants> expectedOutputOne = Collections.singleton( MetricConstants.BIAS_FRACTION );

        assertEquals( actualOutputOne, expectedOutputOne );

        // Discover the unique time windows available
        Set<TimeWindow> actualOutputTwo = Slicer.discover( listOfOutputs, next -> next.getMetadata().getTimeWindow() );
        Set<TimeWindow> expectedOutputTwo = new TreeSet<>( Arrays.asList( windowOne, windowTwo, windowThree ) );

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
                                 next -> Pair.of( next.getMetadata().getTimeWindow().getEarliestLeadTime(),
                                                  next.getMetadata().getTimeWindow().getLatestLeadTime() ) );

        Set<Pair<Duration, Duration>> expectedOutputFour =
                new TreeSet<>( Arrays.asList( Pair.of( Duration.ofHours( 1 ), Duration.ofHours( 1 ) ),
                                              Pair.of( Duration.ofHours( 2 ), Duration.ofHours( 2 ) ) ) );

        assertEquals( actualOutputFour, expectedOutputFour );        

    }

}
