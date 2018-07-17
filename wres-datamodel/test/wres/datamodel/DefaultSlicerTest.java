package wres.datamodel;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.inputs.pairs.SafeTimeSeriesOfEnsemblePairs.SafeTimeSeriesOfEnsemblePairsBuilder;
import wres.datamodel.inputs.pairs.SafeTimeSeriesOfSingleValuedPairs.SafeTimeSeriesOfSingleValuedPairsBuilder;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.thresholds.Threshold;
import wres.datamodel.thresholds.ThresholdConstants.Operator;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdDataType;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;

/**
 * Tests the {@link DefaultSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DefaultSlicerTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Slicer.
     */

    Slicer slicer = DefaultSlicer.getInstance();

    /**
     * Tests the {@link Slicer#getLeftSide(SingleValuedPairs)}.
     */

    @Test
    public void testGetLeftSideSingleValued()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 0.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( metIn.ofSingleValuedPairs( values,
                                                                                  metIn.getMetadataFactory()
                                                                                       .getMetadata() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#getLeftSide(EnsemblePairs)}.
     */

    @Test
    public void testGetLeftSideEnsemble()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        double[] expected = new double[] { 0, 0, 1, 1, 0, 1 };
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( metIn.ofEnsemblePairs( values,
                                                                              metIn.getMetadataFactory()
                                                                                   .getMetadata() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#getRightSide(SingleValuedPairs)}.
     */

    @Test
    public void testGetRightSide()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 0.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 3.0 / 5.0, 1.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0, 0.0 / 5.0, 1.0 / 5.0 };
        assertTrue( "The right side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getRightSide( metIn.ofSingleValuedPairs( values,
                                                                                   metIn.getMetadataFactory()
                                                                                        .getMetadata() ) ),
                                   expected ) );
    }

    /**
     * Tests the {@link Slicer#filter(SingleValuedPairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterSingleValuedPairsByLeft()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 0.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 1.0 / 5.0 ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 0.0 ),
                                                 Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        SingleValuedPairs pairs = metIn.ofSingleValuedPairs( values, values, meta, meta, null );
        SingleValuedPairs sliced =
                slicer.filter( pairs, Slicer.left( threshold::test ), clim -> threshold.test( clim ) );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        SingleValuedPairs pairsNoBase = metIn.ofSingleValuedPairs( values, meta );
        SingleValuedPairs slicedNoBase =
                slicer.filter( pairsNoBase, Slicer.left( threshold::test ), clim -> threshold.test( clim ) );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    /**
     * Tests the {@link Slicer#filter(EnsemblePairs, java.util.function.Predicate, java.util.function.DoublePredicate)}.
     */

    @Test
    public void testFilterEnsemblePairsByLeft()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        double[] expected = new double[] { 1, 1, 1 };
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 0.0 ),
                                                 Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs pairs = metIn.ofEnsemblePairs( values, values, meta, meta, null );
        EnsemblePairs sliced =
                slicer.filter( pairs, Slicer.leftVector( threshold::test ), clim -> threshold.test( clim ) );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        EnsemblePairs pairsNoBase = metIn.ofEnsemblePairs( values, meta );
        EnsemblePairs slicedNoBase =
                slicer.filter( pairsNoBase, Slicer.leftVector( threshold::test ), clim -> threshold.test( clim ) );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( slicedNoBase ), expected ) );
    }

    /**
     * Tests the {@link Slicer#toSingleValuedPairs(EnsemblePairs, Function)}.
     */

    @Test
    public void testTransformEnsemblePairsToSingleValuedPairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( metIn.pairOf( 0, new double[] { 6, 7, 8, 9, 10 } ) );
        values.add( metIn.pairOf( 1, new double[] { 11, 12, 13, 14, 15 } ) );
        values.add( metIn.pairOf( 1, new double[] { 16, 17, 18, 19, 20 } ) );
        values.add( metIn.pairOf( 0, new double[] { 21, 22, 23, 24, 25 } ) );
        values.add( metIn.pairOf( 1, new double[] { 26, 27, 28, 29, 30 } ) );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs input = metIn.ofEnsemblePairs( values, values, meta, meta, null );
        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper = ( in ) -> {
            return metIn.pairOf( in.getItemOne(), Arrays.stream( in.getItemTwo() ).average().getAsDouble() );
        };
        double[] expected = new double[] { 3.0, 8.0, 13.0, 18.0, 23.0, 28.0 };
        //Test without baseline
        double[] actualNoBase =
                slicer.getRightSide( slicer.toSingleValuedPairs( metIn.ofEnsemblePairs( values, meta ), mapper ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = slicer.getRightSide( slicer.toSingleValuedPairs( input, mapper ).getBaselineData() );
        assertTrue( "The transformed test data does not match the benchmark.", Arrays.equals( actualBase, expected ) );
    }

    /**
     * Tests the {@link Slicer#toDichotomousPairs(SingleValuedPairs, Function)}. 
     */

    @Test
    public void testTransformSingleValuedPairsToDichotomousPairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 0, 0.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 1.0 / 5.0 ) );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        Function<PairOfDoubles, PairOfBooleans> mapper = ( in ) -> {
            return metIn.pairOf( in.getItemOne() > 0, in.getItemTwo() > 0 );
        };
        final List<PairOfBooleans> expectedValues = new ArrayList<>();
        expectedValues.add( metIn.pairOf( false, true ) );
        expectedValues.add( metIn.pairOf( false, true ) );
        expectedValues.add( metIn.pairOf( true, true ) );
        expectedValues.add( metIn.pairOf( true, true ) );
        expectedValues.add( metIn.pairOf( false, false ) );
        expectedValues.add( metIn.pairOf( true, true ) );
        DichotomousPairs expectedNoBase = metIn.ofDichotomousPairsFromAtomic( expectedValues, meta );
        DichotomousPairs expectedBase = metIn.ofDichotomousPairsFromAtomic( expectedValues,
                                                                            expectedValues,
                                                                            meta,
                                                                            meta,
                                                                            null );

        //Test without baseline
        DichotomousPairs actualNoBase = slicer.toDichotomousPairs( metIn.ofSingleValuedPairs( values, meta ), mapper );
        assertTrue( "The transformed test data does not match the benchmark.",
                    actualNoBase.getRawData().equals( expectedNoBase.getRawData() ) );
        //Test baseline
        DichotomousPairs actualBase =
                slicer.toDichotomousPairs( metIn.ofSingleValuedPairs( values, values, meta, meta, null ),
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
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 2, 3, 3 } ) );
        values.add( metIn.pairOf( 3, new double[] { 3, 3, 3, 3, 3 } ) );
        values.add( metIn.pairOf( 4, new double[] { 4, 4, 4, 4, 4 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } ) );
        values.add( metIn.pairOf( 5, new double[] { 1, 1, 6, 6, 50 } ) );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 3.0 ),
                                                 Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::toDiscreteProbabilityPair;
        double[] expectedLeft = new double[] { 0.0, 0.0, 0.0, 1.0, 0.0, 1.0 };
        double[] expectedRight = new double[] { 2.0 / 5.0, 0.0 / 5.0, 0.0 / 5.0, 5.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0 };

        //Test without baseline
        double[] actualNoBaseLeft = slicer.getLeftSide( slicer.toDiscreteProbabilityPairs( metIn.ofEnsemblePairs( values, meta ),
                                                                          threshold,
                                                                          mapper ) );
        double[] actualNoBaseRight = slicer.getRightSide( slicer.toDiscreteProbabilityPairs( metIn.ofEnsemblePairs( values, meta ),
                                                                            threshold,
                                                                            mapper ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBaseLeft, expectedLeft ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBaseRight, expectedRight ) );

        //Test baseline
        double[] actualBaseLeft = slicer.getLeftSide( slicer.toDiscreteProbabilityPairs( metIn.ofEnsemblePairs( values,
                                                                                               values,
                                                                                               meta,
                                                                                               meta,
                                                                                               null ),
                                                                        threshold,
                                                                        mapper )
                                                            .getBaselineData() );
        double[] actualBaseRight = slicer.getRightSide( slicer.toDiscreteProbabilityPairs( metIn.ofEnsemblePairs( values,
                                                                                                 values,
                                                                                                 meta,
                                                                                                 meta,
                                                                                                 null ),
                                                                          threshold,
                                                                          mapper )
                                                              .getBaselineData() );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualBaseLeft, expectedLeft ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualBaseRight, expectedRight ) );
    }

    /**
     * Tests the {@link Slicer#toDiscreteProbabilityPair(PairOfDoubleAndVectorOfDoubles, Threshold)}.
     */

    @Test
    public void testTransformEnsemblePairToDiscreteProbabilityPair()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf( 0, new double[] { 1, 2, 2, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf( 3, new double[] { 3, 3, 3, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf( 4, new double[] { 4, 4, 4, 4, 4 } );
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf( 5, new double[] { 1, 1, 6, 6, 50 } );
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 3.0 ),
                                                 Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::toDiscreteProbabilityPair;
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( a, threshold ).equals( metIn.pairOf( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( b, threshold ).equals( metIn.pairOf( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( "The transfored pair does not match the benchmark",
                    mapper.apply( c, threshold ).equals( metIn.pairOf( 0.0, 0.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( d, threshold ).equals( metIn.pairOf( 1.0, 5.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( e, threshold ).equals( metIn.pairOf( 0.0, 2.0 / 5.0 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( f, threshold ).equals( metIn.pairOf( 1.0, 3.0 / 5.0 ) ) );
    }

    /**
     * Tests the {@link Slicer#getQuantileFunction(double[])}.
     */

    @Test
    public void testGetInverseCumulativeProbability()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
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
        DoubleUnaryOperator qFA = slicer.getQuantileFunction( sorted );
        DoubleUnaryOperator qFB = slicer.getQuantileFunction( sortedB );

        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    metIn.doubleEquals( qFA.applyAsDouble( testA ), expectedA, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    metIn.doubleEquals( qFA.applyAsDouble( testB ), expectedB, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    metIn.doubleEquals( qFA.applyAsDouble( testC ), expectedC, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    metIn.doubleEquals( qFA.applyAsDouble( testD ), expectedD, 7 ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    metIn.doubleEquals( qFB.applyAsDouble( testE ), expectedE, 7 ) );

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
        DataFactory metIn = DefaultDataFactory.getInstance();
        double[] sorted = new double[] { 1.5, 4.9, 6.3, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2 };
        double[] sortedSecond = new double[] { 1.5 };
        double tA = 0.0;
        double tB = 1.0;
        double tC = 7.0 / 11.0;
        double tD = ( 8.0 + ( ( 5005.0 - 2009.8 ) / ( 7001.4 - 2009.8 ) ) ) / 11.0;
        double[] tE = new double[] { 0.25, 0.5 };
        double tF = 8.0 / 11.0;
        double tG = 0.01;

        Threshold testA = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tA ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testB = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tB ),
                                                        Operator.LESS,
                                                        ThresholdDataType.LEFT );
        Threshold testC = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tC ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testD = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tD ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testE = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tE[0], tE[1] ),
                                                        Operator.BETWEEN,
                                                        ThresholdDataType.LEFT );
        Threshold testF = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tF ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testG = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tG ),
                                                        Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold expectedA = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.5 ),
                                                         SafeOneOrTwoDoubles.of( tA ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );
        Threshold expectedB = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 17897.2 ),
                                                         SafeOneOrTwoDoubles.of( tB ),
                                                         Operator.LESS,
                                                         ThresholdDataType.LEFT );
        Threshold expectedC = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1647.1818181818185 ),
                                                         SafeOneOrTwoDoubles.of( tC ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );
        Threshold expectedD = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 8924.920568373052 ),
                                                         SafeOneOrTwoDoubles.of( tD ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );
        Threshold expectedE = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 6.3,
                                                                                 433.9 ),
                                                         SafeOneOrTwoDoubles.of( tE[0],
                                                                                 tE[1] ),
                                                         Operator.BETWEEN,
                                                         ThresholdDataType.LEFT );
        Threshold expectedF = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.5 ),
                                                         SafeOneOrTwoDoubles.of( tF ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );
        Threshold expectedG = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.5 ),
                                                         SafeOneOrTwoDoubles.of( tG ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );

        //Test for equality
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testA, sorted ).equals( expectedA ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testB, sorted ).equals( expectedB ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testC, sorted ).equals( expectedC ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testD, sorted ).equals( expectedD ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testE, sorted ).equals( expectedE ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testF, sortedSecond ).equals( expectedF ) );
        assertTrue( "The inverse cumulative probability does not match the benchmark",
                    slicer.getQuantileFromProbability( testG, sorted ).equals( expectedG ) );

        //Check exceptional cases
        exception.expect( NullPointerException.class );

        slicer.getQuantileFromProbability( null, sorted );
        slicer.getQuantileFromProbability( testA, null );
        slicer.getQuantileFromProbability( testA, new double[] {} );
    }

    /**
     * Tests the {@link Slicer#ofSingleValuedPairMapper(PairOfDoubleAndVectorOfDoubles)}.
     */

    @Test
    public void testTransformPair()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf( 0, new double[] { 1, 2, 2, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf( 3, new double[] { 3, 3, 3, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf( 4, new double[] { 4, 4, 4, 4, 4 } );
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf( 5, new double[] { 1, 1, 6, 6, 50 } );
        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper =
                metIn.getSlicer().ofSingleValuedPairMapper( vector -> vector[0] );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( a ).equals( metIn.pairOf( 3, 1 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( b ).equals( metIn.pairOf( 0, 1 ) ) );
        assertTrue( "The transfored pair does not match the benchmark",
                    mapper.apply( c ).equals( metIn.pairOf( 3, 3 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( d ).equals( metIn.pairOf( 4, 4 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( e ).equals( metIn.pairOf( 0, 1 ) ) );
        assertTrue( "The transformed pair does not match the benchmark",
                    mapper.apply( f ).equals( metIn.pairOf( 5, 1 ) ) );
    }

    /**
     * Tests the {@link Slicer#filterByRightSize(List)}.
     */

    @Test
    public void testFilterByRight()
    {
        List<PairOfDoubleAndVectorOfDoubles> input = new ArrayList<>();
        DataFactory metIn = DefaultDataFactory.getInstance();
        input.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        input.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        input.add( metIn.pairOf( 1, new double[] { 1, 2, 3 } ) );
        input.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( metIn.pairOf( 2, new double[] { 1, 2, 3, 4, 5 } ) );
        input.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        input.add( metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5, 6 } ) );
        //Slice
        Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliced = slicer.filterByRightSize( input );
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
                slicer.filterByMetricComponent( toSlice );

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
        DataFactory metIn = DefaultDataFactory.getInstance();
        List<PairOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        values.add( metIn.pairOf( 1, 1.0 / 5.0 ) );
        values.add( metIn.pairOf( Double.NaN, Double.NaN ) );
        values.add( metIn.pairOf( 0, Double.NaN ) );
        values.add( metIn.pairOf( Double.NaN, 0 ) );

        List<PairOfDoubles> expectedValues = new ArrayList<>();
        expectedValues.add( metIn.pairOf( 1, 2.0 / 5.0 ) );
        expectedValues.add( metIn.pairOf( 1, 3.0 / 5.0 ) );
        expectedValues.add( metIn.pairOf( 1, 1.0 / 5.0 ) );

        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5 } );

        Metadata meta = metIn.getMetadataFactory().getMetadata();
        SingleValuedPairs pairs = metIn.ofSingleValuedPairs( values, values, meta, meta, climatology );
        SingleValuedPairs sliced = slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getRawData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getRawDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( slicer.filter( pairs, Slicer.leftAndRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        SingleValuedPairs pairsNoBase = metIn.ofSingleValuedPairs( values, meta );
        SingleValuedPairs slicedNoBase = slicer.filter( pairsNoBase, Slicer.leftAndRight( Double::isFinite ), null );

        assertTrue( "The sliced data without a baseline does not match the benchmark.",
                    slicedNoBase.getRawData().equals( expectedValues ) );
    }

    /**
     * Tests the {@link Slicer#filter(EnsemblePairs, Function)}.
     */

    @Test
    public void testFilterEnsemblePairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        values.add( metIn.pairOf( Double.NaN, new double[] { Double.NaN, Double.NaN, Double.NaN } ) );
        values.add( metIn.pairOf( 0, new double[] { Double.NaN, Double.NaN, Double.NaN } ) );
        values.add( metIn.pairOf( 0, new double[] { Double.NaN, 2, 3, Double.NaN } ) );

        List<PairOfDoubleAndVectorOfDoubles> expectedValues = new ArrayList<>();
        expectedValues.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        expectedValues.add( metIn.pairOf( 0, new double[] { 2, 3 } ) );

        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5 } );

        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs pairs = metIn.ofEnsemblePairs( values, values, meta, meta, climatology );
        EnsemblePairs sliced = slicer.filter( pairs, slicer.leftAndEachOfRight( Double::isFinite ), Double::isFinite );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getRawData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getRawDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( slicer.filter( pairs, slicer.leftAndEachOfRight( Double::isFinite ), null )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getRawData().equals( values ) );
        //Test without baseline or climatology
        EnsemblePairs pairsNoBase = metIn.ofEnsemblePairs( values, meta );
        EnsemblePairs slicedNoBase = slicer.filter( pairsNoBase, slicer.leftAndEachOfRight( Double::isFinite ), null );

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
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();

        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 10 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 11 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 12 ) ) );

        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 13 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 14 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 15 ) ) );

        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 16 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 17 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 18 ) ) );
        Metadata meta = metaFac.getMetadata();

        //Add the time-series
        TimeSeriesOfSingleValuedPairs firstSeries =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();

        // Filter all values where the left side is greater than 0
        TimeSeriesOfSingleValuedPairs firstResult =
                slicer.filter( firstSeries,
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 0 ),
                               null );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    firstResult.getRawData().equals( firstSeries.getRawData() ) );

        // Filter all values where the left side is greater than 3
        TimeSeriesOfSingleValuedPairs secondResult =
                slicer.filter( firstSeries,
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 3 ),
                               clim -> clim > 0 );

        List<Event<PairOfDoubles>> secondData = new ArrayList<>();
        secondResult.timeIterator().forEach( secondData::add );
        List<Event<PairOfDoubles>> secondBenchmark = new ArrayList<>();
        secondBenchmark.addAll( second );
        secondBenchmark.addAll( third );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    secondData.equals( secondBenchmark ) );

        // Add climatology for later
        VectorOfDoubles climatology = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5, Double.NaN } );
        VectorOfDoubles climatologyExpected = metIn.vectorOf( new double[] { 1, 2, 3, 4, 5 } );

        b.setClimatology( climatology );

        // Filter all values where the left and right sides are both greater than 7
        TimeSeriesOfSingleValuedPairs thirdResult =
                slicer.filter( firstSeries,
                               Slicer.anyOfLeftAndAnyOfRightInTimeSeriesOfSingleValuedPairs( value -> value > 7 ),
                               null );

        List<Event<PairOfDoubles>> thirdData = new ArrayList<>();
        thirdResult.timeIterator().forEach( thirdData::add );
        List<Event<PairOfDoubles>> thirdBenchmark = new ArrayList<>();
        thirdBenchmark.addAll( third );

        assertTrue( "The filtered time-series does not match the benchmark.",
                    thirdData.equals( thirdBenchmark ) );

        // Filter on climatology simultaneously
        TimeSeriesOfSingleValuedPairs fourthResult =
                slicer.filter( b.build(),
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
                slicer.filter( b.build(),
                               Slicer.anyOfLeftInTimeSeriesOfSingleValuedPairs( value -> value > 4 ),
                               clim -> clim > 0 );

        List<Event<PairOfDoubles>> fifthData = new ArrayList<>();
        fifthResult.timeIterator().forEach( fifthData::add );

        // Same as second benchmark for main data
        assertTrue( "The filtered time-series does not match the benchmark.",
                    fifthData.equals( secondBenchmark ) );

        // Baseline data
        List<Event<PairOfDoubles>> fifthDataBase = new ArrayList<>();
        fifthResult.getBaselineData().timeIterator().forEach( fifthDataBase::add );
        List<Event<PairOfDoubles>> fifthBenchmarkBase = new ArrayList<>();
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
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, new double[] { 9 } ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeries<PairOfDoubleAndVectorOfDoubles> filtered =
                slicer.filterByBasisTime( ts, a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getValue()
                            .equals( metIn.pairOf( 4, new double[] { 4 } ) ) );

        //Check for empty output on none filter
        List<Instant> sliced = slicer.filterByBasisTime( ts, a -> a.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) )
                                     .getBasisTimes();
        assertTrue( "Expected nullity on filtering basis times.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        slicer.filterByBasisTime( (TimeSeriesOfEnsemblePairs) null, null );
        slicer.filterByBasisTime( ts, null );
    }

    /**
     * Tests the {@link Slicer#filterByDuration(TimeSeriesOfEnsemblePairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterEnsembleTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubleAndVectorOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubleAndVectorOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfEnsemblePairsBuilder b = new SafeTimeSeriesOfEnsemblePairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, new double[] { 1 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, new double[] { 2 } ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, new double[] { 3 } ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, new double[] { 4 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, new double[] { 5 } ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, new double[] { 6 } ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, new double[] { 7 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, new double[] { 8 } ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, new double[] { 9 } ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfEnsemblePairs ts =
                (TimeSeriesOfEnsemblePairs) b.addTimeSeriesData( firstBasisTime, first )
                                             .addTimeSeriesData( secondBasisTime, second )
                                             .addTimeSeriesData( thirdBasisTime, third )
                                             .setMetadata( meta )
                                             .build();
        //Iterate and test
        TimeSeriesOfEnsemblePairs filtered = slicer.filterByBasisTime( ts, p -> p.equals( secondBasisTime ) );
        filtered = slicer.filterByDuration( filtered, q -> q.equals( Duration.ofHours( 3 ) ) );

        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator()
                            .iterator()
                            .next()
                            .getValue()
                            .equals( metIn.pairOf( 6, new double[] { 6 } ) ) );

        //Check for empty output on none filter
        Set<Duration> sliced = slicer.filterByBasisTime( ts, p -> p.equals( Duration.ofHours( 4 ) ) ).getDurations();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        slicer.filterByDuration( (TimeSeriesOfEnsemblePairs) null, null );
        slicer.filterByDuration( ts, null );

    }


    /**
     * Tests the {@link Slicer#filterByBasisTime(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testFilterSingleValuedTimeSeriesByBasisTime()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        //Iterate and test
        TimeSeries<PairOfDoubles> filtered = slicer.filterByBasisTime( ts, a -> a.equals( secondBasisTime ) );
        assertTrue( "Unexpected number of issue times in the filtered time-series.",
                    filtered.getBasisTimes().size() == 1 );
        assertTrue( "Unexpected issue time in the filtered time-series.",
                    filtered.getBasisTimes().get( 0 ).equals( secondBasisTime ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( metIn.pairOf( 4, 4 ) ) );

        //Check for empty output on none filter
        List<Instant> sliced = slicer.filterByBasisTime( ts, p -> p.equals( Instant.parse( "1985-01-04T00:00:00Z" ) ) )
                                     .getBasisTimes();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        slicer.filterByDuration( (TimeSeriesOfSingleValuedPairs) null, null );
        slicer.filterByDuration( ts, null );
    }

    /**
     * Tests the {@link Slicer#filterByDuration(TimeSeriesOfSingleValuedPairs, java.util.function.Predicate)} 
     * method.
     */

    @Test
    public void testSingleValuedTimeSeriesByDuration()
    {
        //Build a time-series with three basis times 
        List<Event<PairOfDoubles>> first = new ArrayList<>();
        List<Event<PairOfDoubles>> second = new ArrayList<>();
        List<Event<PairOfDoubles>> third = new ArrayList<>();
        SafeTimeSeriesOfSingleValuedPairsBuilder b = new SafeTimeSeriesOfSingleValuedPairsBuilder();
        DataFactory metIn = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = metIn.getMetadataFactory();
        Instant firstBasisTime = Instant.parse( "1985-01-01T00:00:00Z" );
        first.add( Event.of( Instant.parse( "1985-01-01T01:00:00Z" ), metIn.pairOf( 1, 1 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T02:00:00Z" ), metIn.pairOf( 2, 2 ) ) );
        first.add( Event.of( Instant.parse( "1985-01-01T03:00:00Z" ), metIn.pairOf( 3, 3 ) ) );
        Instant secondBasisTime = Instant.parse( "1985-01-02T00:00:00Z" );
        second.add( Event.of( Instant.parse( "1985-01-02T01:00:00Z" ), metIn.pairOf( 4, 4 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T02:00:00Z" ), metIn.pairOf( 5, 5 ) ) );
        second.add( Event.of( Instant.parse( "1985-01-02T03:00:00Z" ), metIn.pairOf( 6, 6 ) ) );
        Instant thirdBasisTime = Instant.parse( "1985-01-03T00:00:00Z" );
        third.add( Event.of( Instant.parse( "1985-01-03T01:00:00Z" ), metIn.pairOf( 7, 7 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T02:00:00Z" ), metIn.pairOf( 8, 8 ) ) );
        third.add( Event.of( Instant.parse( "1985-01-03T03:00:00Z" ), metIn.pairOf( 9, 9 ) ) );
        Metadata meta = metaFac.getMetadata();
        //Add the time-series
        TimeSeriesOfSingleValuedPairs ts =
                (TimeSeriesOfSingleValuedPairs) b.addTimeSeriesData( firstBasisTime, first )
                                                 .addTimeSeriesData( secondBasisTime, second )
                                                 .addTimeSeriesData( thirdBasisTime, third )
                                                 .setMetadata( meta )
                                                 .build();
        
        //Iterate and test
        TimeSeriesOfSingleValuedPairs filtered = slicer.filterByBasisTime( ts, p -> p.equals( secondBasisTime ) );
        filtered = slicer.filterByDuration( filtered, q -> q.equals( Duration.ofHours( 3 ) ) );

        assertTrue( "Unexpected number of durations in filtered time-series.", filtered.getDurations().size() == 1 );
        assertTrue( "Unexpected duration in the filtered time-series.",
                    filtered.getDurations().first().equals( Duration.ofHours( 3 ) ) );
        assertTrue( "Unexpected value in the filtered time-series.",
                    filtered.timeIterator().iterator().next().getValue().equals( metIn.pairOf( 6, 6 ) ) );
        
        //Check for empty output on none filter
        Set<Duration> sliced = slicer.filterByBasisTime( ts, p -> p.equals( Duration.ofHours( 4 ) ) ).getDurations();
        assertTrue( "Expected nullity on filtering durations.", sliced.isEmpty() );

        //Check exceptional cases
        exception.expect( NullPointerException.class );
        slicer.filterByDuration( (TimeSeriesOfEnsemblePairs) null, null );
        slicer.filterByDuration( ts, null );

    }

}
