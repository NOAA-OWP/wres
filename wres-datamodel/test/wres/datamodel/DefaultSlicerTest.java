package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;

import org.junit.Test;

import wres.datamodel.ThresholdConstants.Operator;
import wres.datamodel.ThresholdConstants.ThresholdDataType;
import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;

/**
 * Tests the {@link DefaultSlicer}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DefaultSlicerTest
{

    /**
     * Slicer.
     */

    Slicer slicer = DefaultSlicer.getInstance();

    /**
     * Tests the {@link Slicer#getLeftSide(SingleValuedPairs)}.
     */

    @Test
    public void test1GetLeftSideSingleValued()
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
    public void test2GetLeftSideEnsemble()
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
    public void test3GetRightSide()
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
     * Tests the {@link Slicer#filterByLeft(SingleValuedPairs, Threshold)}.
     * 
     * @throws MetricInputSliceException if the filtering fails
     */

    @Test
    public void test4FilterByLeft() throws MetricInputSliceException
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
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 0.0 ), Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        SingleValuedPairs pairs = metIn.ofSingleValuedPairs( values, values, meta, meta, null );
        SingleValuedPairs sliced = slicer.filterByLeft( pairs, threshold );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        SingleValuedPairs pairsNoBase = metIn.ofSingleValuedPairs( values, meta );
        SingleValuedPairs slicedNoBase = slicer.filterByLeft( pairsNoBase, threshold );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( slicedNoBase ), expected ) );
        //Test exception
        try
        {
            slicer.filterByLeft( pairs, metIn.ofThreshold( SafeOneOrTwoDoubles.of( 1.0 ), Operator.GREATER,
                                                           ThresholdDataType.LEFT ) );
            fail( "Expected an exception on attempting to return an empty subset." );
        }
        catch ( Exception e )
        {
        }

        //Test null return on baseline
        final List<PairOfDoubles> nullValuesBase = new ArrayList<>();
        nullValuesBase.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        nullValuesBase.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        nullValuesBase.add( metIn.pairOf( 0, 2.0 / 5.0 ) );
        nullValuesBase.add( metIn.pairOf( 0, 3.0 / 5.0 ) );
        nullValuesBase.add( metIn.pairOf( 0, 0.0 / 5.0 ) );
        nullValuesBase.add( metIn.pairOf( 0, 1.0 / 5.0 ) );
        SingleValuedPairs pairsNullBase = metIn.ofSingleValuedPairs( values, nullValuesBase, meta, meta, null );
        try
        {
            slicer.filterByLeft( pairsNullBase, threshold );
            fail( "Expected an exception on attempting to return an empty subset for the baseline." );
        }
        catch ( Exception e )
        {
        }
    }

    /**
     * Tests the {@link Slicer#filterByLeft(EnsemblePairs, Threshold)}.
     * 
     * @throws MetricInputSliceException if the filtering fails
     */

    @Test
    public void test5FilterByLeft() throws MetricInputSliceException
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
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 0.0 ), Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs pairs = metIn.ofEnsemblePairs( values, values, meta, meta, null );
        EnsemblePairs sliced = slicer.filterByLeft( pairs, threshold );
        //Test with baseline
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( sliced.getBaselineData() ), expected ) );
        //Test without baseline
        EnsemblePairs pairsNoBase = metIn.ofEnsemblePairs( values, meta );
        EnsemblePairs slicedNoBase = slicer.filterByLeft( pairsNoBase, threshold );
        assertTrue( "The left side of the test data does not match the benchmark.",
                    Arrays.equals( slicer.getLeftSide( slicedNoBase ), expected ) );
        //Test exception
        try
        {
            slicer.filterByLeft( pairs, metIn.ofThreshold( SafeOneOrTwoDoubles.of( 1.0 ), Operator.GREATER,
                                                           ThresholdDataType.LEFT ) );
            fail( "Expected an exception on attempting to return an empty subset." );
        }
        catch ( Exception e )
        {
        }

        //Test exception on baseline
        final List<PairOfDoubleAndVectorOfDoubles> nullValuesBase = new ArrayList<>();
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        nullValuesBase.add( metIn.pairOf( 0, new double[] { 1, 2, 3 } ) );
        EnsemblePairs pairsNullBase = metIn.ofEnsemblePairs( values, nullValuesBase, meta, meta, null );
        try
        {
            slicer.filterByLeft( pairsNullBase, threshold );
            fail( "Expected an exception on attempting to return an empty subset for the baseline." );
        }
        catch ( Exception e )
        {
        }
    }

    /**
     * Tests the {@link Slicer#transformPairs(EnsemblePairs, Function)}.
     */

    @Test
    public void test6TransformPairs()
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
                slicer.getRightSide( slicer.transformPairs( metIn.ofEnsemblePairs( values, meta ), mapper ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBase, expected ) );
        //Test baseline
        double[] actualBase = slicer.getRightSide( slicer.transformPairs( input, mapper ).getBaselineData() );
        assertTrue( "The transformed test data does not match the benchmark.", Arrays.equals( actualBase, expected ) );
    }

    /**
     * Tests the {@link Slicer#transformPairs(SingleValuedPairs, Function)}. 
     */

    @Test
    public void test7TransformPairs()
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
        DichotomousPairs actualNoBase = slicer.transformPairs( metIn.ofSingleValuedPairs( values, meta ), mapper );
        assertTrue( "The transformed test data does not match the benchmark.",
                    actualNoBase.getData().equals( expectedNoBase.getData() ) );
        //Test baseline
        DichotomousPairs actualBase =
                slicer.transformPairs( metIn.ofSingleValuedPairs( values, values, meta, meta, null ),
                                       mapper );
        assertTrue( "The transformed test data does not match the benchmark.",
                    actualBase.getDataForBaseline().equals( expectedBase.getDataForBaseline() ) );
    }

    /**
     * Tests the {@link Slicer#transformPairs(EnsemblePairs, Threshold, BiFunction)}.
     */

    @Test
    public void test8TransformPairs()
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
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 3.0 ), Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
        double[] expectedLeft = new double[] { 0.0, 0.0, 0.0, 1.0, 0.0, 1.0 };
        double[] expectedRight = new double[] { 2.0 / 5.0, 0.0 / 5.0, 0.0 / 5.0, 5.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0 };

        //Test without baseline
        double[] actualNoBaseLeft = slicer.getLeftSide( slicer.transformPairs( metIn.ofEnsemblePairs( values, meta ),
                                                                               threshold,
                                                                               mapper ) );
        double[] actualNoBaseRight = slicer.getRightSide( slicer.transformPairs( metIn.ofEnsemblePairs( values, meta ),
                                                                                 threshold,
                                                                                 mapper ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBaseLeft, expectedLeft ) );
        assertTrue( "The transformed test data does not match the benchmark.",
                    Arrays.equals( actualNoBaseRight, expectedRight ) );

        //Test baseline
        double[] actualBaseLeft = slicer.getLeftSide( slicer.transformPairs(
                                                                             metIn.ofEnsemblePairs( values,
                                                                                                    values,
                                                                                                    meta,
                                                                                                    meta,
                                                                                                    null ),
                                                                             threshold,
                                                                             mapper )
                                                            .getBaselineData() );
        double[] actualBaseRight = slicer.getRightSide( slicer.transformPairs(
                                                                               metIn.ofEnsemblePairs( values,
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
     * Tests the {@link Slicer#transformPair(PairOfDoubleAndVectorOfDoubles, Threshold)}.
     */

    @Test
    public void test9TransformPair()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf( 0, new double[] { 1, 2, 2, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf( 3, new double[] { 3, 3, 3, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf( 4, new double[] { 4, 4, 4, 4, 4 } );
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf( 5, new double[] { 1, 1, 6, 6, 50 } );
        Threshold threshold = metIn.ofThreshold( SafeOneOrTwoDoubles.of( 3.0 ), Operator.GREATER,
                                                 ThresholdDataType.LEFT );
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
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
    public void test10GetInverseCumulativeProbability()
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

        //Test the exception conditions
        try
        {
            qFA.applyAsDouble( -0.1 );
            fail( "Expected and exception on using an out-of-bounds probability." );
        }
        catch ( Exception e )
        {
        }
        try
        {
            qFA.applyAsDouble( 1.1 );
            fail( "Expected and exception on using an out-of-bounds probability." );
        }
        catch ( Exception e )
        {
        }
        try
        {
            slicer.getQuantileFunction( new double[] {} ).applyAsDouble( 0.0 );
            fail( "Expected and exception on using an empty test array." );
        }
        catch ( Exception e )
        {
        }
    }

    /**
     * Tests the {@link Slicer#getQuantileFromProbability(Threshold, double[], Integer)}.
     */

    @Test
    public void test11GetQuantileFromProbability()
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

        Threshold testA = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tA ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testB = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tB ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testC = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tC ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testD = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tD ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testE = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tE[0], tE[1] ), Operator.BETWEEN,
                                                        ThresholdDataType.LEFT );
        Threshold testF = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tF ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold testG = metIn.ofProbabilityThreshold( SafeOneOrTwoDoubles.of( tG ), Operator.GREATER,
                                                        ThresholdDataType.LEFT );
        Threshold expectedA = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 1.5 ),
                                                         SafeOneOrTwoDoubles.of( tA ),
                                                         Operator.GREATER,
                                                         ThresholdDataType.LEFT );
        Threshold expectedB = metIn.ofQuantileThreshold( SafeOneOrTwoDoubles.of( 17897.2 ),
                                                         SafeOneOrTwoDoubles.of( tB ),
                                                         Operator.GREATER,
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
        //Test the exception conditions
        try
        {
            slicer.getQuantileFromProbability( null, sorted );
            fail( "Expected and exception on using an out-of-bounds probability." );
        }
        catch ( Exception e )
        {
        }
        try
        {
            slicer.getQuantileFromProbability( testA, null );
            fail( "Expected and exception on using an out-of-bounds probability." );
        }
        catch ( Exception e )
        {
        }
        try
        {
            slicer.getQuantileFromProbability( testA, new double[] {} );
            fail( "Expected and exception on using an empty test array." );
        }
        catch ( Exception e )
        {
        }
    }

    /**
     * Tests the {@link Slicer#transformPair(PairOfDoubleAndVectorOfDoubles)}.
     */

    @Test
    public void test12TransformPair()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf( 3, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf( 0, new double[] { 1, 2, 2, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf( 3, new double[] { 3, 3, 3, 3, 3 } );
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf( 4, new double[] { 4, 4, 4, 4, 4 } );
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf( 0, new double[] { 1, 2, 3, 4, 5 } );
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf( 5, new double[] { 1, 1, 6, 6, 50 } );
        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
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
     * Tests the {@link Slicer#filterByRight(List)}.
     */

    @Test
    public void test13FilterByRight()
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
        Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliced = slicer.filterByRight( input );
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
    public void test14FilterByMetricComponent()
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
     * Tests the {@link Slicer#filter(SingleValuedPairs, java.util.function.DoublePredicate, boolean)}.
    
     * @throws MetricInputSliceException if slicing results in an unexpected exception
     */

    @Test
    public void test15FilterSingleValuedPairs() throws MetricInputSliceException
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
        SingleValuedPairs sliced = slicer.filter( pairs, a -> Double.isFinite( a ), true );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( slicer.filter( pairs, a -> Double.isFinite( a ), false )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getData().equals( values ) );
        //Test without baseline or climatology
        SingleValuedPairs pairsNoBase = metIn.ofSingleValuedPairs( values, meta );
        SingleValuedPairs slicedNoBase = slicer.filter( pairsNoBase, a -> Double.isFinite( a ), false );

        assertTrue( "The sliced data without a baseline does not match the benchmark.",
                    slicedNoBase.getData().equals( expectedValues ) );

        //Test exceptions
        //No pairs in main
        try
        {
            List<PairOfDoubles> none = new ArrayList<>();
            none.add( metIn.pairOf( 1, 1 ) );
            none.add( metIn.pairOf( Double.NaN, Double.NaN ) );
            slicer.filter( metIn.ofSingleValuedPairs( none, meta ), a -> a > 1, false );
            fail( "Expected an exception on attempting to filter with no data." );
        }
        catch ( MetricInputSliceException e )
        {
        }
        //No pairs in baseline
        try
        {
            List<PairOfDoubles> none = new ArrayList<>();
            none.add( metIn.pairOf( 1, 1 ) );
            none.add( metIn.pairOf( Double.NaN, Double.NaN ) );
            slicer.filter( metIn.ofSingleValuedPairs( values, none, meta, meta ), a -> a > 1, false );
            fail( "Expected an exception on attempting to filter with no baseline data." );
        }
        catch ( MetricInputSliceException e )
        {
        }

        //No climatological data
        try
        {
            SingleValuedPairs test =
                    metIn.ofSingleValuedPairs( values,
                                               meta,
                                               metIn.vectorOf( new double[] { 1, Double.NaN } ) );
            slicer.filter( test, a -> a > 1, true );
            fail( "Expected an exception on attempting to filter with no climatological data." );
        }
        catch ( MetricInputSliceException e )
        {
        }
    }

    /**
     * Tests the {@link Slicer#filter(EnsemblePairs, java.util.function.DoublePredicate, boolean)}.
    
     * @throws MetricInputSliceException if slicing results in an unexpected exception
     */

    @Test
    public void test16FilterEnsemblePairs() throws MetricInputSliceException
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
        EnsemblePairs sliced = slicer.filter( pairs, a -> Double.isFinite( a ), true );

        //Test with baseline
        assertTrue( "The sliced data does not match the benchmark.", sliced.getData().equals( expectedValues ) );
        assertTrue( "The sliced baseline data does not match the benchmark.",
                    sliced.getDataForBaseline().equals( expectedValues ) );
        assertTrue( "The sliced climatology data does not match the benchmark.",
                    Arrays.equals( sliced.getClimatology().getDoubles(), climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced climatology.",
                    !Arrays.equals( slicer.filter( pairs, a -> Double.isFinite( a ), false )
                                          .getClimatology()
                                          .getDoubles(),
                                    climatologyExpected.getDoubles() ) );
        assertTrue( "Unexpected equality of the sliced and unsliced data.",
                    !sliced.getData().equals( values ) );
        //Test without baseline or climatology
        EnsemblePairs pairsNoBase = metIn.ofEnsemblePairs( values, meta );
        EnsemblePairs slicedNoBase = slicer.filter( pairsNoBase, a -> Double.isFinite( a ), false );

        assertTrue( "The sliced data without a baseline does not match the benchmark.",
                    slicedNoBase.getData().equals( expectedValues ) );

        //Test exceptions
        //No pairs in main
        try
        {
            List<PairOfDoubleAndVectorOfDoubles> none = new ArrayList<>();
            none.add( metIn.pairOf( 1, new double[] { 1 } ) );
            none.add( metIn.pairOf( Double.NaN, new double[] { Double.NaN } ) );
            slicer.filter( metIn.ofEnsemblePairs( none, meta ), a -> a > 1, false );
            fail( "Expected an exception on attempting to filter with no data." );
        }
        catch ( MetricInputSliceException e )
        {
        }
        //No pairs in baseline
        try
        {
            List<PairOfDoubleAndVectorOfDoubles> none = new ArrayList<>();
            none.add( metIn.pairOf( 1, new double[] { 1 } ) );
            none.add( metIn.pairOf( Double.NaN, new double[] { Double.NaN } ) );
            slicer.filter( metIn.ofEnsemblePairs( values, none, meta, meta ), a -> a > 1, false );
            fail( "Expected an exception on attempting to filter with no baseline data." );
        }
        catch ( MetricInputSliceException e )
        {
        }

        //No climatological data
        try
        {
            EnsemblePairs test =
                    metIn.ofEnsemblePairs( values,
                                           meta,
                                           metIn.vectorOf( new double[] { 1, Double.NaN } ) );
            slicer.filter( test, a -> a > 1, true );
            fail( "Expected an exception on attempting to filter with no climatological data." );
        }
        catch ( MetricInputSliceException e )
        {
        }
    }


}
