package wres.datamodel;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Test;

import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.SafeVectorOfBooleans;
import wres.datamodel.Threshold.Operator;

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
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        double[] expected = new double[]{0, 0, 1, 1, 0, 1};
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(metIn.ofSingleValuedPairs(values,
                                                                              metIn.getMetadataFactory()
                                                                                   .getMetadata())),
                                 expected));
    }

    /**
     * Tests the {@link Slicer#getLeftSide(EnsemblePairs)}.
     */

    @Test
    public void test2GetLeftSideEnsemble()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        double[] expected = new double[]{0, 0, 1, 1, 0, 1};
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(metIn.ofEnsemblePairs(values,
                                                                          metIn.getMetadataFactory().getMetadata())),
                                 expected));
    }

    /**
     * Tests the {@link Slicer#getRightSide(List)}.
     */

    @Test
    public void test3GetRightSide()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        double[] expected = new double[]{3.0 / 5.0, 1.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0, 0.0 / 5.0, 1.0 / 5.0};
        assertTrue("The right side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getRightSide(metIn.ofSingleValuedPairs(values,
                                                                               metIn.getMetadataFactory()
                                                                                    .getMetadata())),
                                 expected));
    }

    /**
     * Tests the {@link Slicer#sliceByLeft(SingleValuedPairs, Threshold)}.
     * 
     * @throws MetricInputSliceException
     */

    @Test
    public void test4SliceByLeft() throws MetricInputSliceException
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        double[] expected = new double[]{1, 1, 1};
        Threshold threshold = metIn.getThreshold(0.0, Operator.GREATER);
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        SingleValuedPairs pairs = metIn.ofSingleValuedPairs(values, values, meta, meta, null);
        SingleValuedPairs sliced = slicer.sliceByLeft(pairs, threshold);
        //Test with baseline
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(sliced.getBaselineData()), expected));
        //Test without baseline
        SingleValuedPairs pairsNoBase = metIn.ofSingleValuedPairs(values, meta);
        SingleValuedPairs slicedNoBase = slicer.sliceByLeft(pairsNoBase, threshold);
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(slicedNoBase), expected));
        //Test exception
        try
        {
            slicer.sliceByLeft(pairs, metIn.getThreshold(1.0, Operator.GREATER));
            fail("Expected an exception on attempting to return an empty subset.");
        }
        catch(Exception e)
        {
        }

        //Test null return on baseline
        final List<PairOfDoubles> nullValuesBase = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(0, 2.0 / 5.0));
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        SingleValuedPairs pairsNullBase = metIn.ofSingleValuedPairs(values, nullValuesBase, meta, meta, null);
        try
        {
            slicer.sliceByLeft(pairsNullBase, threshold);
            fail("Expected an exception on attempting to return an empty subset for the baseline.");
        }
        catch(Exception e)
        {
        }           
    }

    /**
     * Tests the {@link Slicer#sliceByLeft(EnsemblePairs, Threshold)}.
     * 
     * @throws MetricInputSliceException
     */

    @Test
    public void test5SliceByLeft() throws MetricInputSliceException
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        double[] expected = new double[]{1, 1, 1};
        Threshold threshold = metIn.getThreshold(0.0, Operator.GREATER);
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs pairs = metIn.ofEnsemblePairs(values, values, meta, meta, null);
        EnsemblePairs sliced = slicer.sliceByLeft(pairs, threshold);
        //Test with baseline
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(sliced.getBaselineData()), expected));
        //Test without baseline
        EnsemblePairs pairsNoBase = metIn.ofEnsemblePairs(values, meta);
        EnsemblePairs slicedNoBase = slicer.sliceByLeft(pairsNoBase, threshold);
        assertTrue("The left side of the test data does not match the benchmark.",
                   Arrays.equals(slicer.getLeftSide(slicedNoBase), expected));
        //Test exception
        try
        {
            slicer.sliceByLeft(pairs, metIn.getThreshold(1.0, Operator.GREATER));
            fail("Expected an exception on attempting to return an empty subset.");
        }
        catch(Exception e)
        {
        }        
        
        //Test exception on baseline
        final List<PairOfDoubleAndVectorOfDoubles> nullValuesBase = new ArrayList<>();
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3}));
        EnsemblePairs pairsNullBase = metIn.ofEnsemblePairs(values, nullValuesBase, meta, meta, null);
        try
        {
            slicer.sliceByLeft(pairsNullBase, threshold);
            fail("Expected an exception on attempting to return an empty subset for the baseline.");
        }
        catch(Exception e)
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
        values.add(metIn.pairOf(0, new double[]{1, 2, 3, 4, 5}));
        values.add(metIn.pairOf(0, new double[]{6, 7, 8, 9, 10}));
        values.add(metIn.pairOf(1, new double[]{11, 12, 13, 14, 15}));
        values.add(metIn.pairOf(1, new double[]{16, 17, 18, 19, 20}));
        values.add(metIn.pairOf(0, new double[]{21, 22, 23, 24, 25}));
        values.add(metIn.pairOf(1, new double[]{26, 27, 28, 29, 30}));
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        EnsemblePairs input = metIn.ofEnsemblePairs(values, values, meta, meta, null);
        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper = (in) -> {
            return metIn.pairOf(in.getItemOne(), Arrays.stream(in.getItemTwo()).average().getAsDouble());
        };
        double[] expected = new double[]{3.0, 8.0, 13.0, 18.0, 23.0, 28.0};
        //Test without baseline
        double[] actualNoBase = slicer.getRightSide(slicer.transformPairs(metIn.ofEnsemblePairs(values, meta), mapper));
        assertTrue("The transformed test data does not match the benchmark.", Arrays.equals(actualNoBase, expected));
        //Test baseline
        double[] actualBase = slicer.getRightSide(slicer.transformPairs(input, mapper).getBaselineData());
        assertTrue("The transformed test data does not match the benchmark.", Arrays.equals(actualBase, expected));
    }

    /**
     * Tests the {@link Slicer#transformPairs(SingleValuedPairs, Function)}. TODO: implement equals in
     * {@link SafeVectorOfBooleans}, then uncomment below
     */

    @Test
    public void test7TransformPairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 1.0 / 5.0));
        values.add(metIn.pairOf(1, 2.0 / 5.0));
        values.add(metIn.pairOf(1, 3.0 / 5.0));
        values.add(metIn.pairOf(0, 0.0 / 5.0));
        values.add(metIn.pairOf(1, 1.0 / 5.0));
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        Function<PairOfDoubles, PairOfBooleans> mapper = (in) -> {
            return metIn.pairOf(in.getItemOne() > 0, in.getItemTwo() > 0);
        };
        final List<PairOfBooleans> expectedValues = new ArrayList<>();
        expectedValues.add(metIn.pairOf(false, true));
        expectedValues.add(metIn.pairOf(false, true));
        expectedValues.add(metIn.pairOf(true, true));
        expectedValues.add(metIn.pairOf(true, true));
        expectedValues.add(metIn.pairOf(false, false));
        expectedValues.add(metIn.pairOf(true, true));
        DichotomousPairs expectedNoBase = metIn.ofDichotomousPairsFromAtomic(expectedValues, meta);
        DichotomousPairs expectedBase = metIn.ofDichotomousPairsFromAtomic(expectedValues,
                                                                           expectedValues,
                                                                           meta,
                                                                           meta,
                                                                           null);

        //Test without baseline
        DichotomousPairs actualNoBase = slicer.transformPairs(metIn.ofSingleValuedPairs(values, meta), mapper);
//        assertTrue("The transformed test data does not match the benchmark.",
//                  actualNoBase.getData().equals(expectedNoBase.getData()));
        //Test baseline
        DichotomousPairs actualBase = slicer.transformPairs(metIn.ofSingleValuedPairs(values, values, meta, meta, null),
                                                            mapper);
//        assertTrue("The transformed test data does not match the benchmark.",
//                   actualBase.getDataForBaseline().equals(expectedBase.getDataForBaseline()));
    }

    /**
     * Tests the {@link Slicer#transformPairs(EnsemblePairs, Threshold, BiFunction)}.
     */

    @Test
    public void test8TransformPairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(3, new double[]{1, 2, 3, 4, 5}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 2, 3, 3}));
        values.add(metIn.pairOf(3, new double[]{3, 3, 3, 3, 3}));
        values.add(metIn.pairOf(4, new double[]{4, 4, 4, 4, 4}));
        values.add(metIn.pairOf(0, new double[]{1, 2, 3, 4, 5}));
        values.add(metIn.pairOf(5, new double[]{1, 1, 6, 6, 50}));
        Metadata meta = metIn.getMetadataFactory().getMetadata();
        Threshold threshold = metIn.getThreshold(3.0, Operator.GREATER);
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
        double[] expectedLeft = new double[]{0.0, 0.0, 0.0, 1.0, 0.0, 1.0};
        double[] expectedRight = new double[]{2.0 / 5.0, 0.0 / 5.0, 0.0 / 5.0, 5.0 / 5.0, 2.0 / 5.0, 3.0 / 5.0};

        //Test without baseline
        double[] actualNoBaseLeft = slicer.getLeftSide(slicer.transformPairs(metIn.ofEnsemblePairs(values, meta),
                                                                             threshold,
                                                                             mapper));
        double[] actualNoBaseRight = slicer.getRightSide(slicer.transformPairs(metIn.ofEnsemblePairs(values, meta),
                                                                               threshold,
                                                                               mapper));
        assertTrue("The transformed test data does not match the benchmark.",
                   Arrays.equals(actualNoBaseLeft, expectedLeft));
        assertTrue("The transformed test data does not match the benchmark.",
                   Arrays.equals(actualNoBaseRight, expectedRight));

        //Test baseline
        double[] actualBaseLeft = slicer.getLeftSide(slicer.transformPairs(
                                                                           metIn.ofEnsemblePairs(values,
                                                                                                 values,
                                                                                                 meta,
                                                                                                 meta,
                                                                                                 null),
                                                                           threshold,
                                                                           mapper)
                                                           .getBaselineData());
        double[] actualBaseRight = slicer.getRightSide(slicer.transformPairs(
                                                                             metIn.ofEnsemblePairs(values,
                                                                                                   values,
                                                                                                   meta,
                                                                                                   meta,
                                                                                                   null),
                                                                             threshold,
                                                                             mapper)
                                                             .getBaselineData());
        assertTrue("The transformed test data does not match the benchmark.",
                   Arrays.equals(actualBaseLeft, expectedLeft));
        assertTrue("The transformed test data does not match the benchmark.",
                   Arrays.equals(actualBaseRight, expectedRight));
    }

    /**
     * Tests the {@link Slicer#transformPair(PairOfDoubleAndVectorOfDoubles, Threshold)}.
     */

    @Test
    public void test9TransformPair()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf(3, new double[]{1, 2, 3, 4, 5});
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf(0, new double[]{1, 2, 2, 3, 3});
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf(3, new double[]{3, 3, 3, 3, 3});
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf(4, new double[]{4, 4, 4, 4, 4});
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf(0, new double[]{1, 2, 3, 4, 5});
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf(5, new double[]{1, 1, 6, 6, 50});
        Threshold threshold = metIn.getThreshold(3.0, Operator.GREATER);
        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
        assertTrue("The transformed pair does not match the benchmark",
                   mapper.apply(a, threshold).equals(metIn.pairOf(0.0, 2.0 / 5.0)));
        assertTrue("The transformed pair does not match the benchmark",
                   mapper.apply(b, threshold).equals(metIn.pairOf(0.0, 0.0 / 5.0)));
        assertTrue("The transfored pair does not match the benchmark",
                   mapper.apply(c, threshold).equals(metIn.pairOf(0.0, 0.0 / 5.0)));
        assertTrue("The transformed pair does not match the benchmark",
                   mapper.apply(d, threshold).equals(metIn.pairOf(1.0, 5.0 / 5.0)));
        assertTrue("The transformed pair does not match the benchmark",
                   mapper.apply(e, threshold).equals(metIn.pairOf(0.0, 2.0 / 5.0)));
        assertTrue("The transformed pair does not match the benchmark",
                   mapper.apply(f, threshold).equals(metIn.pairOf(1.0, 3.0 / 5.0)));
    }

    /**
     * Tests the {@link Slicer#getQuantile(double, double[])}.
     */

    @Test
    public void test10GetInverseCumulativeProbability()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        double[] sorted = new double[]{1.5, 6.3, 4.9, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2};
        double testA = 0.0;
        double testB = 1.0;
        double testC = 7.0 / 11.0;
        double testD = (8.0 + ((5005.0 - 2009.8) / (7001.4 - 2009.8))) / 11.0;
        double expectedA = 1.5;
        double expectedB = 17897.2;
        double expectedC = 1012.6;
        double expectedD = 5005.0;

        //Test for equality
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   metIn.doubleEquals(slicer.getQuantile(testA, sorted), expectedA, 7));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   metIn.doubleEquals(slicer.getQuantile(testB, sorted), expectedB, 7));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   metIn.doubleEquals(slicer.getQuantile(testC, sorted), expectedC, 7));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   metIn.doubleEquals(slicer.getQuantile(testD, sorted), expectedD, 7));
        //Test the exception conditions
        try
        {
            slicer.getQuantile(-0.1, sorted);
            fail("Expected and exception on using an out-of-bounds probability.");
        }
        catch(Exception e)
        {
        }
        try
        {
            slicer.getQuantile(1.1, sorted);
            fail("Expected and exception on using an out-of-bounds probability.");
        }
        catch(Exception e)
        {
        }
        try
        {
            slicer.getQuantile(0.0, new double[]{});
            fail("Expected and exception on using an empty test array.");
        }
        catch(Exception e)
        {
        }
    }

    /**
     * Tests the {@link Slicer#getQuantileFromProbability(ProbabilityThreshold, double[])}.
     */

    @Test
    public void test11GetQuantileFromProbability()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        double[] sorted = new double[]{1.5, 6.3, 4.9, 27, 43.3, 433.9, 1012.6, 2009.8, 7001.4, 12038.5, 17897.2};
        double[] sortedSecond = new double[]{1.5};
        double tA = 0.0;
        double tB = 1.0;
        double tC = 7.0 / 11.0;
        double tD = (8.0 + ((5005.0 - 2009.8) / (7001.4 - 2009.8))) / 11.0;
        double[] tE = new double[]{0.25, 0.5};
        double tF = 8.0 / 11.0;
        double tG = 0.01;

        ProbabilityThreshold testA = metIn.getProbabilityThreshold(tA, Operator.GREATER);
        ProbabilityThreshold testB = metIn.getProbabilityThreshold(tB, Operator.GREATER);
        ProbabilityThreshold testC = metIn.getProbabilityThreshold(tC, Operator.GREATER);
        ProbabilityThreshold testD = metIn.getProbabilityThreshold(tD, Operator.GREATER);
        ProbabilityThreshold testE = metIn.getProbabilityThreshold(tE[0], tE[1], Operator.BETWEEN);
        ProbabilityThreshold testF = metIn.getProbabilityThreshold(tF, Operator.GREATER);
        ProbabilityThreshold testG = metIn.getProbabilityThreshold(tG, Operator.GREATER);
        QuantileThreshold expectedA = metIn.getQuantileThreshold(1.5, tA, Operator.GREATER);
        QuantileThreshold expectedB = metIn.getQuantileThreshold(17897.2, tB, Operator.GREATER);
        QuantileThreshold expectedC = metIn.getQuantileThreshold(1012.6, tC, Operator.GREATER);
        QuantileThreshold expectedD = metIn.getQuantileThreshold(5005.000000000002, tD, Operator.GREATER);
        QuantileThreshold expectedE = metIn.getQuantileThreshold(5.25,
                                                                 238.59999999999997,
                                                                 tE[0],
                                                                 tE[1],
                                                                 Operator.BETWEEN);
        QuantileThreshold expectedF = metIn.getQuantileThreshold(1.5, tF, Operator.GREATER);
        QuantileThreshold expectedG = metIn.getQuantileThreshold(1.5, tG, Operator.GREATER);

        //Test for equality
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testA, sorted).equals(expectedA));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testB, sorted).equals(expectedB));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testC, sorted).equals(expectedC));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testD, sorted).equals(expectedD));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testE, sorted).equals(expectedE));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testF, sortedSecond).equals(expectedF));
        assertTrue("The inverse cumulative probability does not match the benchmark",
                   slicer.getQuantileFromProbability(testG, sorted).equals(expectedG));
        //Test the exception conditions
        try
        {
            slicer.getQuantileFromProbability(null, sorted);
            fail("Expected and exception on using an out-of-bounds probability.");
        }
        catch(Exception e)
        {
        }
        try
        {
            slicer.getQuantileFromProbability(testA, null);
            fail("Expected and exception on using an out-of-bounds probability.");
        }
        catch(Exception e)
        {
        }
        try
        {
            slicer.getQuantileFromProbability(testA, new double[]{});
            fail("Expected and exception on using an empty test array.");
        }
        catch(Exception e)
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
        PairOfDoubleAndVectorOfDoubles a = metIn.pairOf(3, new double[]{1, 2, 3, 4, 5});
        PairOfDoubleAndVectorOfDoubles b = metIn.pairOf(0, new double[]{1, 2, 2, 3, 3});
        PairOfDoubleAndVectorOfDoubles c = metIn.pairOf(3, new double[]{3, 3, 3, 3, 3});
        PairOfDoubleAndVectorOfDoubles d = metIn.pairOf(4, new double[]{4, 4, 4, 4, 4});
        PairOfDoubleAndVectorOfDoubles e = metIn.pairOf(0, new double[]{1, 2, 3, 4, 5});
        PairOfDoubleAndVectorOfDoubles f = metIn.pairOf(5, new double[]{1, 1, 6, 6, 50});
        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper = metIn.getSlicer()::transformPair;
        assertTrue("The transformed pair does not match the benchmark", mapper.apply(a).equals(metIn.pairOf(3, 1)));
        assertTrue("The transformed pair does not match the benchmark", mapper.apply(b).equals(metIn.pairOf(0, 1)));
        assertTrue("The transfored pair does not match the benchmark", mapper.apply(c).equals(metIn.pairOf(3, 3)));
        assertTrue("The transformed pair does not match the benchmark", mapper.apply(d).equals(metIn.pairOf(4, 4)));
        assertTrue("The transformed pair does not match the benchmark", mapper.apply(e).equals(metIn.pairOf(0, 1)));
        assertTrue("The transformed pair does not match the benchmark", mapper.apply(f).equals(metIn.pairOf(5, 1)));
    }

    /**
     * Tests the {@link Slicer#sliceByRight(List)}.
     */

    @Test
    public void test13SliceByRight()
    {
        List<PairOfDoubleAndVectorOfDoubles> input = new ArrayList<>();
        DataFactory metIn = DefaultDataFactory.getInstance();
        input.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        input.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        input.add(metIn.pairOf(1, new double[]{1, 2, 3}));
        input.add(metIn.pairOf(2, new double[]{1, 2, 3, 4, 5}));
        input.add(metIn.pairOf(2, new double[]{1, 2, 3, 4, 5}));
        input.add(metIn.pairOf(2, new double[]{1, 2, 3, 4, 5}));
        input.add(metIn.pairOf(2, new double[]{1, 2, 3, 4, 5}));
        input.add(metIn.pairOf(2, new double[]{1, 2, 3, 4, 5}));
        input.add(metIn.pairOf(3, new double[]{1, 2, 3, 4, 5, 6}));
        input.add(metIn.pairOf(3, new double[]{1, 2, 3, 4, 5, 6}));
        input.add(metIn.pairOf(3, new double[]{1, 2, 3, 4, 5, 6}));
        input.add(metIn.pairOf(3, new double[]{1, 2, 3, 4, 5, 6}));
        //Slice
        Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliced = slicer.sliceByRight(input);
        //Check the results
        assertTrue("Expected three slices of data.", sliced.size() == 3);
        assertTrue("Expected the first slice to contain three pairs.", sliced.get(3).size() == 3);
        assertTrue("Expected the second slice to contain five pairs.", sliced.get(5).size() == 5);
        assertTrue("Expected the third slice to contain four pairs.", sliced.get(6).size() == 4);
    }

    /**
     * Tests the {@link Slicer#sliceByMetricComponent(MetricOutputMapByLeadThreshold)}.
     */

    @Test
    public void test14SliceByMetricComponent()
    {
        //Obtain input and slice
        MetricOutputMapByLeadThreshold<VectorOutput> toSlice =
                                                             DataModelTestDataFactory.getVectorMetricOutputMapByLeadThresholdOne();
        Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> sliced =
                                                                                  slicer.sliceByMetricComponent(toSlice);

        //Check the results
        assertTrue("Expected five slices of data.",
                   sliced.size() == toSlice.getMetadata().getMetricComponentID().getMetricComponents().size());
        sliced.forEach((key, value) -> assertTrue("Expected 638 elements in each slice.", value.size() == 638));
    }

}
