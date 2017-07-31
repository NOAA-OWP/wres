package wres.datamodel.metric;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import org.junit.Test;

import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;

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
     * Tests the {@link Slicer#getLeftSide(List)}.
     */

    @Test
    public void test1GetLeftSide()
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
                                                                                   .getMetadata(6))),
                                 expected));
    }

    /**
     * Tests the {@link Slicer#getRightSide(List)}.
     */

    @Test
    public void test2GetRightSide()
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
                                                                                    .getMetadata(6))),
                                 expected));
    }

    /**
     * Tests the {@link Slicer#transformPairs(EnsemblePairs, Function)}.
     */

    @Test
    public void test3TransformPairs()
    {
        DataFactory metIn = DefaultDataFactory.getInstance();
        final List<PairOfDoubleAndVectorOfDoubles> values = new ArrayList<>();
        values.add(metIn.pairOf(0, new double[]{1, 2, 3, 4, 5}));
        values.add(metIn.pairOf(0, new double[]{6, 7, 8, 9, 10}));
        values.add(metIn.pairOf(1, new double[]{11, 12, 13, 14, 15}));
        values.add(metIn.pairOf(1, new double[]{16, 17, 18, 19, 20}));
        values.add(metIn.pairOf(0, new double[]{21, 22, 23, 24, 25}));
        values.add(metIn.pairOf(1, new double[]{26, 27, 28, 29, 30}));
        Metadata meta = metIn.getMetadataFactory().getMetadata(6);
        EnsemblePairs input = metIn.ofEnsemblePairs(values, values, meta, meta);
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

//    @Test
//    public void test4TransformPairs()
//    {
//        DataFactory metIn = DefaultDataFactory.getInstance();
//        final List<PairOfDoubles> values = new ArrayList<>();
//        values.add(metIn.pairOf(0, 3.0 / 5.0));
//        values.add(metIn.pairOf(0, 1.0 / 5.0));
//        values.add(metIn.pairOf(1, 2.0 / 5.0));
//        values.add(metIn.pairOf(1, 3.0 / 5.0));
//        values.add(metIn.pairOf(0, 0.0 / 5.0));
//        values.add(metIn.pairOf(1, 1.0 / 5.0));
//        Metadata meta = metIn.getMetadataFactory().getMetadata(6);
//        Function<PairOfDoubles, PairOfBooleans> mapper = (in) -> {
//            return metIn.pairOf(in.getItemOne() > 0, in.getItemTwo() > 0);
//        };
//        final List<PairOfBooleans> expectedValues = new ArrayList<>();
//        expectedValues.add(metIn.pairOf(false, true));
//        expectedValues.add(metIn.pairOf(false, true));
//        expectedValues.add(metIn.pairOf(true, true));
//        expectedValues.add(metIn.pairOf(true, true));
//        expectedValues.add(metIn.pairOf(false, false));
//        expectedValues.add(metIn.pairOf(true, true));
//        DichotomousPairs expectedNoBase = metIn.ofDichotomousPairsFromAtomic(expectedValues, meta);
//        DichotomousPairs expectedBase = metIn.ofDichotomousPairsFromAtomic(expectedValues, expectedValues, meta, meta);
//
//        //Test without baseline
//        DichotomousPairs actualNoBase = slicer.transformPairs(metIn.ofSingleValuedPairs(values, meta), mapper);
//        assertTrue("The transformed test data does not match the benchmark.",
//                  actualNoBase.getData().equals(expectedNoBase.getData()));
//        //Test baseline
//        DichotomousPairs actualBase = slicer.transformPairs(metIn.ofSingleValuedPairs(values, values, meta, meta),
//                                                            mapper);
//        assertTrue("The transformed test data does not match the benchmark.",
//                   actualBase.getDataForBaseline().equals(expectedBase.getDataForBaseline()));
//    }

}
