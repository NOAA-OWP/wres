package wres.datamodel;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.MetricInputFactory;

/**
 * Created by jesse on 6/7/17.
 */
public class SlicerTest
{
    private final double THRESHOLD = 0.00001;
    
    private final MetricInputFactory metIn = DefaultMetricInputFactory.getInstance(); //JBr

    @Test
    public void getAnItemOneForEachItemTwoValueTest()
    {
        final List<PairOfDoubleAndVectorOfDoubles> pairList = new ArrayList<>();
        final double[] arrOne = { 2, 3, 4, 5, 6 };
        final double[] arrTwo = { 7, 8, 9 };

        pairList.add(metIn.pairOf(1, arrOne));
        pairList.add(metIn.pairOf(10, arrTwo));

        final double[] arrFlat = Slicer.getItemsOneForEachItemTwo(pairList);

        final int expectedSize = arrOne.length + arrTwo.length;

        assertEquals(expectedSize, arrFlat.length);
        assertEquals(5, countOfValueInDoubleArray(arrFlat, 1));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 2));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 3));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 4));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 5));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 6));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 7));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 8));
        assertEquals(0, countOfValueInDoubleArray(arrFlat, 9));
        assertEquals(3, countOfValueInDoubleArray(arrFlat, 10));
    }

    private int countOfValueInDoubleArray(final double[] theArray, final double valueToFind)
    {
        int result = 0;
        for (int i = 0; i < theArray.length; i++)
        {
            if (theArray[i] == valueToFind)
            {
                result++;
            }
        }
        return result;
    }

    @Test
    public void getListOfSimplePairsFromListOfPairOfDoubleAndVectorOfDoublesTest()
    {
        final List<PairOfDoubleAndVectorOfDoubles> pairList = new ArrayList<>();
        final double[] arrOne = { 2, 3 };
        final double[] arrTwo = { 5, 6, 7 };

        pairList.add(metIn.pairOf(1, arrOne));
        pairList.add(metIn.pairOf(4, arrTwo));

        final List<PairOfDoubles> resultPairs = Slicer.getFlatDoublePairs(pairList);

        final int expectedSize = arrOne.length + arrTwo.length;

        assertEquals(expectedSize, resultPairs.size());

        // currently pairs are ordered, but we don't necessarily need
        // them to be ordered, so these assertions can change when we don't
        // order them.
        assertEquals(1, resultPairs.get(0).getItemOne(), THRESHOLD);
        assertEquals(2, resultPairs.get(0).getItemTwo(), THRESHOLD);
        assertEquals(1, resultPairs.get(1).getItemOne(), THRESHOLD);
        assertEquals(3, resultPairs.get(1).getItemTwo(), THRESHOLD);
        assertEquals(4, resultPairs.get(2).getItemOne(), THRESHOLD);
        assertEquals(5, resultPairs.get(2).getItemTwo(), THRESHOLD);
        assertEquals(4, resultPairs.get(3).getItemOne(), THRESHOLD);
        assertEquals(6, resultPairs.get(3).getItemTwo(), THRESHOLD);
        assertEquals(4, resultPairs.get(4).getItemOne(), THRESHOLD);
        assertEquals(7, resultPairs.get(4).getItemTwo(), THRESHOLD);

    }
}

