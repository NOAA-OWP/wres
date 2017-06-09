package wres.datamodel;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by jesse on 6/7/17.
 */
public class SlicerTest
{
    private final double THRESHOLD = 0.00001;

    @Test
    public void getAnItemOneForEachItemTwoValueTest()
    {
        List<PairOfDoubleAndVectorOfDoubles> pairList = new ArrayList<>();
        DataFactory df = DataFactory.instance();

        double[] arrOne = { 2, 3, 4, 5, 6 };
        double[] arrTwo = { 7, 8, 9 };

        pairList.add(df.pairOf(1, arrOne));
        pairList.add(df.pairOf(10, arrTwo));

        double[] arrFlat = Slicer.getItemsOneForEachItemTwo(pairList);

        int expectedSize = arrOne.length + arrTwo.length;

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

    private int countOfValueInDoubleArray(double[] theArray, double valueToFind)
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
        List<PairOfDoubleAndVectorOfDoubles> pairList = new ArrayList<>();
        DataFactory df = DataFactory.instance();

        double[] arrOne = { 2, 3 };
        double[] arrTwo = { 5, 6, 7 };

        pairList.add(df.pairOf(1, arrOne));
        pairList.add(df.pairOf(4, arrTwo));

        List<PairOfDoubles> resultPairs = Slicer.getFlatDoublePairs(pairList);

        int expectedSize = arrOne.length + arrTwo.length;

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

