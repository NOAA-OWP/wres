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
    @Test public void flattenPairValuesTest()
    {
        List<PairOfDoubleAndVectorOfDoubles> pairList = new ArrayList<>();
        DataFactory df = DataFactory.instance();

        double[] arrOne = { 2, 3, 4, 5, 6 };
        double[] arrTwo = { 7, 8, 9 };

        pairList.add(df.pairOf(1, arrOne));
        pairList.add(df.pairOf(10, arrTwo));

        double[] arrFlat = Slicer.flatArray(pairList);

        int expectedSize = arrOne.length + arrTwo.length + pairList.size();

        assertEquals(expectedSize, arrFlat.length);
        assertTrue(doubleArrayContains(arrFlat, 1));
        assertTrue(doubleArrayContains(arrFlat, 2));
        assertTrue(doubleArrayContains(arrFlat, 3));
        assertTrue(doubleArrayContains(arrFlat, 4));
        assertTrue(doubleArrayContains(arrFlat, 5));
        assertTrue(doubleArrayContains(arrFlat, 6));
        assertTrue(doubleArrayContains(arrFlat, 7));
        assertTrue(doubleArrayContains(arrFlat, 8));
        assertTrue(doubleArrayContains(arrFlat, 9));
        assertTrue(doubleArrayContains(arrFlat, 10));
    }

    private boolean doubleArrayContains(double[] theArray, double valueToFind)
    {
        for (int i = 0; i < theArray.length; i++)
        {
            if (theArray[i] == valueToFind)
            {
                return true;
            }
        }
        return false;
    }
}

