package wres.datamodel;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Created by jesse on 7/17/17.
 */
public class SafePairOfDoubleAndVectorOfDoublesTest
{
    @Test
    public void fancyPairWithEqualValuesIsEqualTest()
    {
        double[] firstArr = {1.0, 2.0};
        double[] secondArr = {1.0, 2.0};
        PairOfDoubleAndVectorOfDoubles firstPair =
                SafePairOfDoubleAndVectorOfDoubles.of(3.0, firstArr);
        PairOfDoubleAndVectorOfDoubles secondPair =
                SafePairOfDoubleAndVectorOfDoubles.of(3.0, secondArr);
        assertTrue("Expect pairs to be equal",
                   firstPair.compareTo(secondPair) == 0);
        assertTrue("Expect pairs to be equal",
                   secondPair.compareTo(firstPair) == 0);
    }

    @Test
    public void fancyPairFirstValueLessThanSecond()
    {
        double[] emptyArr = {};
        PairOfDoubleAndVectorOfDoubles firstPair =
                SafePairOfDoubleAndVectorOfDoubles.of(1.0, emptyArr);
        PairOfDoubleAndVectorOfDoubles secondPair =
                SafePairOfDoubleAndVectorOfDoubles.of(2.0, emptyArr);
        assertTrue("Expect first pair to be less than second",
                   firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second pair to be more than first",
                   secondPair.compareTo(firstPair) > 0);
    }

    @Test
    public void fancyPairEmptyArrayIsLessThanNonEmptyTest()
    {
        double[] emptyArr = {};
        double[] nonEmptyArr = {1.0};
        PairOfDoubleAndVectorOfDoubles emptyPair =
                SafePairOfDoubleAndVectorOfDoubles.of(2.0, emptyArr);
        PairOfDoubleAndVectorOfDoubles nonEmptyPair =
                SafePairOfDoubleAndVectorOfDoubles.of(2.0, nonEmptyArr);
        assertTrue("Expect empty array pair to be less than nonEmpty",
                   emptyPair.compareTo(nonEmptyPair) < 0);
        assertTrue("Expect nonEmpty array pair to be more than empty",
                   nonEmptyPair.compareTo(emptyPair) > 0);
    }

    @Test
    public void fancyPairEqualLengthButDifferentValuesInArrayTest()
    {
        double[] firstArr = {1.0, 2.0};
        double[] secondArr = {1.0, 3.0};
        PairOfDoubleAndVectorOfDoubles firstPair =
                SafePairOfDoubleAndVectorOfDoubles.of(4.0, firstArr);
        PairOfDoubleAndVectorOfDoubles secondPair =
                SafePairOfDoubleAndVectorOfDoubles.of(4.0, secondArr);
        assertTrue("Expect first array pair to be less than second",
                   firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second array pair to be more than first",
                secondPair.compareTo(firstPair) > 0);
    }

    @Test
    public void fancyPairEqualsItself()
    {
        double[] firstArr = {1.0, 2.0};
        PairOfDoubleAndVectorOfDoubles firstPair =
                SafePairOfDoubleAndVectorOfDoubles.of(4.0, firstArr);
        assertTrue("Expect a pair to equal itself",
                   firstPair.compareTo(firstPair) == 0);
    }

}
