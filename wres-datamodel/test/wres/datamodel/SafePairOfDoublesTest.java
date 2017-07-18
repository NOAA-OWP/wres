package wres.datamodel;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SafePairOfDoublesTest
{
    @Test
    public void itemOneDifferent()
    {
        PairOfDoubles firstPair = SafePairOfDoubles.of(1.0, 2.0);
        PairOfDoubles secondPair = SafePairOfDoubles.of(3.0, 1.0);
        assertTrue("Expect first pair to be less than second pair",
                firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second pair to be more than first pair",
                secondPair.compareTo(firstPair) > 0);
    }

    @Test
    public void itemOneSameItemTwoDifferent()
    {
        PairOfDoubles firstPair = SafePairOfDoubles.of(1.0, 2.0);
        PairOfDoubles secondPair = SafePairOfDoubles.of(1.0, 3.0);
        assertTrue("Expect first pair to be less than second pair",
                firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second pair to be more than first pair",
                secondPair.compareTo(firstPair) > 0);
    }

    @Test
    public void equalsItself()
    {
        PairOfDoubles firstPair = SafePairOfDoubles.of(1.0, 2.0);
        assertTrue("Expect a pair to equal itself",
                firstPair.compareTo(firstPair) == 0);
    }
}
