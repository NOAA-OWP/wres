package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.inputs.pairs.PairOfDoubles;

public class SafePairOfDoublesTest
{
    @Test
    public void itemOneDifferent()
    {
        PairOfDoubles firstPair = new SafePairOfDoubles(1.0, 2.0);
        PairOfDoubles secondPair = new SafePairOfDoubles(3.0, 1.0);
        assertTrue("Expect first pair to be less than second pair",
                firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second pair to be more than first pair",
                secondPair.compareTo(firstPair) > 0);
        assertFalse("Expect first pair to not equal second pair",
                   firstPair.equals(secondPair));
        assertFalse("Expect second pair to not equal first pair",
                    secondPair.equals(firstPair));
        assertTrue("Expect different pairs to have different hashCode",
                   firstPair.hashCode() != secondPair.hashCode());
    }

    @Test
    public void itemOneSameItemTwoDifferent()
    {
        PairOfDoubles firstPair = new SafePairOfDoubles(1.0, 2.0);
        PairOfDoubles secondPair = new SafePairOfDoubles(1.0, 3.0);
        assertTrue("Expect first pair to be less than second pair",
                firstPair.compareTo(secondPair) < 0);
        assertTrue("Expect second pair to be more than first pair",
                secondPair.compareTo(firstPair) > 0);
        assertFalse("Expect first pair to not equal second pair",
                firstPair.equals(secondPair));
        assertFalse("Expect second pair to not equal first pair",
                secondPair.equals(firstPair));
        assertTrue("Expect different pairs to have different hashCode",
                firstPair.hashCode() != secondPair.hashCode());
    }

    @Test
    public void equalsItself()
    {
        PairOfDoubles firstPair = new SafePairOfDoubles(1.0, 2.0);
        assertTrue("Expect a pair to equal itself",
                firstPair.compareTo(firstPair) == 0);
        assertTrue("Expect a pair to equal itself",
                   firstPair.equals(firstPair));
        assertTrue("Expect a pair's hashcode to be consistent",
                   firstPair.hashCode() == firstPair.hashCode());
    }

    @Test
    public void notEqualToAnotherType()
    {
        PairOfDoubles thePair = new SafePairOfDoubles(1.0, 2.0);
        Integer iAmAnInteger = 5;
        assertFalse("Expect a pair to not equal another type",
                    thePair.equals(iAmAnInteger));
    }
}
