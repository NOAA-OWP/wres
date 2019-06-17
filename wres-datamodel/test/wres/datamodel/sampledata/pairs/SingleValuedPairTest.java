package wres.datamodel.sampledata.pairs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SingleValuedPairTest
{
    @Test
    public void itemOneDifferent()
    {
        SingleValuedPair firstPair = SingleValuedPair.of(1.0, 2.0);
        SingleValuedPair secondPair = SingleValuedPair.of(3.0, 1.0);
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
        SingleValuedPair firstPair = SingleValuedPair.of(1.0, 2.0);
        SingleValuedPair secondPair = SingleValuedPair.of(1.0, 3.0);
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
        SingleValuedPair firstPair = SingleValuedPair.of(1.0, 2.0);
        assertTrue("Expect a pair to equal itself",
                firstPair.compareTo(firstPair) == 0);
        assertTrue("Expect a pair to equal itself",
                   firstPair.equals(firstPair));
        assertEquals( firstPair.hashCode(), firstPair.hashCode() );
    }

    @Test
    public void notEqualToAnotherType()
    {
        SingleValuedPair thePair = SingleValuedPair.of(1.0, 2.0);
        Integer iAmAnInteger = 5;
        assertNotEquals( iAmAnInteger, thePair );
    }
}
