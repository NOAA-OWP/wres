package wres.datamodel;

import org.junit.Test;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertTrue;

public class SafeVectorOfDoublesTest
{
    @Test
    public void itemOneDifferent()
    {
        double[] first = {1.0, 2.0};
        double[] second = {3.0, 1.0};
        VectorOfDoubles firstVec = SafeVectorOfDoubles.of(first);
        VectorOfDoubles secondVec = SafeVectorOfDoubles.of(second);
        assertTrue("Expect first vec to be less than second vec",
                   firstVec.compareTo(secondVec) < 0);
        assertTrue("Expect second vec to be more than first vec",
                   secondVec.compareTo(firstVec) > 0);
        assertFalse("Expect first vec to not equal second vec",
                    firstVec.equals(secondVec));
        assertFalse("Expect second vec to not equal first vec",
                    secondVec.equals(firstVec));
        assertTrue("Expect first hashcode to be different from second",
                   firstVec.hashCode() != secondVec.hashCode());
    }

    @Test
    public void itemOneSameItemTwoDifferent()
    {
        double[] first = {1.0, 2.0};
        double[] second = {1.0, 3.0};
        VectorOfDoubles firstVec = SafeVectorOfDoubles.of(first);
        VectorOfDoubles secondVec = SafeVectorOfDoubles.of(second);
        assertTrue("Expect first pair to be less than second pair",
                   firstVec.compareTo(secondVec) < 0);
        assertTrue("Expect second pair to be more than first pair",
                   secondVec.compareTo(firstVec) > 0);
        assertFalse("Expect first vec to not equal second vec",
                    firstVec.equals(secondVec));
        assertFalse("Expect second vec to not equal first vec",
                    secondVec.equals(firstVec));
        assertTrue("Expect first hashcode to be different from second",
                   firstVec.hashCode() != secondVec.hashCode());
    }

    @Test
    public void equalsItself()
    {
        double[] doubles = {1.0, 2.0};
        VectorOfDoubles vec = SafeVectorOfDoubles.of(doubles);
        assertTrue("Expect a pair to equal itself",
                   vec.compareTo(vec) == 0);
        assertTrue("Expect a vec to equal itself",
                   vec.equals(vec));
    }

    @Test
    public void differentTypeNotEqual()
    {
        double[] doubles = {1.0, 2.0};
        VectorOfDoubles vec = SafeVectorOfDoubles.of(doubles);
        Boolean thisIsABoolean = true;
        assertFalse("Expect a Boolean to not equal an array",
                    vec.equals(thisIsABoolean));
    }
}
