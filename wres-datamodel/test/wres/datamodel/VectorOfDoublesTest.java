package wres.datamodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class VectorOfDoublesTest
{
    public static final double DELTA = 0.00001;

    @Test
    void itemOneDifferent()
    {
        double[] first = { 1.0, 2.0 };
        double[] second = { 3.0, 1.0 };
        VectorOfDoubles firstVec = VectorOfDoubles.of( first );
        VectorOfDoubles secondVec = VectorOfDoubles.of( second );
        assertTrue( firstVec.compareTo( secondVec ) < 0,
                    "Expect first vec to be less than second vec" );
        assertTrue( secondVec.compareTo( firstVec ) > 0,
                    "Expect second vec to be more than first vec" );
        assertNotEquals( firstVec, secondVec, "Expect first vec to not equal second vec" );
        assertNotEquals( secondVec, firstVec, "Expect second vec to not equal first vec" );
        assertTrue( firstVec.hashCode() != secondVec.hashCode(),
                    "Expect first hashcode to be different from second" );
    }

    @Test
    void itemOneSameItemTwoDifferent()
    {
        double[] first = { 1.0, 2.0 };
        double[] second = { 1.0, 3.0 };
        VectorOfDoubles firstVec = VectorOfDoubles.of( first );
        VectorOfDoubles secondVec = VectorOfDoubles.of( second );
        assertTrue( firstVec.compareTo( secondVec ) < 0,
                    "Expect first pair to be less than second pair" );
        assertTrue( secondVec.compareTo( firstVec ) > 0,
                    "Expect second pair to be more than first pair" );
        assertNotEquals( firstVec, secondVec, "Expect first vec to not equal second vec" );
        assertNotEquals( secondVec, firstVec, "Expect second vec to not equal first vec" );
        assertTrue( firstVec.hashCode() != secondVec.hashCode(),
                    "Expect first hashcode to be different from second" );
    }

    @Test
    void equalsItself()
    {
        double[] doubles = { 1.0, 2.0 };
        VectorOfDoubles vec = VectorOfDoubles.of( doubles );
        assertEquals( 0, vec.compareTo( vec ), "Expect a pair to equal itself" );
        assertEquals( vec, vec, "Expect a vec to equal itself" );
    }

    @Test
    void differentTypeNotEqual()
    {
        double[] doubles = { 1.0, 2.0 };
        VectorOfDoubles vec = VectorOfDoubles.of( doubles );
        Boolean thisIsABoolean = true;
        assertNotEquals( thisIsABoolean, vec );
    }


    @Test
    void vectorOfDoublesTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], DELTA );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], DELTA );
    }

    @Test
    void vectorOfDoublesMutationTest()
    {
        final double[] arrOne = { 1.0, 2.0 };
        final VectorOfDoubles doubleVecOne = VectorOfDoubles.of( arrOne );
        arrOne[0] = 3.0;
        arrOne[1] = 4.0;
        assertNotNull( doubleVecOne );
        assertEquals( 1.0, doubleVecOne.getDoubles()[0], DELTA );
        assertEquals( 2.0, doubleVecOne.getDoubles()[1], DELTA );
    }
}
