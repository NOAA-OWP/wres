package wres.datamodel;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class SafeVectorOfBooleansTest
{
    @Test
    public void itemOneDifferent()
    {
        boolean[] first = { false, true };
        boolean[] second = { true, false };
        VectorOfBooleans firstVec = SafeVectorOfBooleans.of( first );
        VectorOfBooleans secondVec = SafeVectorOfBooleans.of( second );
        assertFalse( "Expect first vec to not equal second vec.",
                     firstVec.equals( secondVec ) );
        assertFalse( "Expect second vec to not equal first vec.",
                     secondVec.equals( firstVec ) );
        assertTrue( "Expect first hashcode to be different from second.",
                    firstVec.hashCode() != secondVec.hashCode() );
    }

    @Test
    public void itemOneSameItemTwoDifferent()
    {
        boolean[] first = { false, false };
        boolean[] second = { false, true };
        VectorOfBooleans firstVec = SafeVectorOfBooleans.of( first );
        VectorOfBooleans secondVec = SafeVectorOfBooleans.of( second );
        assertFalse( "Expect first vec to not equal second vec.",
                     firstVec.equals( secondVec ) );
        assertFalse( "Expect second vec to not equal first vec.",
                     secondVec.equals( firstVec ) );
        assertTrue( "Expect first hashcode to be different from second.",
                    firstVec.hashCode() != secondVec.hashCode() );
    }

    @Test
    public void equalsItself()
    {
        boolean[] first = { true, false };
        VectorOfBooleans vec = SafeVectorOfBooleans.of( first );
        assertTrue( "Expect a vec to equal itself.",
                    vec.equals( vec ) );
    }

    @Test
    public void differentTypeNotEqual()
    {
        boolean[] first = { false, false };
        VectorOfBooleans firstVec = SafeVectorOfBooleans.of( first );
        Boolean thisIsABoolean = true;
        assertFalse( "Expect a Boolean to not equal an array of booleans.",
                     firstVec.equals( thisIsABoolean ) );
    }
    
    @Test
    public void firstDifferentSizeThanSecond() 
    {
        boolean[] first = { false, false, false };
        boolean[] second = { false, true };
        VectorOfBooleans firstVec = SafeVectorOfBooleans.of( first );
        VectorOfBooleans secondVec = SafeVectorOfBooleans.of( second );
        assertTrue( "Expected first to have different size than second.",
                    firstVec.size() != secondVec.size() );
        assertTrue( "Expected first to have size of 3.",
                    firstVec.size() == 3 );
        assertTrue( "Expected second to have size of 2.",
                    secondVec.size() == 2 );   
    }
    
    @Test
    public void firstSameSizeAsSecond() 
    {
        boolean[] first = { false, false };
        boolean[] second = { false, true };
        VectorOfBooleans firstVec = SafeVectorOfBooleans.of( first );
        VectorOfBooleans secondVec = SafeVectorOfBooleans.of( second );
        assertTrue( "Expected first to have same size as second.",
                    firstVec.size() == secondVec.size() );
    }    
}
