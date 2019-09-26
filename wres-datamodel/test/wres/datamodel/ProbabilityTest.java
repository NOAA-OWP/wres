package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;


/**
 * Tests the {@link Probability}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ProbabilityTest
{

    /**
     * Instance for testing.
     */

    private final Probability testInstance = Probability.of( 0.33 );

    @Test
    public void testProbabilityReturnsExpectedProbability()
    {
        assertEquals( 0.33, this.testInstance.getProbability(), 0.0001 );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertTrue( this.testInstance.equals( this.testInstance ) );

        // Symmetric
        Probability anotherInstance = Probability.of( 0.33 );

        assertTrue( anotherInstance.equals( this.testInstance ) && this.testInstance.equals( anotherInstance ) );

        // Transitive
        Probability oneMoreInstance = Probability.of( 0.33 );

        assertTrue( this.testInstance.equals( anotherInstance ) && anotherInstance.equals( oneMoreInstance )
                    && this.testInstance.equals( oneMoreInstance ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( this.testInstance.equals( anotherInstance ) );
        }

        // Nullity
        assertNotEquals( null, testInstance );
        assertNotEquals( testInstance, null );

        // Check unequal cases
        Probability unequal = Probability.of( 0.33000001 );

        assertNotEquals( this.testInstance, unequal );
    }

    @Test
    public void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.testInstance.hashCode(), this.testInstance.hashCode() );

        // Consistent when invoked multiple times
        Probability anotherInstance = Probability.of( 0.33 );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testInstance.hashCode(), anotherInstance.hashCode() );
        }
    }

    @Test
    public void testCompareTo()
    {
        //Equal
        Probability anotherInstance = Probability.of( 0.33 );       
        assertTrue( this.testInstance.compareTo( anotherInstance ) == 0 );


        Probability less = Probability.of( 0.32 );

        Probability evenLess = Probability.of( 0.31 );

        //Transitive
        //x.compareTo(y) > 0
        assertTrue( this.testInstance.compareTo( less ) > 0 );

        //y.compareTo(z) > 0
        assertTrue( less.compareTo( evenLess ) > 0 );

        //x.compareTo(z) > 0
        assertTrue( this.testInstance.compareTo( evenLess ) > 0 );
    }

    @Test
    public void testToString()
    {
        String expected = "0.33";

        String actual = this.testInstance.toString();

        assertEquals( expected, actual );
    }

    @Test
    public void checkForExpectedExceptionOnConstructionWhenProbabilityIsOOB()
    {
        // Above upper bound
        IllegalArgumentException expected = assertThrows( IllegalArgumentException.class, () -> Probability.of( 1.01 ) );
        
        assertEquals( "The input probability is not in the unit interval: 1.01", expected.getMessage() );

        // Below lower bound
        assertThrows( IllegalArgumentException.class, () -> Probability.of( -0.1 ) );

        // Not a finite number
        assertThrows( IllegalArgumentException.class, () -> Probability.of( Double.NaN ) );
    }

}
