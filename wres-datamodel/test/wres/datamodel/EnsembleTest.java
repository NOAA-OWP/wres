package wres.datamodel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Test;


/**
 * Tests the {@link Ensemble}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class EnsembleTest
{

    /**
     * Instance for testing.
     */

    private final Ensemble testInstance =
            Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "D" } );

    @Test
    public void testGetMembersReturnsExpectedMembers()
    {
        double[] expected = new double[] { 1, 2, 3, 4 };

        double[] actual = this.testInstance.getMembers();

        assertTrue( Arrays.equals( expected, actual ) );
    }

    @Test
    public void testGetLabelsReturnsExpectedLabels()
    {
        String[] expected = new String[] { "A", "B", "C", "D" };

        String[] actual = this.testInstance.getLabels().get();

        assertTrue( Arrays.equals( expected, actual ) );
    }

    @Test
    public void testGetSizeReturnsExpectedSize()
    {
        assertEquals( 4, this.testInstance.size() );
    }

    @Test
    public void testLabelsAreEmptyWhenExpected()
    {
        Ensemble emptyLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );

        assertTrue( emptyLabels.getLabels().isEmpty() );
    }

    @Test
    public void testEquals()
    {
        // Reflexive 
        assertTrue( this.testInstance.equals( this.testInstance ) );

        // Symmetric
        Ensemble anotherInstance = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "D" } );

        assertTrue( anotherInstance.equals( this.testInstance ) && this.testInstance.equals( anotherInstance ) );

        // Transitive
        Ensemble oneMoreInstance = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "D" } );

        assertTrue( this.testInstance.equals( anotherInstance ) && anotherInstance.equals( oneMoreInstance )
                    && this.testInstance.equals( oneMoreInstance ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( this.testInstance.equals( anotherInstance ) );
        }

        // Equals without labels
        Ensemble noLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );

        assertEquals( noLabels, noLabels );

        // Nullity
        assertNotEquals( null, testInstance );
        assertNotEquals( testInstance, null );

        // Check unequal cases
        Ensemble unequalOnMembers = Ensemble.of( new double[] { 1, 2, 3, 5 }, new String[] { "A", "B", "C", "D" } );

        assertNotEquals( this.testInstance, unequalOnMembers );

        Ensemble unequalOnLabels = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "E" } );

        assertNotEquals( this.testInstance, unequalOnLabels );

        Ensemble unequalOnLabelsPresent = Ensemble.of( new double[] { 1, 2, 3, 4 } );

        assertNotEquals( this.testInstance, unequalOnLabelsPresent );
    }

    @Test
    public void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.testInstance.hashCode(), this.testInstance.hashCode() );

        // Consistent when invoked multiple times
        Ensemble anotherInstance = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "D" } );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testInstance.hashCode(), anotherInstance.hashCode() );
        }

        // Check an instance without labels      
        Ensemble noLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );

        Ensemble anotherWithoutLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( noLabels.hashCode(), anotherWithoutLabels.hashCode() );
        }
    }

    @Test
    public void testCompareTo()
    {
        //Equal
        Ensemble anotherInstance = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "D" } );
        assertTrue( this.testInstance.compareTo( anotherInstance ) == 0 );


        Ensemble noLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );
        Ensemble anotherWithoutLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );
        assertTrue( noLabels.compareTo( anotherWithoutLabels ) == 0 );

        Ensemble less = Ensemble.of( new double[] { 0, 2, 3, 4 } );

        Ensemble evenLess = Ensemble.of( new double[] { -1, 2, 3, 4 } );

        //Transitive
        //x.compareTo(y) > 0
        assertTrue( this.testInstance.compareTo( less ) > 0 );

        //y.compareTo(z) > 0
        assertTrue( less.compareTo( evenLess ) > 0 );

        //x.compareTo(z) > 0
        assertTrue( this.testInstance.compareTo( evenLess ) > 0 );

        // Differences on label presence
        assertTrue( this.testInstance.compareTo( noLabels ) > 0 );

        // Differences on labels
        Ensemble oneMoreInstance = Ensemble.of( new double[] { 1, 2, 3, 4 }, new String[] { "A", "B", "C", "E" } );

        assertTrue( this.testInstance.compareTo( oneMoreInstance ) < 0 );

    }

    @Test
    public void testToString()
    {
        String expected = "[{A,1.0},{B,2.0},{C,3.0},{D,4.0}]";
        
        String actual = this.testInstance.toString();
        
        assertEquals( expected, actual );
        
        // Members only
        Ensemble noLabels = Ensemble.of( new double[] { 1, 2, 3, 4 } );
        
        String expectedNoLabels = "[1.0,2.0,3.0,4.0]";
        
        String actualNoLabels = noLabels.toString();
        
        assertEquals( expectedNoLabels, actualNoLabels );
    }
    
    @Test
    public void checkForExpectedExceptionOnConstructionWhenThereAreMoreLabelsThanMembers()
    {
        IllegalArgumentException expected =
                assertThrows( IllegalArgumentException.class,
                              () -> Ensemble.of( new double[] { 1 }, new String[] { "A", "B" } ) );

        assertEquals( expected.getMessage(), "Expected the same number of members (1) as labels (2)." );

    }

}
