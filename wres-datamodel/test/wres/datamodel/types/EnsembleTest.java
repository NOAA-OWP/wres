package wres.datamodel.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import wres.datamodel.types.Ensemble.Labels;

/**
 * Tests the {@link Ensemble}.
 *
 * @author James Brown
 */
final class EnsembleTest
{

    /**
     * Labels instance for testing.
     */

    private final Labels labelsTestInstance = Labels.of( "A", "B", "C", "D" );

    /**
     * Instance for testing.
     */

    private final Ensemble testInstance = Ensemble.of( new double[] { 1, 2, 3, 4 },
                                                       this.labelsTestInstance );

    @Test
    void testGetMembersReturnsExpectedMembers()
    {
        double[] expected = new double[] { 1, 2, 3, 4 };

        double[] actual = this.testInstance.getMembers();

        assertArrayEquals( expected, actual );
    }

    @Test
    void testGetLabelsReturnsExpectedLabels()
    {
        Labels expected = Labels.of( "A", "B", "C", "D" );

        Labels actual = this.testInstance.getLabels();

        assertEquals( expected, actual );
    }

    @Test
    void testGetMember()
    {
        assertEquals( 1.0, this.testInstance.getMember( "A" ), 0.0001 );
        assertEquals( 2.0, this.testInstance.getMember( "B" ), 0.0001 );
        assertEquals( 3.0, this.testInstance.getMember( "C" ), 0.0001 );
        assertEquals( 4.0, this.testInstance.getMember( "D" ), 0.0001 );

        assertThrows( IllegalArgumentException.class, () -> this.testInstance.getMember( "E" ) );
    }

    @Test
    void testGetSizeReturnsExpectedSize()
    {
        assertEquals( 4, this.testInstance.size() );
    }

    @Test
    void testLabelsAreEmptyWhenExpected()
    {
        Ensemble emptyLabels = Ensemble.of( 1, 2, 3, 4 );

        Assertions.assertFalse( emptyLabels.hasLabels() );
    }

    @Test
    void testGetSortedEnsembleMembers()
    {
        Ensemble unsorted = Ensemble.of( new double[] { 1, 4, 3, Double.NaN, 2, 5 }, null, true );
        double[] actual = unsorted.getSortedMembers();
        assertArrayEquals( new double[] { 1, 2, 3, 4, 5, Double.NaN }, actual );
    }

    @Test
    void testEquals()
    {
        // Reflexive 
        assertEquals( this.testInstance, this.testInstance );

        // Symmetric
        Ensemble anotherInstance =
                Ensemble.of( new double[] { 1, 2, 3, 4 }, Labels.of( "A", "B", "C", "D" ) );

        assertTrue( anotherInstance.equals( this.testInstance ) && this.testInstance.equals( anotherInstance ) );

        // Transitive
        Ensemble oneMoreInstance =
                Ensemble.of( new double[] { 1, 2, 3, 4 }, Labels.of( "A", "B", "C", "D" ) );

        assertTrue( this.testInstance.equals( anotherInstance ) && anotherInstance.equals( oneMoreInstance )
                    && this.testInstance.equals( oneMoreInstance ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testInstance, anotherInstance );
        }

        // Equals without labels
        Ensemble noLabels = Ensemble.of( 1, 2, 3, 4 );

        assertEquals( noLabels, noLabels );

        // Nullity
        assertNotEquals( null, testInstance );

        // Check unequal cases
        Ensemble unequalOnMembers =
                Ensemble.of( new double[] { 1, 2, 3, 5 }, Labels.of( "A", "B", "C", "D" ) );

        assertNotEquals( this.testInstance, unequalOnMembers );

        Ensemble unequalOnLabels =
                Ensemble.of( new double[] { 1, 2, 3, 4 }, Labels.of( "A", "B", "C", "E" ) );

        assertNotEquals( this.testInstance, unequalOnLabels );

        Ensemble unequalOnLabelsPresent = Ensemble.of( 1, 2, 3, 4 );

        assertNotEquals( this.testInstance, unequalOnLabelsPresent );
    }

    @Test
    void testHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.testInstance.hashCode(), this.testInstance.hashCode() );

        // Consistent when invoked multiple times
        Ensemble anotherInstance =
                Ensemble.of( new double[] { 1, 2, 3, 4 }, Labels.of( "A", "B", "C", "D" ) );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.testInstance.hashCode(), anotherInstance.hashCode() );
        }

        // Check an instance without labels      
        Ensemble noLabels = Ensemble.of( 1, 2, 3, 4 );

        Ensemble anotherWithoutLabels = Ensemble.of( 1, 2, 3, 4 );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( noLabels.hashCode(), anotherWithoutLabels.hashCode() );
        }
    }

    @Test
    void testLabelEquals()
    {
        // Reflexive 
        assertEquals( this.labelsTestInstance, this.labelsTestInstance );

        // Symmetric
        Labels anotherInstance = Labels.of( "A", "B", "C", "D" );

        assertTrue( anotherInstance.equals( this.labelsTestInstance )
                    && this.labelsTestInstance.equals( anotherInstance ) );

        // Transitive
        Labels oneMoreInstance = Labels.of( "A", "B", "C", "D" );

        assertTrue( this.labelsTestInstance.equals( anotherInstance ) && anotherInstance.equals( oneMoreInstance )
                    && this.labelsTestInstance.equals( oneMoreInstance ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.labelsTestInstance, anotherInstance );
        }

        // Equals with empty labels
        Labels noLabels = Labels.of();

        assertEquals( noLabels, noLabels );

        // Nullity
        assertNotEquals( null, labelsTestInstance );

        // Check unequal cases
        Labels unequalOnLabels = Labels.of( "A", "B", "C", "E" );

        assertNotEquals( this.labelsTestInstance, unequalOnLabels );

        assertNotEquals( noLabels, unequalOnLabels );
    }

    @Test
    void testLabelHashCode()
    {
        // Equal objects have the same hashcode
        assertEquals( this.labelsTestInstance.hashCode(), this.labelsTestInstance.hashCode() );

        // Consistent when invoked multiple times
        Labels anotherInstance = Labels.of( "A", "B", "C", "D" );

        // Check an instance without labels      
        assertEquals( Labels.of().hashCode(), Labels.of().hashCode() );

        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.labelsTestInstance.hashCode(), anotherInstance.hashCode() );
        }
    }

    @Test
    void testLabelsCache()
    {
        Labels emptyLabels = Labels.of();
        Labels anotherEmptyLabels = Labels.of();

        assertSame( emptyLabels, anotherEmptyLabels );

        Labels someLabels = Labels.of( "A", "B", "C" );
        Labels someMoreLabels = Labels.of( "A", "B", "C" );

        assertSame( someLabels, someMoreLabels );

        // Ensure some labels are evicted
        for ( int i = 0; i < 101; i++ )
        {
            Labels.of( String.valueOf( i ) );
        }

        Labels oneMore = Labels.of( "X", "Y", "Z" );
        Labels yetOneMore = Labels.of( "X", "Y", "Z" );

        assertEquals( oneMore, yetOneMore );

        assertSame( oneMore, yetOneMore );
    }

    @Test
    void testCompareTo()
    {
        //Equal
        Ensemble anotherInstance =
                Ensemble.of( new double[] { 1, 2, 3, 4 }, Labels.of( "A", "B", "C", "D" ) );
        assertEquals( 0, this.testInstance.compareTo( anotherInstance ) );


        Ensemble noLabels = Ensemble.of( 1, 2, 3, 4 );
        Ensemble anotherWithoutLabels = Ensemble.of( 1, 2, 3, 4 );
        assertEquals( 0, noLabels.compareTo( anotherWithoutLabels ) );

        Ensemble less = Ensemble.of( 0, 2, 3, 4 );

        Ensemble evenLess = Ensemble.of( -1, 2, 3, 4 );

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
        Ensemble oneMoreInstance = Ensemble.of( new double[] { 1, 2, 3, 4 },
                                                Labels.of( "A", "B", "C", "E" ) );

        assertTrue( this.testInstance.compareTo( oneMoreInstance ) < 0 );

    }

    @Test
    void testToString()
    {
        String expected = "[{A,1.0},{B,2.0},{C,3.0},{D,4.0}]";

        String actual = this.testInstance.toString();

        assertEquals( expected, actual );

        // Members only
        Ensemble noLabels = Ensemble.of( 1, 2, 3, 4 );

        String expectedNoLabels = "[1.0,2.0,3.0,4.0]";

        String actualNoLabels = noLabels.toString();

        assertEquals( expectedNoLabels, actualNoLabels );
    }

    @Test
    void testDefaultLabels()
    {
        Ensemble actual = Ensemble.of( new double[] { 1, 2, 3, 4 }, true );
        Ensemble expected = Ensemble.of( new double[] { 1, 2, 3, 4 },
                                         Labels.of( "MEMBER 1", "MEMBER 2", "MEMBER 3", "MEMBER 4" ) );
        assertEquals( expected, actual );
    }

    @Test
    void checkForExpectedExceptionOnConstructionWhenThereAreMoreLabelsThanMembers()
    {
        Labels labels = Labels.of( "A", "B" );
        IllegalArgumentException expected =
                assertThrows( IllegalArgumentException.class,
                              () -> Ensemble.of( new double[] { 1 }, labels ) );

        assertEquals( "Expected the same number of members (1) as labels (2).", expected.getMessage() );

    }

}
