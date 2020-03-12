package wres.io.data.details;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests the {@link FeatureDetails}.
 */
public final class FeatureDetailsTest
{

    private static final String S101112 = "101112";
    /**
     * Instance to test.
     */

    private FeatureDetails.FeatureKey first;

    @Before
    public void runBeforeEachTest()
    {
        this.first = new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.8 );
    }

    @Test
    public void testFeatureKeyEquals()
    {
        // Reflexive
        assertTrue( "The FeatureKey does not meet the equals contract for reflexivity.",
                    this.first.equals( this.first ) );

        // Symmetric
        FeatureDetails.FeatureKey second = new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( "The FeatureKey does not meet the equals contract for symmetry.",
                    first.equals( second ) && second.equals( this.first ) );

        // Transitive
        FeatureDetails.FeatureKey third = new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( "The FeatureKey does not meet the equals contract for transitivity.",
                    first.equals( second ) && second.equals( third ) && first.equals( third ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( "The FeatureKey does not meet the equals contract for consistency.",
                        this.first.equals( second ) );
        }

        // Nullity
        assertNotEquals( null, this.first );

        // Check unequal cases
        assertNotEquals( "Expected different object classes to be unequal.", this.first, "notTheSame" );

        FeatureDetails.FeatureKey unequalOnComid =
                new FeatureDetails.FeatureKey( 1234, "456", "789", S101112, 23.7, 43.8 );

        assertFalse( "Expected the FeatureKey to differ on comid.", this.first.equals( unequalOnComid ) );

        FeatureDetails.FeatureKey unequalOnLid =
                new FeatureDetails.FeatureKey( 123, "4567", "789", S101112, 23.7, 43.8 );

        assertFalse( "Expected the FeatureKey to differ on lid.", this.first.equals( unequalOnLid ) );

        FeatureDetails.FeatureKey unequalOnGageId =
                new FeatureDetails.FeatureKey( 123, "456", "78910", S101112, 23.7, 43.8 );

        assertFalse( "Expected the FeatureKey to differ on gage id.", this.first.equals( unequalOnGageId ) );

        FeatureDetails.FeatureKey unequalOnHuc =
                new FeatureDetails.FeatureKey( 123, "456", "789", "10111213", 23.7, 43.8 );

        assertFalse( "Expected the FeatureKey to differ on HUC.", this.first.equals( unequalOnHuc ) );

        FeatureDetails.FeatureKey unequalOnLon =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.9, 43.8 );

        assertFalse( "Expected the FeatureKey to differ on longitude.", this.first.equals( unequalOnLon ) );

        FeatureDetails.FeatureKey unequalOnLat =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.9 );

        assertFalse( "Expected the FeatureKey to differ on latitude.", this.first.equals( unequalOnLat ) );
    }

    /**
     * Constructs an {@link Event} and tests {@link Event#hashCode()} against the hashes of other instances.
     */

    @Test
    public void testFeatureKeyHashCode()
    {
        // Consistent with equals
        FeatureDetails.FeatureKey second = new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( "The hashcode of the FeatureKey is inconsistent with equals.",
                    this.first.equals( second ) && this.first.hashCode() == second.hashCode() );

        // Consistent when called repeatedly
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( "The FeatureKey does not meet the hashcode contract for consistency.",
                          this.first.hashCode(),
                          this.first.hashCode() );
        }
    }

    @Test
    public void testFeatureKeyCompareTo()
    {
        FeatureDetails.FeatureKey isEqual = new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isEqual ) == 0 );

        FeatureDetails.FeatureKey isLessOnComid =
                new FeatureDetails.FeatureKey( 023, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isLessOnComid ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnComid =
                new FeatureDetails.FeatureKey( 234, "456", "789", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isGreaterOnComid ) > 0 );

        FeatureDetails.FeatureKey isLessOnLid =
                new FeatureDetails.FeatureKey( 123, "457", "789", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isLessOnLid ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnLid =
                new FeatureDetails.FeatureKey( 123, "356", "789", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isGreaterOnLid ) > 0 );

        FeatureDetails.FeatureKey isLessOnGageId =
                new FeatureDetails.FeatureKey( 123, "456", "889", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isLessOnGageId ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnGageId =
                new FeatureDetails.FeatureKey( 123, "456", "689", S101112, 23.7, 43.8 );

        assertTrue( this.first.compareTo( isGreaterOnGageId ) > 0 );

        FeatureDetails.FeatureKey isLessOnHuc =
                new FeatureDetails.FeatureKey( 123, "456", "789", "101113", 23.7, 43.8 );

        assertTrue( this.first.compareTo( isLessOnHuc ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnHuc =
                new FeatureDetails.FeatureKey( 123, "456", "789", "091112", 23.7, 43.8 );

        assertTrue( this.first.compareTo( isGreaterOnHuc ) > 0 );

        FeatureDetails.FeatureKey isLessOnLong =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 24.7, 43.8 );

        assertTrue( this.first.compareTo( isLessOnLong ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnLong =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 22.7, 43.8 );

        assertTrue( this.first.compareTo( isGreaterOnLong ) > 0 );

        FeatureDetails.FeatureKey isLessOnLat =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.9 );

        assertTrue( this.first.compareTo( isLessOnLat ) < 0 );

        FeatureDetails.FeatureKey isGreaterOnLat =
                new FeatureDetails.FeatureKey( 123, "456", "789", S101112, 23.7, 43.7 );

        assertTrue( this.first.compareTo( isGreaterOnLat ) > 0 );

        // Consistent with equals
        assertEquals( 0, this.first.compareTo( isEqual ) );
        assertNotEquals( 0, this.first.compareTo( isLessOnComid ) );
    }

    @Test
    public void testExceptionOnNullInputForFeatureKeyCompareTo()
    {
        assertThrows( NullPointerException.class,
                      () -> this.first.compareTo( null ) );
    }

}
