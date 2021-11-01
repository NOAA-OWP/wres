package wres.datamodel.space;

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the {@link FeatureGroup}.
 * 
 * @author James Brown
 */

class FeatureGroupTest
{

    /** A feature group to test. **/
    private FeatureGroup aGroup;

    /** A feature tuple to test. **/
    private FeatureTuple aTuple;

    /** Another feature tuple to test. **/
    private FeatureTuple anotherTuple;

    @BeforeEach
    void runBeforeEachTest()
    {
        FeatureKey keyOne = new FeatureKey( "A", null, null, null );
        FeatureKey keyTwo = new FeatureKey( "B", null, null, null );
        FeatureKey keyThree = new FeatureKey( "C", null, null, null );
        this.aTuple = new FeatureTuple( keyOne, keyTwo, keyThree );

        this.aGroup = FeatureGroup.of( "aGroup", this.aTuple );

        FeatureKey keyFour = new FeatureKey( "A", "a feature", null, null );
        this.anotherTuple = new FeatureTuple( keyOne, keyTwo, keyFour );
    }

    @Test
    void testEquals()
    {
        // Reflexive 
        assertEquals( this.aGroup, this.aGroup );

        // Symmetric
        FeatureGroup anotherGroup = FeatureGroup.of( "aGroup", this.aTuple );

        assertTrue( anotherGroup.equals( this.aGroup )
                    && this.aGroup.equals( anotherGroup ) );

        // Transitive
        FeatureGroup yetAnotherGroup = FeatureGroup.of( "aGroup", this.aTuple );

        assertTrue( this.aGroup.equals( anotherGroup )
                    && anotherGroup.equals( yetAnotherGroup )
                    && this.aGroup.equals( yetAnotherGroup ) );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertTrue( aGroup.equals( anotherGroup ) );
        }

        // Nullity
        assertNotEquals( null, this.aGroup );
        assertNotEquals( this.aGroup, null );

        // Unequal cases
        FeatureGroup oneMoreGroup = FeatureGroup.of( "anotherGroup", this.aTuple );

        assertNotEquals( this.aGroup, oneMoreGroup );

        FeatureGroup yetOneMoreGroup = FeatureGroup.of( "aGroup", Set.of( this.aTuple, this.anotherTuple ) );

        assertNotEquals( yetOneMoreGroup, aGroup );

        FeatureGroup oneLastGroup = FeatureGroup.of( "aGroup", this.anotherTuple );

        assertNotEquals( oneLastGroup, aGroup );
    }

    @Test
    void testHashcode()
    {
        // Equals consistent with hashcode
        assertEquals( this.aGroup, this.aGroup );

        // Consistent
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( this.aGroup.hashCode(), this.aGroup.hashCode() );
        }
    }

    @Test
    void testCompareTo()
    {
        // Consistent with equals
        assertEquals( 0, this.aGroup.compareTo( this.aGroup ) );
        FeatureGroup anotherGroup = FeatureGroup.of( "anotherName", this.aTuple );
        assertNotEquals( 0, this.aGroup.compareTo( anotherGroup ) );

        // Unequal size
        FeatureGroup smallerGroup = FeatureGroup.of( Set.of( this.aTuple ) );
        FeatureGroup biggerGroup = FeatureGroup.of( Set.of( this.aTuple, this.anotherTuple ) );
        assertTrue( smallerGroup.compareTo( biggerGroup ) < 0 );

        // Equal size, equal group name, lesser tuple name
        FeatureKey keyOne = new FeatureKey( "A", null, null, null );
        FeatureKey keyTwo = new FeatureKey( "B", null, null, null );
        FeatureGroup lesserGroup = FeatureGroup.of( new FeatureTuple( keyOne, keyOne, null ) );
        FeatureGroup greaterGroup = FeatureGroup.of( new FeatureTuple( keyTwo, keyTwo, null ) );

        assertTrue( greaterGroup.compareTo( lesserGroup ) > 0 );
    }

    @Test
    void testGetName()
    {
        assertEquals( "aGroup", this.aGroup.getName() );
    }

    @Test
    void testGetFeatures()
    {
        assertEquals( Set.of( this.aTuple ), this.aGroup.getFeatures() );
    }

    @Test
    void testOfSingletons()
    {
        Set<FeatureGroup> groups = FeatureGroup.ofSingletons( Set.of( this.aTuple, this.anotherTuple ) );

        FeatureGroup firstExpected = FeatureGroup.of( this.aTuple );
        FeatureGroup secondExpected = FeatureGroup.of( this.anotherTuple );

        assertEquals( Set.of( firstExpected, secondExpected ), groups );
    }

    @Test
    void testFeatureGroupThrowsExpectedExceptionWhenNameTooLong()
    {
        IllegalArgumentException exception =
                assertThrows( IllegalArgumentException.class,
                              () -> FeatureGroup.of( new String( new char[99] ), this.aTuple ) );
        
        String actualMessage = exception.getMessage();
        String expectedMessageStartsWith = "A feature group name cannot be longer than" ;

        assertTrue( actualMessage.startsWith( expectedMessageStartsWith ) );
    }

}
