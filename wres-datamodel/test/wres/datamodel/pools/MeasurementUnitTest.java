package wres.datamodel.pools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

/**
 * Tests the {@link MeasurementUnit}.
 * 
 * @author James Brown
 */

public class MeasurementUnitTest
{

    /**
     * Test {@link MeasurementUnit#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertEquals( MeasurementUnit.of(), MeasurementUnit.of() );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        // Reflexive
        assertEquals( m1, m1 );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        // Symmetric
        assertEquals( m1, m2 );
        assertEquals( m2, m1 );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        // Transitive
        assertEquals( m1, m2 );
        assertEquals( m2, m3 );
        assertEquals( m1, m3 );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( "Unexpected inequality between two dimension instances.", m1, m2 );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertNotEquals( m1, m4 );
        // Null check
        assertNotEquals(null, m1 );
        // Other type check
        assertNotEquals( 2.0, m1 );
    }

    /**
     * Test {@link MeasurementUnit#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertEquals( MeasurementUnit.of(), MeasurementUnit.of() );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        assertEquals( m1, m1 );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        assertEquals( m1.hashCode(), m2.hashCode() );
        assertEquals( m2, m3 );
        assertEquals( m1, m3 );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertEquals( m1.hashCode(), m2.hashCode() );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertNotEquals( m1.hashCode(), m4.hashCode() );
        // Other type check
        assertNotEquals( m1.hashCode(), Double.valueOf( 2 ).hashCode() );
    }

    @Test 
    public void testConstructionThrowsExceptionWithBlankInputString()
    {
        assertThrows( IllegalArgumentException.class, () -> MeasurementUnit.of( "" ) );
    }    
    
}
