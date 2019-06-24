package wres.datamodel.sampledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.sampledata.MeasurementUnit;

/**
 * Tests the {@link MeasurementUnit}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MeasurementUnitTest
{

    /**
     * Test {@link MeasurementUnit#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertTrue( MeasurementUnit.of().equals( MeasurementUnit.of() ) );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        // Reflexive
        assertTrue( m1.equals( m1 ) );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        // Symmetric
        assertTrue( m1.equals( m2 ) );
        assertTrue( m2.equals( m1 ) );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        // Transitive
        assertTrue( m1.equals( m2 ) );
        assertTrue( m2.equals( m3 ) );
        assertTrue( m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertFalse( m1.equals( m4 ) );
        // Null check
        assertNotEquals(null, m1 );
        // Other type check
        assertNotEquals( Double.valueOf( 2 ), m1 );
    }

    /**
     * Test {@link MeasurementUnit#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertTrue( MeasurementUnit.of().equals( MeasurementUnit.of() ) );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        assertEquals( m1, m1 );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        assertTrue( m1.hashCode() == m2.hashCode() );
        assertTrue( m2.equals( m3 ) );
        assertTrue( m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertFalse( m1.hashCode() == m4.hashCode() );
        // Other type check
        assertFalse( m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
