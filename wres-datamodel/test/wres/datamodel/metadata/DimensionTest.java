package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link MeasurementUnit}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DimensionTest
{

    /**
     * Test {@link MeasurementUnit#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertTrue( "Unexpected inequality between two dimension instances.",
                    MeasurementUnit.of().equals( MeasurementUnit.of() ) );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        // Reflexive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m1 ) );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        // Symmetric
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m1 ) );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        // Transitive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( m4 ) );
        // Null check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link MeasurementUnit#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertTrue( "Unexpected inequality between two dimension hashcodes.",
                    MeasurementUnit.of().equals( MeasurementUnit.of() ) );
        MeasurementUnit m1 = MeasurementUnit.of( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m1.hashCode() );
        MeasurementUnit m2 = MeasurementUnit.of( "A" );
        MeasurementUnit m3 = MeasurementUnit.of( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        MeasurementUnit m4 = MeasurementUnit.of( "B" );
        assertFalse( "Unexpected equality between two dimension hashcodes.", m1.hashCode() == m4.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two dimension hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
