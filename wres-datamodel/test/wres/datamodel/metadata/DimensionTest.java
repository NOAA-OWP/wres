package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link Dimension}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DimensionTest
{

    /**
     * Test {@link Dimension#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        assertTrue( "Unexpected inequality between two dimension instances.",
                    MetadataFactory.getDimension().equals( MetadataFactory.getDimension() ) );
        Dimension m1 = MetadataFactory.getDimension( "A" );
        // Reflexive
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m1 ) );
        Dimension m2 = MetadataFactory.getDimension( "A" );
        // Symmetric
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m1 ) );
        Dimension m3 = MetadataFactory.getDimension( "A" );
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
        Dimension m4 = MetadataFactory.getDimension( "B" );
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( m4 ) );
        // Null check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( null ) );
        // Other type check
        assertFalse( "Unexpected equality between two dimension instances.", m1.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link Dimension#hashCode()}.
     */

    @Test
    public void testHashcode()
    {
        // Equal
        assertTrue( "Unexpected inequality between two dimension hashcodes.",
                    MetadataFactory.getDimension().equals( MetadataFactory.getDimension() ) );
        Dimension m1 = MetadataFactory.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m1.hashCode() );
        Dimension m2 = MetadataFactory.getDimension( "A" );
        Dimension m3 = MetadataFactory.getDimension( "A" );
        assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dimension instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dimension instances.", m1.equals( m3 ) );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dimension hashcodes.", m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        Dimension m4 = MetadataFactory.getDimension( "B" );
        assertFalse( "Unexpected equality between two dimension hashcodes.", m1.hashCode() == m4.hashCode() );
        // Other type check
        assertFalse( "Unexpected equality between two dimension hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
