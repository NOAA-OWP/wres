package wres.datamodel.metadata;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link DatasetIdentifier}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class DatasetIdentifierTest
{

    /**
     * Test {@link DatasetIdentifier#equals(Object)}.
     */

    @Test
    public void testEquals()
    {
        // Reflexive
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m1 = MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m1 ) );

        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m2 = MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" );

        // Symmetric
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m1 ) );

        // Transitive
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m3 = MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", m2.equals( m3 ) );
        assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m3 ) );

        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.", m1.equals( m2 ) );
        }

        // Equal with some identifiers missing
        Location lp1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p1 = MetadataFactory.getDatasetIdentifier( lp1, "SQIN", null );

        Location lp2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p2 = MetadataFactory.getDatasetIdentifier( lp2, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p1.equals( p2 ) );

        Location lp3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p3 = MetadataFactory.getDatasetIdentifier( lp3, null, null );

        Location lp4 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p4 = MetadataFactory.getDatasetIdentifier( lp4, null, null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p3.equals( p4 ) );

        DatasetIdentifier p5 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", p5.equals( p6 ) );

        // Equal with scenario identifier for baseline
        Location lb1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b1 = MetadataFactory.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );

        Location lb2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b2 = MetadataFactory.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );

        assertTrue( "Unexpected inequality between two dataset identifier instances.", b1.equals( b2 ) );

        // Unequal
        Location l4 = MetadataFactory.getLocation( "DRRC3" );
        DatasetIdentifier m4 = MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" );

        Location l5 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m5 = MetadataFactory.getDatasetIdentifier( l5, "SQIN2", "HEFS" );

        Location l6 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m6 = MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS4" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m4 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m5 ) );
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( m6 ) );

        // Unequal with some identifiers missing
        Location lp7 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p7 = MetadataFactory.getDatasetIdentifier( lp7, "SQIN", null );

        Location lp8 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p8 = MetadataFactory.getDatasetIdentifier( lp8, "SQIN", "HEFS" );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p7.equals( p8 ) );

        Location lp9 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p9 = MetadataFactory.getDatasetIdentifier( lp9, null, null );

        DatasetIdentifier p10 = MetadataFactory.getDatasetIdentifier( null, "VAR", null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p9.equals( p10 ) );

        DatasetIdentifier p11 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 = MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "AB" ), null, null );

        assertFalse( "Unexpected equality between two dataset identifier instances.", p11.equals( p12 ) );

        // Unequal scenario identifiers for baseline
        Location lb3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b3 = MetadataFactory.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );

        Location lb4 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b4 = MetadataFactory.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );

        Location lb5 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b5 = MetadataFactory.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );

        assertFalse( "Unexpected inequality between two dataset identifier instances.", b3.equals( b4 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", p1.equals( b3 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", b5.equals( b3 ) );
        assertFalse( "Unexpected inequality between two dataset identifier instances.", b3.equals( b5 ) );

        // Null check
        assertFalse( "Unexpected equality between two dataset identifier instances.", m1.equals( null ) );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier instances.",
                     m1.equals( Double.valueOf( 2 ) ) );
    }

    /**
     * Test {@link DatasetIdentifier#hashCode()}.
     */

    @Test
    public void testHashCode()
    {
        // Equal
        Location l1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m1 = MetadataFactory.getDatasetIdentifier( l1, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m1.hashCode() );
        Location l2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m2 = MetadataFactory.getDatasetIdentifier( l2, "SQIN", "HEFS" );
        Location l3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m3 = MetadataFactory.getDatasetIdentifier( l3, "SQIN", "HEFS" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m2.hashCode() );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m2.hashCode() == m3.hashCode() );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", m1.hashCode() == m3.hashCode() );
        // Consistent
        for ( int i = 0; i < 20; i++ )
        {
            assertTrue( "Unexpected inequality between two dataset identifier instances.",
                        m1.hashCode() == m2.hashCode() );
        }
        // Unequal
        Location l4 = MetadataFactory.getLocation( "DRRC3" );
        DatasetIdentifier m4 = MetadataFactory.getDatasetIdentifier( l4, "SQIN", "HEFS" );
        Location l5 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m5 = MetadataFactory.getDatasetIdentifier( l5, "SQIN2", "HEFS" );
        Location l6 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier m6 = MetadataFactory.getDatasetIdentifier( l6, "SQIN", "HEFS4" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m4.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m5.hashCode() );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", m1.hashCode() == m6.hashCode() );

        // Equal with some identifiers missing
        Location lp1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p1 = MetadataFactory.getDatasetIdentifier( lp1, "SQIN", null );
        Location lp2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p2 = MetadataFactory.getDatasetIdentifier( lp2, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p1.hashCode() == p2.hashCode() );
        Location lp3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p3 = MetadataFactory.getDatasetIdentifier( lp3, null, null );
        Location lp4 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p4 = MetadataFactory.getDatasetIdentifier( lp4, null, null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p3.hashCode() == p4.hashCode() );
        DatasetIdentifier p5 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p6 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", p5.hashCode() == p6.hashCode() );
        // Equal with scenario identifier for baseline
        Location lb1 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b1 = MetadataFactory.getDatasetIdentifier( lb1, "SQIN", "HEFS", "ESP" );
        Location lb2 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b2 = MetadataFactory.getDatasetIdentifier( lb2, "SQIN", "HEFS", "ESP" );
        assertTrue( "Unexpected inequality between two dataset identifier hashcodes.", b1.hashCode() == b2.hashCode() );
        // Unequal with some identifiers missing
        Location lp7 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p7 = MetadataFactory.getDatasetIdentifier( lp7, "SQIN", null );
        Location lp8 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p8 = MetadataFactory.getDatasetIdentifier( lp8, "SQIN", "HEFS" );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p7.hashCode() == p8.hashCode() );
        Location lp9 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier p9 = MetadataFactory.getDatasetIdentifier( lp9, null, null );
        DatasetIdentifier p10 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.", p9.hashCode() == p10.hashCode() );
        DatasetIdentifier p11 = MetadataFactory.getDatasetIdentifier( null, "SQIN", null );
        DatasetIdentifier p12 =
                MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "LOC" ), null, null );
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     p11.hashCode() == p12.hashCode() );
        // Unequal scenario identifiers for baseline
        Location lb3 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b3 = MetadataFactory.getDatasetIdentifier( lb3, "SQIN", "HEFS", "ESP" );
        Location lb4 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b4 = MetadataFactory.getDatasetIdentifier( lb4, "SQIN", "HEFS", "ESP2" );
        Location lb5 = MetadataFactory.getLocation( "DRRC2" );
        DatasetIdentifier b5 = MetadataFactory.getDatasetIdentifier( lb5, "SQIN", "HEFS", null );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     b3.hashCode() == b4.hashCode() );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     p1.hashCode() == b3.hashCode() );
        assertFalse( "Unexpected inequality between two dataset identifier hashcodes.",
                     b3.hashCode() == b5.hashCode() );

        // Other type check
        assertFalse( "Unexpected equality between two dataset identifier hashcodes.",
                     m1.hashCode() == Double.valueOf( 2 ).hashCode() );
    }

}
